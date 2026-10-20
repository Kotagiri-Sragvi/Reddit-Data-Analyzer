import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { createServer, getServerPort, reddit, redis } from '@devvit/web/server';
import type { Post, Comment } from '@devvit/web/server';
// @ts-ignore
import rawConfig from './config.json';

/* =====================================================================
 *  P01DA26 harvester - merged version
 *
 *  Entry points (all call the same step() function):
 *    - onAppInstall / onAppUpgrade triggers  -> start automatically
 *    - scheduler cron "tick" (every minute)  -> start / continue automatically
 *    - moderator menu item                   -> manual fallback (same as before)
 *
 *  Modes: posts | comments | both | post | comment   (original, unchanged)
 *         date_range                                  (new: scan /new by date)
 *         probe                                       (new: rate-limit tests)
 *
 *  Long jobs are split into time-boxed steps. Progress lives in Redis, so a
 *  step that runs out of time simply continues on the next tick.
 * ===================================================================== */

// --- Type Definitions ---
interface ConfigPayload {
  runId?: string;
  targetSubreddit?: string;
  harvestMode?: string;
  targetPostId?: string | null;
  targetCommentId?: string | null;
  includeComments?: boolean;
  includeParentPost?: boolean;
  postSort?: 'hot' | 'top' | 'new' | 'rising';
  startDate?: string | null;
  endDate?: string | null;
  commentLimit?: number;
  timeBudgetSeconds?: number;
}

const config = rawConfig as ConfigPayload;

const app: Hono = new Hono();

type HarvestMode = 'posts' | 'comments' | 'both' | 'post' | 'comment' | 'date_range' | 'probe';
type PostSort = 'hot' | 'top' | 'new' | 'rising';

interface ExtractedPost {
  'post ID': string;
  'subreddit name': string;
  title: string;
  'body/self-text': string;
  author: string;
  'creation timestamp': number;
  'score/upvotes': number;
  'number of comments': number;
  'URL/permalink': string;
}

interface ExtractedComment {
  'comment ID': string;
  'parent post ID': string;
  'parent comment ID': string;
  subreddit: string;
  'comment text': string;
  author: string;
  timestamp: number;
  'score/upvotes': number;
  role?: 'parent_comment' | 'target_comment' | 'child_comment';
}

// --- Constants ---
const STATE_KEY = 'harvest:state';
const LOCK_KEY = 'harvest:lock';
const LOCK_TTL_SECONDS = 45; // frees itself if a step crashes
const MAX_SCAN = 1000; // Reddit's ceiling on any listing
const SCAN_PAGE = 100;
const STOP_STREAK = 3; // consecutive posts older than the window that end the scan
const MAX_ERROR_STREAK = 5; // give up after this many failed steps in a row

// Helper to yield the event loop and allow stdout stream buffers to flush cleanly
const flushStdout = (ms: number = 20) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Direct Configuration Resolvers ---

function getTargetSubreddit(): string {
  const sub = config.targetSubreddit || 'gaming';
  // Reddit subreddit names are case-insensitive; lowercase keeps things consistent.
  return sub.trim().toLowerCase().replace(/^r\//, '');
}

function getHarvestMode(): HarvestMode {
  const mode = config.harvestMode?.trim().toLowerCase();
  if (
    mode === 'posts' ||
    mode === 'comments' ||
    mode === 'both' ||
    mode === 'post' ||
    mode === 'comment' ||
    mode === 'date_range' ||
    mode === 'probe'
  ) {
    return mode as HarvestMode;
  }
  return 'both';
}

function getTargetPostId(): `t3_${string}` | null {
  const id = config.targetPostId?.trim();
  if (!id || id.toLowerCase() === 'n') return null;

  const cleanId = id.startsWith('t3_') ? id.slice(3) : id;
  return `t3_${cleanId}`;
}

function getTargetCommentId(): `t1_${string}` | null {
  const id = config.targetCommentId?.trim();
  if (!id || id.toLowerCase() === 'n') return null;

  const cleanId = id.startsWith('t1_') ? id.slice(3) : id;
  return `t1_${cleanId}`;
}

function getIncludeComments(): boolean {
  return Boolean(config.includeComments);
}

function getIncludeParentPost(): boolean {
  return config.includeParentPost !== undefined ? Boolean(config.includeParentPost) : true;
}

function getPostSort(): PostSort {
  const sort = config.postSort?.trim().toLowerCase();
  if (sort === 'top' || sort === 'new' || sort === 'rising') {
    return sort as PostSort;
  }
  return 'hot';
}

function getStartDate(): string | null {
  return config.startDate?.trim() || null;
}

function getEndDate(): string | null {
  return config.endDate?.trim() || null;
}

function getCommentLimit(): number {
  const n = Math.floor(Number(config.commentLimit));
  return Number.isFinite(n) && n > 0 ? Math.min(n, 1000) : 100;
}

/** Per-step time budget. Capped at 25 s to stay under the (reported) 30 s request limit. */
function getTimeBudgetMs(): number {
  const s = Number(config.timeBudgetSeconds);
  return (Number.isFinite(s) && s >= 5 ? Math.min(s, 25) : 20) * 1000;
}

// --- Date window (UTC) with strict validation ---

interface DateWindow {
  startMs: number;
  endMs: number;
}

/** "YYYY-MM-DD" -> UTC midnight in ms, or null if malformed / impossible (e.g. 2026-02-31). */
function parseDay(text: string | null | undefined): number | null {
  const t = (text ?? '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) return null;
  const ms = Date.parse(`${t}T00:00:00.000Z`);
  if (Number.isNaN(ms)) return null;
  if (new Date(ms).toISOString().slice(0, 10) !== t) return null;
  return ms;
}

/** Never falls back to "everything": a bad or missing date is an error. */
function resolveWindow(): DateWindow | { error: string } {
  const startText = getStartDate();
  if (!startText) return { error: 'startDate is required for date_range mode (YYYY-MM-DD)' };
  const startMs = parseDay(startText);
  if (startMs === null) return { error: `startDate "${startText}" is not a valid YYYY-MM-DD date` };

  const endText = getEndDate() ?? startText;
  const endDayMs = parseDay(endText);
  if (endDayMs === null) return { error: `endDate "${endText}" is not a valid YYYY-MM-DD date` };

  if (endDayMs < startMs) return { error: 'endDate is before startDate' };
  if (startMs > Date.now()) return { error: 'startDate is in the future' };
  return { startMs, endMs: endDayMs + 86_400_000 - 1 };
}

const iso = (ms: number | null): string | null => (ms === null ? null : new Date(ms).toISOString());

// --- Row builders (same field names as the original harvester) ---

function toPostRow(p: Post, sub: string): ExtractedPost {
  const ms = p.createdAt ? new Date(p.createdAt).getTime() : 0;
  return {
    'post ID': p.id,
    'subreddit name': p.subredditName || sub,
    title: p.title,
    'body/self-text': p.body || '',
    author: p.authorName ?? '[deleted]',
    'creation timestamp': ms ? Math.floor(ms / 1000) : 0,
    'score/upvotes': p.score,
    'number of comments': p.numberOfComments,
    'URL/permalink': `https://reddit.com${p.permalink}`,
  };
}

function toCommentRow(c: Comment, sub: string): ExtractedComment {
  const ms = c.createdAt ? new Date(c.createdAt).getTime() : 0;
  return {
    'comment ID': c.id,
    'parent post ID': c.postId,
    'parent comment ID': c.parentId,
    subreddit: c.subredditName || sub,
    'comment text': c.body,
    author: c.authorName ?? '[deleted]',
    timestamp: ms ? Math.floor(ms / 1000) : 0,
    'score/upvotes': c.score,
  };
}


// ======================================================================
//  ORIGINAL harvest modes: posts | comments | both | post | comment
//  Unchanged from your working version. It was only moved out of the menu
//  route into this function so that triggers and cron can start it too.
// ======================================================================
async function runLegacy(): Promise<{ postCount: number; commentCount: number }> {
  const TARGET_SUBREDDIT = getTargetSubreddit();
  const HARVEST_MODE = getHarvestMode();
  const TARGET_POST_ID = getTargetPostId();
  const TARGET_COMMENT_ID = getTargetCommentId();
  const INCLUDE_COMMENTS = getIncludeComments();
  const INCLUDE_PARENT_POST = getIncludeParentPost();
  const POST_SORT = getPostSort();

  console.log(
    `\n==================== START STREAM HARVEST: r/${TARGET_SUBREDDIT} [MODE: ${HARVEST_MODE.toUpperCase()}] ====================`
  );
  await flushStdout(30);

  let postCount = 0;
  let commentCount = 0;

  try {
    if (HARVEST_MODE === 'comment') {
      // --- MODE: SINGLE COMMENT + THREAD CONTEXT ---
      if (!TARGET_COMMENT_ID) {
        console.error("❌ Error: No Comment ID supplied for 'comment' mode.");
      } else {
        console.log(`🔍 Fetching target comment: ${TARGET_COMMENT_ID}...`);
        const targetComment = await reddit.getCommentById(TARGET_COMMENT_ID as `t1_${string}`);

        if (targetComment) {
          console.log('\n###--- START_COMMENTS_EXPORT ---###');
          await flushStdout(30);

          const parentId = targetComment.parentId;
          const postId = targetComment.postId;
          const cleanPostId = postId.startsWith('t3_') ? postId.slice(3) : postId;

          // Determine if target is top-level (direct reply to post) or nested (reply to another comment)
          const isTopLevel =
            !parentId ||
            parentId.startsWith('t3_') ||
            parentId === postId ||
            parentId === `t3_${cleanPostId}` ||
            parentId === cleanPostId;

          // 1. Fetch Parent Comment if target comment is a nested reply
          if (!isTopLevel && parentId) {
            const cleanParentCommentId = parentId.startsWith('t1_')
              ? parentId.slice(3)
              : parentId;
            const parentFullId: `t1_${string}` = `t1_${cleanParentCommentId}`;

            try {
              console.log(`⬆️ Fetching parent comment (${parentFullId})...`);
              const parentComment = await reddit.getCommentById(parentFullId);

              if (parentComment) {
                const parentRow: ExtractedComment = {
                  'comment ID': parentComment.id,
                  'parent post ID': parentComment.postId,
                  'parent comment ID': parentComment.parentId,
                  subreddit: parentComment.subredditName || TARGET_SUBREDDIT,
                  'comment text': parentComment.body,
                  author: parentComment.authorName ?? '[deleted]',
                  timestamp: parentComment.createdAt
                    ? Math.floor(new Date(parentComment.createdAt).getTime() / 1000)
                    : 0,
                  'score/upvotes': parentComment.score,
                  role: 'parent_comment',
                };
                console.log(`COMMENT_ROW:${JSON.stringify(parentRow)}`);
                commentCount++;
              }
            } catch (parentErr) {
              console.warn(`⚠️ Could not fetch parent comment: ${parentErr}`);
            }
          } else {
            console.log(
              `ℹ️ Target comment is a top-level comment (direct reply to post ${postId}). No parent comment exists.`
            );
          }

          // 2. Export Target Comment itself
          const targetRow: ExtractedComment = {
            'comment ID': targetComment.id,
            'parent post ID': targetComment.postId,
            'parent comment ID': targetComment.parentId,
            subreddit: targetComment.subredditName || TARGET_SUBREDDIT,
            'comment text': targetComment.body,
            author: targetComment.authorName ?? '[deleted]',
            timestamp: targetComment.createdAt
              ? Math.floor(new Date(targetComment.createdAt).getTime() / 1000)
              : 0,
            'score/upvotes': targetComment.score,
            role: 'target_comment',
          };
          console.log(`COMMENT_ROW:${JSON.stringify(targetRow)}`);
          commentCount++;

          // 3. Fetch Child Comments (direct replies)
          try {
            console.log(`⬇️ Fetching child comments / replies...`);
            const allComments = await reddit
              .getComments({
                postId: targetComment.postId,
                limit: 100,
              })
              .all();

            const targetCleanId = targetComment.id.replace(/^t1_/, '');
            const targetFullId = `t1_${targetCleanId}`;

            const childComments = allComments.filter((comm) => {
              const commParent = comm.parentId || '';
              return (
                commParent === targetCleanId ||
                commParent === targetFullId ||
                commParent === targetComment.id
              );
            });

            for (const child of childComments) {
              const childRow: ExtractedComment = {
                'comment ID': child.id,
                'parent post ID': child.postId,
                'parent comment ID': child.parentId,
                subreddit: child.subredditName || TARGET_SUBREDDIT,
                'comment text': child.body,
                author: child.authorName ?? '[deleted]',
                timestamp: child.createdAt
                  ? Math.floor(new Date(child.createdAt).getTime() / 1000)
                  : 0,
                'score/upvotes': child.score,
                role: 'child_comment',
              };
              console.log(`COMMENT_ROW:${JSON.stringify(childRow)}`);
              commentCount++;
            }
          } catch (childErr) {
            console.warn(`⚠️ Could not fetch child comments: ${childErr}`);
          }

          await flushStdout(30);
          console.log('###--- END_COMMENTS_EXPORT ---###\n');

          // 4. Optionally Fetch Parent Post
          if (INCLUDE_PARENT_POST) {
            try {
              console.log(`📌 Fetching parent post (${targetComment.postId})...`);
              const parentPost = await reddit.getPostById(targetComment.postId as `t3_${string}`);

              if (parentPost) {
                console.log('\n###--- START_POSTS_EXPORT ---###');
                await flushStdout(30);

                const postRow: ExtractedPost = {
                  'post ID': parentPost.id,
                  'subreddit name': parentPost.subredditName || TARGET_SUBREDDIT,
                  title: parentPost.title,
                  'body/self-text': parentPost.body || '',
                  author: parentPost.authorName ?? '[deleted]',
                  'creation timestamp': parentPost.createdAt
                    ? Math.floor(new Date(parentPost.createdAt).getTime() / 1000)
                    : 0,
                  'score/upvotes': parentPost.score,
                  'number of comments': parentPost.numberOfComments,
                  'URL/permalink': `https://reddit.com${parentPost.permalink}`,
                };

                console.log(`POST_ROW:${JSON.stringify(postRow)}`);
                postCount++;
                await flushStdout(30);
                console.log('###--- END_POSTS_EXPORT ---###\n');
              }
            } catch (postErr) {
              console.warn(`⚠️ Could not fetch parent post: ${postErr}`);
            }
          }
        }
      }
    } else if (HARVEST_MODE === 'post') {
      // --- MODE: SINGLE POST ---
      let targetPost: any = null;

      if (TARGET_POST_ID) {
        try {
          console.log(`🔍 Fetching specific post ID: ${TARGET_POST_ID}...`);
          targetPost = await reddit.getPostById(TARGET_POST_ID as `t3_${string}`);
        } catch (e) {
          console.warn(
            `⚠️ Post ID ${TARGET_POST_ID} not found or inaccessible. Falling back to feed selection.`
          );
        }
      }

      if (!targetPost) {
        console.log(
          `🎲 Selecting a random ${POST_SORT.toUpperCase()} post from r/${TARGET_SUBREDDIT}...`
        );
        let fetchedPosts: any[] = [];

        if (POST_SORT === 'top') {
          fetchedPosts = await reddit
            .getTopPosts({ subredditName: TARGET_SUBREDDIT, limit: 50 })
            .all();
        } else if (POST_SORT === 'new') {
          fetchedPosts = await reddit
            .getNewPosts({ subredditName: TARGET_SUBREDDIT, limit: 50 })
            .all();
        } else if (POST_SORT === 'rising') {
          fetchedPosts = await reddit
            .getRisingPosts({ subredditName: TARGET_SUBREDDIT, limit: 50 })
            .all();
        } else {
          fetchedPosts = await reddit
            .getHotPosts({ subredditName: TARGET_SUBREDDIT, limit: 50 })
            .all();
        }

        if (fetchedPosts.length > 0) {
          const randomIndex = Math.floor(Math.random() * fetchedPosts.length);
          targetPost = fetchedPosts[randomIndex];
        } else {
          console.error(`❌ No ${POST_SORT} posts found in r/${TARGET_SUBREDDIT}`);
        }
      }

      if (targetPost) {
        console.log('\n###--- START_POSTS_EXPORT ---###');
        await flushStdout(30);

        const postRow: ExtractedPost = {
          'post ID': targetPost.id,
          'subreddit name': targetPost.subredditName || TARGET_SUBREDDIT,
          title: targetPost.title,
          'body/self-text': targetPost.body || '',
          author: targetPost.authorName ?? '[deleted]',
          'creation timestamp': targetPost.createdAt
            ? Math.floor(new Date(targetPost.createdAt).getTime() / 1000)
            : 0,
          'score/upvotes': targetPost.score,
          'number of comments': targetPost.numberOfComments,
          'URL/permalink': `https://reddit.com${targetPost.permalink}`,
        };

        console.log(`POST_ROW:${JSON.stringify(postRow)}`);
        postCount++;
        await flushStdout(30);
        console.log('###--- END_POSTS_EXPORT ---###\n');

        if (INCLUDE_COMMENTS && targetPost.numberOfComments > 0) {
          try {
            console.log('###--- START_COMMENTS_EXPORT ---###');
            await flushStdout(30);

            const comments = await reddit
              .getComments({
                postId: targetPost.id,
                limit: 100,
              })
              .all();

            for (const comment of comments) {
              const commentRow: ExtractedComment = {
                'comment ID': comment.id,
                'parent post ID': comment.postId,
                'parent comment ID': comment.parentId,
                subreddit: comment.subredditName || TARGET_SUBREDDIT,
                'comment text': comment.body,
                author: comment.authorName ?? '[deleted]',
                timestamp: comment.createdAt
                  ? Math.floor(new Date(comment.createdAt).getTime() / 1000)
                  : 0,
                'score/upvotes': comment.score,
              };
              console.log(`COMMENT_ROW:${JSON.stringify(commentRow)}`);
              commentCount++;
            }

            await flushStdout(30);
            console.log('###--- END_COMMENTS_EXPORT ---###\n');
          } catch (commentError) {
            console.warn(`⚠️ Warning: Failed to fetch comments: ${commentError}`);
          }
        }
      }
    } else {
      // --- BULK MODES ('posts', 'comments', or 'both') ---
      const shouldExportPosts = HARVEST_MODE === 'posts' || HARVEST_MODE === 'both';
      const shouldExportComments = HARVEST_MODE === 'comments' || HARVEST_MODE === 'both';
      const MAX_COMMENTS_TOTAL = 550;

      const posts = await reddit
        .getHotPosts({
          subredditName: TARGET_SUBREDDIT,
          limit: 100,
        })
        .all();

      if (shouldExportPosts) {
        console.log('\n###--- START_POSTS_EXPORT ---###');
        await flushStdout(30);
      }

      for (const post of posts) {
        if (shouldExportPosts) {
          const postRow: ExtractedPost = {
            'post ID': post.id,
            'subreddit name': post.subredditName || TARGET_SUBREDDIT,
            title: post.title,
            'body/self-text': post.body || '',
            author: post.authorName ?? '[deleted]',
            'creation timestamp': post.createdAt
              ? Math.floor(new Date(post.createdAt).getTime() / 1000)
              : 0,
            'score/upvotes': post.score,
            'number of comments': post.numberOfComments,
            'URL/permalink': `https://reddit.com${post.permalink}`,
          };
          console.log(`POST_ROW:${JSON.stringify(postRow)}`);
          postCount++;

          if (postCount % 10 === 0) await flushStdout(20);
        }

        if (
          shouldExportComments &&
          commentCount < MAX_COMMENTS_TOTAL &&
          post.numberOfComments > 0
        ) {
          try {
            const comments = await reddit
              .getComments({
                postId: post.id,
                limit: 50,
              })
              .all();

            if (commentCount === 0 && !shouldExportPosts) {
              console.log('\n###--- START_COMMENTS_EXPORT ---###');
              await flushStdout(30);
            } else if (commentCount === 0 && shouldExportPosts) {
              await flushStdout(50);
              console.log('###--- END_POSTS_EXPORT ---###\n');
              await flushStdout(30);
              console.log('###--- START_COMMENTS_EXPORT ---###');
              await flushStdout(30);
            }

            for (const comment of comments) {
              if (commentCount >= MAX_COMMENTS_TOTAL) break;

              const commentRow: ExtractedComment = {
                'comment ID': comment.id,
                'parent post ID': comment.postId,
                'parent comment ID': comment.parentId,
                subreddit: comment.subredditName || TARGET_SUBREDDIT,
                'comment text': comment.body,
                author: comment.authorName ?? '[deleted]',
                timestamp: comment.createdAt
                  ? Math.floor(new Date(comment.createdAt).getTime() / 1000)
                  : 0,
                'score/upvotes': comment.score,
              };
              console.log(`COMMENT_ROW:${JSON.stringify(commentRow)}`);
              commentCount++;

              if (commentCount % 20 === 0) await flushStdout(20);
            }
          } catch (commentError) {
            console.warn(
              `⚠️ Warning: Failed to fetch comments for post ${post.id}: ${commentError}`
            );
          }
        }
      }

      await flushStdout(60);

      if (shouldExportPosts && !shouldExportComments) {
        console.log('###--- END_POSTS_EXPORT ---###\n');
        await flushStdout(30);
      } else if (shouldExportComments && commentCount > 0) {
        console.log('###--- END_COMMENTS_EXPORT ---###\n');
        await flushStdout(30);
      }
    }

    console.log(
      `📊 Verification Tally -> Streamed ${postCount} Post(s) & ${commentCount} Comment(s).`
    );
    await flushStdout(30);
  } catch (error) {
    console.error(`❌ Network extraction failure: ${error}`);
  }

  console.log(
    `==================== END STREAM HARVEST: r/${TARGET_SUBREDDIT} ====================\n`
  );

  return { postCount, commentCount };
}

// ======================================================================
//  NEW: self-starting, time-boxed harvesting (date_range + probe)
// ======================================================================

interface MatchedPost {
  id: string;
  n: number; // number of comments Reddit reports for the post
  fails?: number;
}

interface RunState {
  runId: string;
  mode: HarvestMode;
  phase: 'scan' | 'comments' | 'done';
  startedAt: number;
  steps: number;
  errorStreak: number;
  // scan progress
  after: string | null;
  scanned: number;
  olderStreak: number;
  reachedOlder: boolean;
  exhausted: boolean;
  oldestScannedMs: number | null;
  // comment progress
  matched: MatchedPost[];
  commentIdx: number;
  failedPosts: string[];
  // totals
  postCount: number;
  commentCount: number;
  // probe progress
  probeStep: number;
}

function newState(runId: string, mode: HarvestMode): RunState {
  return {
    runId,
    mode,
    phase: 'scan',
    startedAt: Date.now(),
    steps: 0,
    errorStreak: 0,
    after: null,
    scanned: 0,
    olderStreak: 0,
    reachedOlder: false,
    exhausted: false,
    oldestScannedMs: null,
    matched: [],
    commentIdx: 0,
    failedPosts: [],
    postCount: 0,
    commentCount: 0,
    probeStep: 0,
  };
}

async function loadState(): Promise<RunState | null> {
  try {
    const raw = await redis.get(STATE_KEY);
    return raw ? (JSON.parse(raw) as RunState) : null;
  } catch {
    return null;
  }
}

async function saveState(state: RunState): Promise<void> {
  await redis.set(STATE_KEY, JSON.stringify(state));
}

/** Atomic lock: INCR returns 1 only for the first caller. Expires on its own if a step crashes. */
async function acquireLock(): Promise<boolean> {
  const n = Number(await redis.incrBy(LOCK_KEY, 1));
  if (n === 1) {
    await redis.expire(LOCK_KEY, LOCK_TTL_SECONDS);
    return true;
  }
  // Self-heal: a lock without an expiry would block everything forever.
  if ((await redis.expireTime(LOCK_KEY)) < 0) await redis.expire(LOCK_KEY, LOCK_TTL_SECONDS);
  return false;
}

async function releaseLock(): Promise<void> {
  await redis.del(LOCK_KEY);
}

async function finish(state: RunState): Promise<void> {
  state.phase = 'done';
  await saveState(state);
  if (state.failedPosts.length > 0) {
    console.log(`FAILED_POSTS:${JSON.stringify(state.failedPosts)}`);
  }
  console.log(
    `📊 Verification Tally -> Streamed ${state.postCount} Post(s) & ${state.commentCount} Comment(s).`
  );
  await flushStdout(60);
  console.log(`==================== END STREAM HARVEST: r/${getTargetSubreddit()} ====================\n`);
}

// --- date_range: scan /new newest -> oldest, then fetch comments for matching posts ---

async function dateRangeStep(state: RunState, deadline: number): Promise<void> {
  const sub = getTargetSubreddit();
  const win = resolveWindow();
  if ('error' in win) {
    console.log(`HARVEST_ERROR:${win.error}`);
    await finish(state);
    return;
  }
  const includeComments = getIncludeComments();
  const limit = getCommentLimit();

  if (state.steps === 1) {
    console.log(
      `📅 Window (UTC): ${iso(win.startMs)} -> ${iso(win.endMs)} | comments: ${
        includeComments ? `yes, up to ${limit} per post` : 'no'
      }`
    );
  }

  // ---- Phase 1: scan the /new listing ----
  if (state.phase === 'scan') {
    while (!state.reachedOlder && !state.exhausted) {
      if (state.scanned >= MAX_SCAN) {
        state.exhausted = true;
        break;
      }
      if (Date.now() > deadline - 3000) break; // out of time: continue next tick

      let batch: Post[] = [];
      try {
        batch = await reddit
          .getNewPosts({
            subredditName: sub,
            limit: SCAN_PAGE,
            pageSize: SCAN_PAGE,
            ...(state.after ? { after: state.after } : {}),
          })
          .all();
      } catch (err) {
        console.log(`⚠️ Listing fetch failed (will retry on the next tick): ${err}`);
        await saveState(state);
        throw err;
      }

      if (batch.length === 0) {
        state.exhausted = true;
        break;
      }

      for (const post of batch) {
        state.scanned++;
        const t = post.createdAt ? new Date(post.createdAt).getTime() : 0;
        if (t && (state.oldestScannedMs === null || t < state.oldestScannedMs)) {
          state.oldestScannedMs = t;
        }

        if (t > win.endMs) {
          state.olderStreak = 0; // newer than the window: keep walking back
        } else if (t >= win.startMs) {
          state.olderStreak = 0;
          console.log(`POST_ROW:${JSON.stringify(toPostRow(post, sub))}`);
          state.postCount++;
          if (includeComments && post.numberOfComments > 0) {
            state.matched.push({ id: post.id, n: post.numberOfComments });
          }
        } else {
          state.olderStreak++;
          if (state.olderStreak >= STOP_STREAK) {
            state.reachedOlder = true;
            break;
          }
        }
      }

      const last = batch[batch.length - 1];
      if (last) state.after = last.id;
      state.errorStreak = 0;
      await saveState(state);
      await flushStdout(20);
    }

    if (state.reachedOlder || state.exhausted) {
      const complete = state.reachedOlder;
      const reason = complete
        ? 'ok'
        : state.scanned >= MAX_SCAN
          ? 'listing_cap_reached'
          : 'listing_ended';
      console.log(
        `COVERAGE:${JSON.stringify({
          complete,
          reason,
          scanned: state.scanned,
          oldestScannedUtc: iso(state.oldestScannedMs),
          windowStartUtc: iso(win.startMs),
          windowEndUtc: iso(win.endMs),
          postsInWindow: state.postCount,
        })}`
      );
      if (!complete) {
        console.warn(
          `⚠️ The listing ended before reaching posts older than ${iso(win.startMs)}. ` +
            `Posts from the start of the window may be unreachable via the live API.`
        );
      }
      state.phase = includeComments && state.matched.length > 0 ? 'comments' : 'done';
      await saveState(state);
    }
  }

  // ---- Phase 2: comments for the posts found in the window ----
  if (state.phase === 'comments') {
    while (state.commentIdx < state.matched.length) {
      if (Date.now() > deadline - 4000) break; // out of time: continue next tick
      const m = state.matched[state.commentIdx];
      if (!m) break;

      try {
        const comments = await reddit
          .getComments({
            postId: m.id as `t3_${string}`,
            limit,
            pageSize: Math.min(limit, 100),
          })
          .all();
        for (const c of comments) {
          console.log(`COMMENT_ROW:${JSON.stringify(toCommentRow(c, sub))}`);
          state.commentCount++;
        }
        console.log(
          `POST_COMMENTS:${JSON.stringify({ id: m.id, reported: m.n, returned: comments.length })}`
        );
        state.commentIdx++;
      } catch (err) {
        m.fails = (m.fails ?? 0) + 1;
        console.log(`⚠️ Comments fetch failed for ${m.id} (attempt ${m.fails}/3): ${err}`);
        if (m.fails >= 3) {
          state.failedPosts.push(m.id);
          state.commentIdx++;
        }
        await saveState(state);
        break; // back off until the next tick
      }
      await saveState(state);
      await flushStdout(20);
    }
    if (state.commentIdx >= state.matched.length) state.phase = 'done';
    await saveState(state);
  }

  if (state.phase === 'done') {
    await finish(state);
  } else {
    console.log(
      `PROGRESS:${JSON.stringify({
        phase: state.phase,
        scanned: state.scanned,
        postsFound: state.postCount,
        commentPostsDone: `${state.commentIdx}/${state.matched.length}`,
        commentsStreamed: state.commentCount,
      })}`
    );
  }
}

// --- probe: measures what the API really does (run once per subreddit) ---

async function probeStep(state: RunState, deadline: number): Promise<void> {
  const sub = getTargetSubreddit();
  const P = (s: string) => console.log(`PROBE: ${s}`);

  // Test 1: how deep does the /new listing really go, and how long does it take?
  if (state.probeStep === 0) {
    P(`--- Test 1/3: listing depth for r/${sub} (limit 1000, 100 per page) ---`);
    try {
      const t0 = Date.now();
      const posts = await reddit.getNewPosts({ subredditName: sub, limit: 1000, pageSize: 100 }).all();
      const ms = Date.now() - t0;
      const oldest = posts[posts.length - 1];
      const ageDays = oldest
        ? (Date.now() - new Date(oldest.createdAt).getTime()) / 86_400_000
        : 0;
      P(`got ${posts.length} posts in ${ms}ms (about ${Math.ceil(posts.length / 100)} requests)`);
      P(`oldest post is ${ageDays.toFixed(2)} days old -> about ${
        ageDays > 0 ? (posts.length / ageDays).toFixed(1) : 'n/a'
      } posts/day`);
      state.matched = posts
        .filter((p) => p.numberOfComments > 0)
        .sort((a, b) => b.numberOfComments - a.numberOfComments)
        .slice(0, 5)
        .map((p) => ({ id: p.id, n: p.numberOfComments }));
    } catch (err) {
      P(`Test 1 FAILED: ${err}`);
    }
    state.probeStep = 1;
    await saveState(state);
  }

  // Test 2: how long do comment fetches take, and how many comments come back?
  if (state.probeStep === 1 && deadline - Date.now() > 8000) {
    P('--- Test 2/3: comment fetch (limit 100) on the 5 busiest recent posts ---');
    for (const m of state.matched) {
      if (Date.now() > deadline - 3000) {
        P('stopped early: time budget reached');
        break;
      }
      const t = Date.now();
      try {
        const cs = await reddit
          .getComments({ postId: m.id as `t3_${string}`, limit: 100, pageSize: 100 })
          .all();
        P(`post ${m.id}: reddit reports ${m.n} comments, returned ${cs.length} in ${Date.now() - t}ms`);
      } catch (err) {
        P(`post ${m.id}: FAILED after ${Date.now() - t}ms: ${err}`);
      }
    }
    state.probeStep = 2;
    await saveState(state);
  }

  // Test 3: burst - 5 parallel calls per round, to see whether anything throttles
  if (state.probeStep === 2 && deadline - Date.now() > 8000) {
    P('--- Test 3/3: burst (5 parallel listing calls per round, up to 8 rounds) ---');
    const lat: number[] = [];
    let failures = 0;
    let firstError = '';
    for (let round = 1; round <= 8 && Date.now() < deadline - 4000; round++) {
      const results = await Promise.allSettled(
        Array.from({ length: 5 }, async () => {
          const t = Date.now();
          await reddit.getNewPosts({ subredditName: sub, limit: 25, pageSize: 25 }).all();
          return Date.now() - t;
        })
      );
      let ok = 0;
      for (const r of results) {
        if (r.status === 'fulfilled') {
          lat.push(r.value);
          ok++;
        } else {
          failures++;
          if (!firstError) firstError = String(r.reason);
        }
      }
      P(`round ${round}: ${ok}/5 ok`);
      if (failures > 0) break; // stop at the first sign of throttling
    }
    if (lat.length > 0) {
      const avg = Math.round(lat.reduce((a, b) => a + b, 0) / lat.length);
      P(`calls ok=${lat.length} failed=${failures} | latency ms min=${Math.min(...lat)} avg=${avg} max=${Math.max(...lat)}`);
    } else {
      P(`no successful calls (failed=${failures})`);
    }
    if (firstError) P(`first error: ${firstError}`);
    state.probeStep = 3;
    await saveState(state);
  }

  if (state.probeStep >= 3) {
    P('--- probe complete ---');
    await finish(state);
  } else {
    console.log(`PROGRESS:${JSON.stringify({ probeStep: state.probeStep })}`);
  }
}

// --- One step of work. Called by triggers, cron and the menu item. ---

async function step(source: string, force = false): Promise<string> {
  let runId = (config.runId ?? '').trim();
  if (!runId) {
    if (!force) return 'idle (no runId configured)'; // cron ticks do nothing without a run
    runId = `manual-${Date.now()}`; // menu click without the launcher: behaves like before
  }
  const mode = getHarvestMode();

  // Cheap check first: a finished run costs one Redis read and no Reddit calls.
  const peek = await loadState();
  if (!force && peek && peek.runId === runId && peek.phase === 'done') {
    return 'idle (run finished)';
  }

  if (!(await acquireLock())) return 'busy (another step is running)';

  let state: RunState | null = null;
  try {
    const existing = await loadState();
    if (existing && existing.runId === runId && existing.phase === 'done' && !force) {
      return 'idle (run finished)';
    }
    const resume = existing !== null && existing.runId === runId && existing.phase !== 'done';
    state = resume && existing ? existing : newState(runId, mode);

    if (!resume) {
      if (mode === 'date_range' || mode === 'probe') {
        console.log(
          `\n==================== START STREAM HARVEST: r/${getTargetSubreddit()} [MODE: ${mode.toUpperCase()}] [run ${runId}] [via ${source}] ====================`
        );
        await flushStdout(30);
      }
      await saveState(state);
    }

    state.steps++;
    const deadline = Date.now() + getTimeBudgetMs();

    if (mode === 'date_range') {
      await dateRangeStep(state, deadline);
    } else if (mode === 'probe') {
      await probeStep(state, deadline);
    } else {
      // Original modes: run once, at most once (marked done BEFORE running so a
      // timeout can never make every cron tick repeat the harvest).
      state.phase = 'done';
      await saveState(state);
      const r = await runLegacy();
      state.postCount = r.postCount;
      state.commentCount = r.commentCount;
      await saveState(state);
    }

    state.errorStreak = 0;
    if (state.phase !== 'done') await saveState(state);
    return state.phase === 'done'
      ? `done (${state.postCount} posts, ${state.commentCount} comments)`
      : `in progress (${state.phase}, step ${state.steps})`;
  } catch (err) {
    console.log(`⚠️ Step failed (${source}): ${err}`);
    if (state) {
      state.errorStreak++;
      if (state.errorStreak >= MAX_ERROR_STREAK) {
        console.log(`HARVEST_ERROR:giving up after ${MAX_ERROR_STREAK} failed steps in a row: ${err}`);
        await finish(state);
      } else {
        await saveState(state);
      }
    }
    return `error: ${err}`;
  } finally {
    await releaseLock();
  }
}

async function safeStep(source: string, force = false): Promise<string> {
  try {
    return await step(source, force);
  } catch (err) {
    console.log(`⚠️ Step crashed before it could start (${source}): ${err}`);
    return `error: ${err}`;
  }
}

// --- Entry points ---

// Manual fallback: the old menu item still works exactly as before.
app.post('/internal/menu/post-create', async (c) => {
  const result = await safeStep('menu', true);
  return c.json({ showToast: { text: `Harvest: ${result}` } });
});

// Automatic starts (no human needed)
app.post('/internal/triggers/app-install', async (c) => {
  await safeStep('onAppInstall');
  return c.json({});
});

app.post('/internal/triggers/app-upgrade', async (c) => {
  await safeStep('onAppUpgrade');
  return c.json({});
});

app.post('/internal/cron/tick', async (c) => {
  await safeStep('cron');
  return c.json({});
});

serve({
  fetch: app.fetch,
  createServer,
  port: getServerPort(),
});

export default app;