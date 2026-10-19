import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { createServer, getServerPort, reddit, redis } from '@devvit/web/server';
// @ts-ignore
import rawConfig from './config.json';

// --- Type Definitions ---
interface ConfigPayload {
  targetSubreddit?: string;
  harvestMode?: string;
  targetPostId?: string | null;
  targetCommentId?: string | null;
  includeComments?: boolean;
  includeParentPost?: boolean;
  postSort?: 'hot' | 'top' | 'new' | 'rising';
}

const config = rawConfig as ConfigPayload;

const app: Hono = new Hono();

type HarvestMode = 'posts' | 'comments' | 'both' | 'post' | 'comment';
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

// Helper to yield the event loop and allow stdout stream buffers to flush cleanly
const flushStdout = (ms: number = 20) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Direct Configuration Resolvers ---

function getTargetSubreddit(): string {
  const sub = config.targetSubreddit || 'gaming';
  return sub.trim().toLowerCase().replace(/^r\//, '');
}

function getHarvestMode(): HarvestMode {
  const mode = config.harvestMode?.trim().toLowerCase();
  if (
    mode === 'posts' ||
    mode === 'comments' ||
    mode === 'both' ||
    mode === 'post' ||
    mode === 'comment'
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

// --- Main Ingestion Route ---

app.post('/internal/menu/post-create', async (c) => {
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

  return c.json({
    showToast: {
      text: `Harvest Complete: ${postCount} Post(s) & ${commentCount} Comment(s) extracted!`,
    },
  });
});

const SMOKE_SUB = 'indiaspeaks';

async function smoke(source: string): Promise<void> {
  const started = Date.now();
  const fires = Number(await redis.incrBy('smoke:count', 1));
  console.log(`SMOKE: ${new Date().toISOString()} source=${source} totalFires=${fires}`);

  try {
    const t = Date.now();
    const posts = await reddit.getNewPosts({ subredditName: SMOKE_SUB, limit: 100 }).all();
    const ms = Date.now() - t;
    const lastPost = posts[posts.length - 1];
    const oldest = lastPost ? lastPost.createdAt : null;
    const ageH = oldest ? ((Date.now() - new Date(oldest).getTime()) / 3600000).toFixed(2) : 'n/a';
    console.log(`SMOKE: r/${SMOKE_SUB} got ${posts.length} posts in ${ms}ms, oldest is ${ageH}h old`);
  } catch (err) {
    console.log(`SMOKE: reddit call FAILED: ${err}`);
  }
  console.log(`SMOKE: handler finished in ${Date.now() - started}ms`);
}

app.post('/internal/triggers/app-install', async (c) => { await smoke('onAppInstall'); return c.json({}); });
app.post('/internal/triggers/app-upgrade', async (c) => { await smoke('onAppUpgrade'); return c.json({}); });
app.post('/internal/cron/tick', async (c) => { await smoke('cron'); return c.json({}); });

serve({
  fetch: app.fetch,
  createServer,
  port: getServerPort(),
});

export default app;