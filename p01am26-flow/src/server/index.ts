import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { createServer, getServerPort, reddit } from '@devvit/web/server';
// @ts-ignore
import rawConfig from './config.json';

// --- Type Definitions ---
interface ConfigPayload {
  targetSubreddit?: string;
  harvestMode?: string;
  targetPostId?: string | null;
  includeComments?: boolean;
  postSort?: 'hot' | 'top' | 'new' | 'rising';
}

const config = rawConfig as ConfigPayload;

const app: Hono = new Hono();

type HarvestMode = 'posts' | 'comments' | 'both' | 'post';
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
  if (mode === 'posts' || mode === 'comments' || mode === 'both' || mode === 'post') {
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

function getIncludeComments(): boolean {
  return Boolean(config.includeComments);
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
  const INCLUDE_COMMENTS = getIncludeComments();
  const POST_SORT = getPostSort();

  console.log(`\n==================== START STREAM HARVEST: r/${TARGET_SUBREDDIT} [MODE: ${HARVEST_MODE.toUpperCase()}] ====================`);
  await flushStdout(30);

  let postCount = 0;
  let commentCount = 0;

  try {
    if (HARVEST_MODE === 'post') {
      let targetPost: any = null;

      // 1. Fetch specific post if ID is supplied
      if (TARGET_POST_ID) {
        try {
          console.log(`🔍 Fetching specific post ID: ${TARGET_POST_ID}...`);
          targetPost = await reddit.getPostById(TARGET_POST_ID as `t3_${string}`);
        } catch (e) {
          console.warn(`⚠️ Post ID ${TARGET_POST_ID} not found or inaccessible. Falling back to feed selection.`);
        }
      }

      // 2. Select random post from requested feed sort if no ID was given or fetch failed
      if (!targetPost) {
        console.log(`🎲 Selecting a random ${POST_SORT.toUpperCase()} post from r/${TARGET_SUBREDDIT}...`);
        let fetchedPosts: any[] = [];

        if (POST_SORT === 'top') {
          fetchedPosts = await reddit.getTopPosts({ subredditName: TARGET_SUBREDDIT, limit: 25 }).all();
        } else if (POST_SORT === 'new') {
          fetchedPosts = await reddit.getNewPosts({ subredditName: TARGET_SUBREDDIT, limit: 25 }).all();
        } else if (POST_SORT === 'rising') {
          fetchedPosts = await reddit.getRisingPosts({ subredditName: TARGET_SUBREDDIT, limit: 25 }).all();
        } else {
          fetchedPosts = await reddit.getHotPosts({ subredditName: TARGET_SUBREDDIT, limit: 25 }).all();
        }

        if (fetchedPosts.length > 0) {
          const randomIndex = Math.floor(Math.random() * fetchedPosts.length);
          targetPost = fetchedPosts[randomIndex];
        } else {
          console.error(`❌ No ${POST_SORT} posts found in r/${TARGET_SUBREDDIT}`);
        }
      }

      // 3. Output post record
      if (targetPost) {
        console.log('\n###--- START_POSTS_EXPORT ---###');
        await flushStdout(30);

        const postRow: ExtractedPost = {
          'post ID': targetPost.id,
          'subreddit name': targetPost.subredditName || TARGET_SUBREDDIT,
          title: targetPost.title,
          'body/self-text': targetPost.body || '',
          author: targetPost.authorName ?? '[deleted]',
          'creation timestamp': targetPost.createdAt ? Math.floor(new Date(targetPost.createdAt).getTime() / 1000) : 0,
          'score/upvotes': targetPost.score,
          'number of comments': targetPost.numberOfComments,
          'URL/permalink': `https://reddit.com${targetPost.permalink}`,
        };

        console.log(`POST_ROW:${JSON.stringify(postRow)}`);
        postCount++;
        await flushStdout(30);
        console.log('###--- END_POSTS_EXPORT ---###\n');

        // 4. Optionally harvest comments for this post
        if (INCLUDE_COMMENTS && targetPost.numberOfComments > 0) {
          try {
            console.log('###--- START_COMMENTS_EXPORT ---###');
            await flushStdout(30);

            const comments = await reddit.getComments({
              postId: targetPost.id,
              limit: 50,
            }).all();

            for (const comment of comments) {
              const commentRow: ExtractedComment = {
                'comment ID': comment.id,
                'parent post ID': comment.postId,
                'parent comment ID': comment.parentId,
                subreddit: comment.subredditName || TARGET_SUBREDDIT,
                'comment text': comment.body,
                author: comment.authorName ?? '[deleted]',
                timestamp: comment.createdAt ? Math.floor(new Date(comment.createdAt).getTime() / 1000) : 0,
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
      // Legacy multi-post harvesting logic ('posts', 'comments', or 'both')
      const shouldExportPosts = HARVEST_MODE === 'posts' || HARVEST_MODE === 'both';
      const shouldExportComments = HARVEST_MODE === 'comments' || HARVEST_MODE === 'both';
      const MAX_COMMENTS_TOTAL = 550;

      const posts = await reddit.getHotPosts({
        subredditName: TARGET_SUBREDDIT,
        limit: 100,
      }).all();

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
            'creation timestamp': post.createdAt ? Math.floor(new Date(post.createdAt).getTime() / 1000) : 0,
            'score/upvotes': post.score,
            'number of comments': post.numberOfComments,
            'URL/permalink': `https://reddit.com${post.permalink}`,
          };
          console.log(`POST_ROW:${JSON.stringify(postRow)}`);
          postCount++;

          if (postCount % 10 === 0) await flushStdout(20);
        }

        if (shouldExportComments && commentCount < MAX_COMMENTS_TOTAL && post.numberOfComments > 0) {
          try {
            const comments = await reddit.getComments({
              postId: post.id,
              limit: 50,
            }).all();

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
                timestamp: comment.createdAt ? Math.floor(new Date(comment.createdAt).getTime() / 1000) : 0,
                'score/upvotes': comment.score,
              };
              console.log(`COMMENT_ROW:${JSON.stringify(commentRow)}`);
              commentCount++;

              if (commentCount % 20 === 0) await flushStdout(20);
            }
          } catch (commentError) {
            console.warn(`⚠️ Warning: Failed to fetch comments for post ${post.id}: ${commentError}`);
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

    console.log(`📊 Verification Tally -> Streamed ${postCount} Post(s) & ${commentCount} Comment(s).`);
    await flushStdout(30);

  } catch (error) {
    console.error(`❌ Network extraction failure: ${error}`);
  }

  console.log(`==================== END STREAM HARVEST: r/${TARGET_SUBREDDIT} ====================\n`);

  return c.json({
    showToast: {
      text: `Harvest Complete: ${postCount} Post(s) & ${commentCount} Comment(s) extracted!`,
    },
  });
});

serve({
  fetch: app.fetch,
  createServer,
  port: getServerPort(),
});

export default app;