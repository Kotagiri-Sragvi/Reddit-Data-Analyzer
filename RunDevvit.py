import argparse
import json
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime


def parse_args():
    """Parse CLI arguments for non-interactive execution."""
    parser = argparse.ArgumentParser(
        description="Devvit Data Extraction Pipeline Launcher"
    )
    parser.add_argument(
        "-s",
        "--subreddit",
        type=str,
        help="Target subreddit name (e.g., technology, gaming, formula1)",
    )
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        choices=["posts", "comments", "both", "post"],
        help="Harvest mode: 'posts', 'comments', 'both', or 'post'",
    )
    parser.add_argument(
        "-p",
        "--post-id",
        type=str,
        help="Specific post ID (for mode 'post')",
    )
    parser.add_argument(
        "-l",
        "--sort",
        type=str,
        choices=["hot", "top", "new", "rising"],
        help="Feed sort filter if no post ID is supplied (hot, top, new, rising)",
    )
    parser.add_argument(
        "-c",
        "--comments",
        type=str,
        choices=["y", "n"],
        help="Include comment section? ('y' or 'n')",
    )
    return parser.parse_args()


def get_parameters(args):
    """Resolve parameters via CLI arguments or interactive prompts."""
    # 1. Subreddit
    subreddit = args.subreddit
    if not subreddit:
        raw_sub = input(
            "\n🎯 Enter Subreddit Target (e.g. technology, formula1) [Default: formula1]: "
        ).strip()
        subreddit = raw_sub if raw_sub else "formula1"

    subreddit = subreddit.lower().replace("r/", "").strip()

    # 2. Harvest Mode
    mode = args.mode
    if not mode:
        print("\n📊 Select Harvest Mode:")
        print("  1) posts (bulk)")
        print("  2) comments (bulk)")
        print("  3) both (bulk)")
        print("  4) post (single post)")
        choice = input("👉 Enter choice (1-4) [Default: 4 - post]: ").strip()
        mode_map = {"1": "posts", "2": "comments", "3": "both", "4": "post"}
        mode = mode_map.get(choice, "post")

    harvest_mode = mode.lower().strip()
    target_post_id = None
    post_sort = "hot"
    include_comments = False

    if harvest_mode == "post":
        # 3. Post ID Selection
        if args.post_id is not None:
            raw_id = args.post_id.strip()
        else:
            raw_id = input(
                "\n🆔 Enter a specific post ID if available (enter 'n' or press Enter for a random post): "
            ).strip()

        if raw_id and raw_id.lower() != "n":
            target_post_id = raw_id
        else:
            target_post_id = None

            # 4. Feed Sort Selection (if no specific Post ID given)
            if args.sort:
                post_sort = args.sort.lower().strip()
            else:
                print("\n🔥 Select Feed Sort Filter for Random Post Selection:")
                print("  1) hot")
                print("  2) top")
                print("  3) new")
                print("  4) rising")
                sort_choice = input("👉 Enter choice (1-4) [Default: 1 - hot]: ").strip()
                sort_map = {"1": "hot", "2": "top", "3": "new", "4": "rising"}
                post_sort = sort_map.get(sort_choice, "hot")

        # 5. Comment Section Toggle
        if args.comments is not None:
            include_comments = args.comments.lower().strip() == "y"
        else:
            raw_comments = input(
                "💬 Is the comment section to be included? (y/n) [Default: n]: "
            ).strip().lower()
            include_comments = raw_comments in ["y", "yes"]

    return subreddit, harvest_mode, target_post_id, post_sort, include_comments


def save_extracted_data(output_dir, subreddit, harvest_mode, posts, comments):
    """Saves extracted posts and comments into JSON and TXT format."""
    if not posts and not comments:
        print("\n⚠️ No structured records captured during this session.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"harvest_{subreddit}_{harvest_mode}_{timestamp}.json"
    txt_path = output_dir / f"harvest_{subreddit}_{harvest_mode}_{timestamp}.txt"

    # Save JSON File
    data_payload = {
        "metadata": {
            "subreddit": subreddit,
            "mode": harvest_mode,
            "timestamp": timestamp,
            "total_posts": len(posts),
            "total_comments": len(comments),
        },
        "posts": posts,
        "comments": comments,
    }

    try:
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(data_payload, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Saved JSON dataset ({len(posts)} posts, {len(comments)} comments):")
        print(f"   -> {json_path}")
    except Exception as e:
        print(f"❌ Failed to save JSON dataset: {e}")

    # Save TXT Summary File
    try:
        with txt_path.open("w", encoding="utf-8") as f:
            f.write(f"=== HARVEST REPORT: r/{subreddit} [{harvest_mode.upper()}] - {timestamp} ===\n\n")

            if posts:
                f.write("--- POSTS ---\n")
                for p in posts:
                    f.write(f"[{p.get('post ID')}] {p.get('title')}\n")
                    f.write(
                        f"Author: {p.get('author')} | Score: {p.get('score/upvotes')} | Comments: {p.get('number of comments')}\n")
                    f.write(f"URL: {p.get('URL/permalink')}\n")
                    if p.get('body/self-text'):
                        f.write(f"Body: {p.get('body/self-text')}\n")
                    f.write("-" * 50 + "\n")

            if comments:
                f.write("\n--- COMMENTS ---\n")
                for c in comments:
                    f.write(
                        f"[{c.get('comment ID')}] Post: {c.get('parent post ID')} | Author: {c.get('author')} | Score: {c.get('score/upvotes')}\n")
                    f.write(f"Text: {c.get('comment text')}\n")
                    f.write("-" * 50 + "\n")

        print(f"📝 Saved TXT summary:")
        print(f"   -> {txt_path}\n")
    except Exception as e:
        print(f"❌ Failed to save TXT report: {e}")


def extract_json_object(line_text, prefix):
    """Safely isolates and parses JSON objects from console output lines."""
    try:
        idx = line_text.find(prefix) + len(prefix)
        raw_json = line_text[idx:].strip()
        start = raw_json.find('{')
        end = raw_json.rfind('}')
        if start != -1 and end != -1:
            clean_json_str = raw_json[start:end + 1]
            return json.loads(clean_json_str)
    except Exception:
        pass
    return None


def launch_devvit():
    args = parse_args()

    base_dir = (
        Path(__file__).resolve().parent
        if "__file__" in globals()
        else Path.cwd()
    )
    target_dir = base_dir / "p01am26-flow"
    output_dir = base_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not target_dir.exists():
        print(f"❌ Error: Target folder not found at: '{target_dir}'")
        sys.exit(1)

    subreddit, harvest_mode, target_post_id, post_sort, include_comments = get_parameters(args)

    print("=" * 66, flush=True)
    print("🚀 DEVVIT DATA EXTRACTION LAUNCHER", flush=True)
    print(f"📁 Target Directory : {target_dir}", flush=True)
    print(f"🎯 Target Subreddit : r/{subreddit}", flush=True)
    print(f"📊 Harvest Mode     : {harvest_mode}", flush=True)
    if harvest_mode == "post":
        print(f"🆔 Target Post ID   : {target_post_id or f'[Random {post_sort.upper()} Post]'}", flush=True)
        print(f"💬 Include Comments : {include_comments}", flush=True)
    print(f"💾 Output Folder    : {output_dir}", flush=True)
    print("=" * 66, flush=True)

    # Inject config into src/server/config.json
    server_dir = target_dir / "src" / "server"
    server_dir.mkdir(parents=True, exist_ok=True)

    config_json_path = server_dir / "config.json"
    config_payload = {
        "targetSubreddit": subreddit,
        "harvestMode": harvest_mode,
        "targetPostId": target_post_id,
        "postSort": post_sort,
        "includeComments": include_comments,
    }

    try:
        with config_json_path.open("w", encoding="utf-8") as f:
            json.dump(config_payload, f, indent=2)
        print("⚙️ Updated src/server/config.json successfully.", flush=True)
    except Exception as e:
        print(f"⚠️ Warning: Could not write src/server/config.json: {e}", flush=True)

    command = "npx devvit playtest"
    print(f"\n⚡ Executing `{command}`...\n", flush=True)
    print("-" * 66, flush=True)

    captured_posts = []
    captured_comments = []

    try:
        process = subprocess.Popen(
            command,
            cwd=target_dir,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1
        )

        for line in iter(process.stdout.readline, ""):
            print(line, end="", flush=True)

            if "POST_ROW:" in line:
                post_data = extract_json_object(line, "POST_ROW:")
                if post_data:
                    captured_posts.append(post_data)

            elif "COMMENT_ROW:" in line:
                comment_data = extract_json_object(line, "COMMENT_ROW:")
                if comment_data:
                    captured_comments.append(comment_data)

            elif "END STREAM HARVEST" in line:
                save_extracted_data(output_dir, subreddit, harvest_mode, captured_posts, captured_comments)
                captured_posts = []
                captured_comments = []

        process.wait()

    except KeyboardInterrupt:
        print("\n\n⚠️ Process interrupted by user (Ctrl+C).", flush=True)
        if captured_posts or captured_comments:
            save_extracted_data(output_dir, subreddit, harvest_mode, captured_posts, captured_comments)
    except Exception as e:
        print(f"\n❌ Execution Error: {e}", flush=True)


if __name__ == "__main__":
    launch_devvit()