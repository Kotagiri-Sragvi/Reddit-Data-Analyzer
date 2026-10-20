#!/usr/bin/env python3
"""
RunDevvit.py - launcher for the P01DA26 Devvit harvester (merged version).

Unattended use (any command-line argument => NO prompts, runs once, then exits):

    # one UTC day, posts + comments
    python RunDevvit.py -s indiaspeaks -sd 2026-10-18 -c y

    # several days in ONE run (cheaper than one run per day; split by day later)
    python RunDevvit.py -s india -sd 2026-10-01 -ed 2026-10-18 -c y

    # measure what the API really does (listing depth, comment speed, bursts)
    python RunDevvit.py -s indiaspeaks -m probe

    # original modes still work
    python RunDevvit.py -s formula1 -m both -c y

Run with no arguments for the interactive menu (as before).

The Devvit app now starts by itself (install/upgrade trigger + scheduler), so
nobody has to click anything in the playtest subreddit.

Exit codes:  0 ok | 1 error or timeout | 2 bad arguments | 3 finished, but the
             requested dates could not be fully reached (data is still saved).

NOTE: all dates and timestamps are UTC. Convert to IST later, e.g. in pandas:
      pd.to_datetime(ts, unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
"""

import argparse
import json
import os
import platform
import queue
import random
import re
import signal
import string
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

END_MARKER = "END STREAM HARVEST"
MODES = ["posts", "comments", "both", "post", "comment", "date_range", "probe"]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SUB_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")
EXIT_OK, EXIT_ERROR, EXIT_USAGE, EXIT_INCOMPLETE = 0, 1, 2, 3
DEFAULT_TIMEOUT_MIN = 120
DEFAULT_COMMENT_LIMIT = 100
DEFAULT_TIME_BUDGET_S = 20

# Markers printed by the Devvit app, one per line.
MARKERS = [
    "POST_ROW:",
    "COMMENT_ROW:",
    "POST_COMMENTS:",
    "COVERAGE:",
    "HARVEST_ERROR:",
    "FAILED_POSTS:",
    "PROBE:",
]


class UsageError(Exception):
    """Bad arguments: report and exit with code 2 (nothing is launched)."""


class StreamTimeout(Exception):
    """The overall wall-clock limit was reached while waiting for the app."""


# ----------------------------------------------------------------------
# Arguments and validation
# ----------------------------------------------------------------------


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Devvit data extraction launcher (self-starting; no manual clicks)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("-s", "--subreddit", help="subreddit name, e.g. indiaspeaks (case-insensitive)")
    p.add_argument("-m", "--mode", choices=MODES, help="harvest mode (default: date_range if a start date is given)")
    p.add_argument("-sd", "--start-date", help="UTC start date, YYYY-MM-DD (date_range)")
    p.add_argument("-ed", "--end-date", help="UTC end date, YYYY-MM-DD (default: same as start date)")
    p.add_argument("-c", "--comments", choices=["y", "n"], help="include comments (default: n)")
    p.add_argument("-cl", "--comment-limit", type=int, help=f"max comments per post (default {DEFAULT_COMMENT_LIMIT})")
    p.add_argument("-tb", "--time-budget", type=int, help=f"seconds of work per app step, 5-25 (default {DEFAULT_TIME_BUDGET_S})")
    p.add_argument("--timeout", type=int, help=f"give up after this many minutes (default {DEFAULT_TIMEOUT_MIN})")
    p.add_argument("-p", "--post-id", help="specific post ID (mode 'post')")
    p.add_argument("-ci", "--comment-id", help="specific comment ID (mode 'comment')")
    p.add_argument("-l", "--sort", choices=["hot", "top", "new", "rising"], help="feed sort for random post (mode 'post')")
    p.add_argument("-pp", "--parent-post", choices=["y", "n"], help="include parent post (mode 'comment')")
    return p.parse_args(argv)


def has_cli_args(args):
    return any(
        getattr(args, name) is not None
        for name in ("subreddit", "mode", "start_date", "end_date", "post_id", "comment_id")
    )


def validate_day(text, label):
    """Strict YYYY-MM-DD. Rejects 18-10-2026 and impossible dates like 2026-02-31."""
    t = (text or "").strip()
    if not DATE_RE.match(t):
        raise UsageError(f"{label} '{text}' must look like YYYY-MM-DD (for example 2026-10-18)")
    try:
        datetime.strptime(t, "%Y-%m-%d")
    except ValueError:
        raise UsageError(f"{label} '{t}' is not a real calendar date")
    return t


def normalize_subreddit(text):
    s = (text or "").strip()
    s = re.sub(r"^/?r/", "", s, flags=re.IGNORECASE).strip().lower()
    if not SUB_RE.match(s):
        raise UsageError(f"'{text}' is not a valid subreddit name (letters, digits, underscore; 2-21 characters)")
    return s


def today_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def build_params(args, now_utc=None):
    """Non-interactive: resolve every setting from arguments, never prompt."""
    today = now_utc or today_utc()
    if not args.subreddit:
        raise UsageError("-s/--subreddit is required")
    mode = args.mode or ("date_range" if args.start_date else None)
    if not mode:
        raise UsageError("choose a mode with -m, or give a start date with -sd")

    params = {
        "subreddit": normalize_subreddit(args.subreddit),
        "mode": mode,
        "start_date": None,
        "end_date": None,
        "include_comments": (args.comments or "n") == "y",
        "include_parent_post": (args.parent_post or "y") == "y",
        "post_id": (args.post_id or "").strip() or None,
        "comment_id": (args.comment_id or "").strip() or None,
        "sort": args.sort or "hot",
        "comment_limit": args.comment_limit if args.comment_limit is not None else DEFAULT_COMMENT_LIMIT,
        "time_budget": args.time_budget if args.time_budget is not None else DEFAULT_TIME_BUDGET_S,
        "timeout_min": args.timeout if args.timeout is not None else DEFAULT_TIMEOUT_MIN,
        "warnings": [],
    }
    if not 1 <= params["comment_limit"] <= 1000:
        raise UsageError("--comment-limit must be between 1 and 1000")
    if not 5 <= params["time_budget"] <= 25:
        raise UsageError("--time-budget must be between 5 and 25 seconds")
    if params["timeout_min"] < 1:
        raise UsageError("--timeout must be at least 1 minute")

    if mode == "date_range":
        if not args.start_date:
            raise UsageError("date_range mode needs -sd/--start-date (YYYY-MM-DD)")
        start = validate_day(args.start_date, "start date")
        end = validate_day(args.end_date, "end date") if args.end_date else start
        if end < start:
            raise UsageError(f"end date {end} is before start date {start}")
        if start > today:
            raise UsageError(f"start date {start} is in the future (today is {today} UTC)")
        params["start_date"], params["end_date"] = start, end
        if end >= today:
            params["warnings"].append(
                f"{end} is today (UTC) and not finished yet; that day will be partial."
            )
    elif mode == "post" and not params["post_id"]:
        pass  # a random post from the chosen sort is fine
    elif mode == "comment" and not params["comment_id"]:
        raise UsageError("comment mode needs -ci/--comment-id")
    return params


def ask(prompt, default=None):
    raw = input(prompt).strip()
    return raw if raw else default


def prompt_params():
    """Interactive menu (used only when the script is started with no arguments)."""
    today = today_utc()
    sub = normalize_subreddit(ask("\nEnter subreddit (e.g. indiaspeaks) [default: indiaspeaks]: ", "indiaspeaks"))
    print("\nSelect harvest mode:")
    for i, m in enumerate(MODES, 1):
        print(f"  {i}) {m}")
    choice = ask("Enter choice 1-7 [default 6 - date_range]: ", "6")
    mode = MODES[int(choice) - 1] if choice.isdigit() and 1 <= int(choice) <= len(MODES) else "date_range"

    ns = argparse.Namespace(
        subreddit=sub, mode=mode, start_date=None, end_date=None, comments=None, comment_limit=None,
        time_budget=None, timeout=None, post_id=None, comment_id=None, sort=None, parent_post=None,
    )
    if mode == "date_range":
        while True:
            try:
                ns.start_date = validate_day(ask(f"Start date YYYY-MM-DD (UTC) [default {today}]: ", today), "start date")
                ns.end_date = validate_day(ask(f"End date YYYY-MM-DD (UTC) [default {ns.start_date}]: ", ns.start_date), "end date")
                build_params(ns)  # full validation (ordering, future dates)
                break
            except UsageError as e:
                print(f"  ! {e}")
    if mode in ("date_range", "post"):
        ns.comments = "y" if ask("Include comments? (y/n) [default n]: ", "n").lower() in ("y", "yes") else "n"
    if mode == "post":
        ns.post_id = ask("Specific post ID (Enter for a random post): ")
        if not ns.post_id:
            ns.sort = ask("Sort for random post: hot/top/new/rising [default hot]: ", "hot")
    if mode == "comment":
        ns.comment_id = ask("Comment ID (e.g. k1abc23): ")
        ns.parent_post = "n" if ask("Include parent post? (y/n) [default y]: ", "y").lower() in ("n", "no") else "y"
    while True:
        try:
            return build_params(ns)
        except UsageError as e:
            print(f"  ! {e}")
            if mode == "comment":
                ns.comment_id = ask("Comment ID (e.g. k1abc23): ")
            else:
                raise


# ----------------------------------------------------------------------
# Talking to the app
# ----------------------------------------------------------------------


def new_run_id():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return stamp + "-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=4))


def write_config(config_path, params, run_id):
    payload = {
        "runId": run_id,
        "targetSubreddit": params["subreddit"],
        "harvestMode": params["mode"],
        "targetPostId": params["post_id"],
        "targetCommentId": params["comment_id"],
        "postSort": params["sort"],
        "includeComments": params["include_comments"],
        "includeParentPost": params["include_parent_post"],
        "startDate": params["start_date"],
        "endDate": params["end_date"],
        "commentLimit": params["comment_limit"],
        "timeBudgetSeconds": params["time_budget"],
    }
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def terminate_process(proc):
    """Stop the subprocess and everything it started (works on Windows and Linux/macOS)."""
    if not proc:
        return
    try:
        if platform.system() == "Windows":
            subprocess.run(
                f"taskkill /F /T /PID {proc.pid}", shell=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        else:
            try:
                pgid = os.getpgid(proc.pid)
                if pgid != os.getpgid(0):  # never signal our own group
                    os.killpg(pgid, signal.SIGTERM)  # the whole devvit process tree
                else:
                    proc.terminate()
            except Exception:
                proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
    except Exception:
        pass


def stream_lines(proc, deadline):
    """Yield output lines; raise StreamTimeout when the wall-clock limit passes."""
    q = queue.Queue()

    def reader():
        try:
            for line in iter(proc.stdout.readline, ""):
                q.put(line)
        finally:
            q.put(None)

    threading.Thread(target=reader, daemon=True).start()
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise StreamTimeout()
        try:
            item = q.get(timeout=min(1.0, remaining))
        except queue.Empty:
            continue
        if item is None:
            return
        yield item


# ----------------------------------------------------------------------
# Reading what the app prints
# ----------------------------------------------------------------------


def find_marker(line):
    """Earliest marker in the line (so a comment that *quotes* a marker is not misread)."""
    best, best_pos = None, None
    for m in MARKERS:
        pos = line.find(m)
        if pos != -1 and (best_pos is None or pos < best_pos):
            best, best_pos = m, pos
    return best, best_pos


def parse_json_after(line, marker, pos):
    text = line[pos + len(marker):].strip()
    try:
        obj, _ = json.JSONDecoder().raw_decode(text)
        return obj
    except ValueError:
        return None


class Result:
    def __init__(self):
        self.posts = []
        self.comments = []
        self.coverage = None
        self.errors = []
        self.failed_posts = []
        self.probe_lines = []
        self.reported_total = 0
        self.returned_total = 0
        self.posts_fetched = 0
        self.finished = False
        self.timed_out = False


def collect(lines, echo=lambda s: print(s, flush=True)):
    """Consume app output until the END marker. Pure function: easy to test."""
    r = Result()
    try:
        for raw in lines:
            line = raw.rstrip("\r\n")
            echo(line)
            marker, pos = find_marker(line)
            if marker == "POST_ROW:":
                obj = parse_json_after(line, marker, pos)
                if isinstance(obj, dict):
                    r.posts.append(obj)
            elif marker == "COMMENT_ROW:":
                obj = parse_json_after(line, marker, pos)
                if isinstance(obj, dict):
                    r.comments.append(obj)
            elif marker == "POST_COMMENTS:":
                obj = parse_json_after(line, marker, pos)
                if isinstance(obj, dict):
                    r.posts_fetched += 1
                    r.reported_total += int(obj.get("reported", 0) or 0)
                    r.returned_total += int(obj.get("returned", 0) or 0)
            elif marker == "COVERAGE:":
                obj = parse_json_after(line, marker, pos)
                if isinstance(obj, dict):
                    r.coverage = obj
            elif marker == "HARVEST_ERROR:":
                r.errors.append(line[pos + len(marker):].strip())
            elif marker == "FAILED_POSTS:":
                obj = parse_json_after(line, marker, pos)
                if isinstance(obj, list):
                    r.failed_posts = [str(x) for x in obj]
            elif marker == "PROBE:":
                r.probe_lines.append(line[pos:])
            if END_MARKER in line:
                r.finished = True
                break
    except StreamTimeout:
        r.timed_out = True
    return r


def dedupe(rows, key):
    """Keep the first copy of each ID. (A step that is retried may print rows twice.)"""
    seen, out = set(), []
    for row in rows:
        k = row.get(key)
        if k is not None and k in seen:
            continue
        seen.add(k)
        out.append(row)
    return out, len(rows) - len(out)


# ----------------------------------------------------------------------
# Saving
# ----------------------------------------------------------------------


def save_results(output_dir, params, run_id, result):
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sub, mode = params["subreddit"], params["mode"]

    if mode == "probe":
        path = output_dir / f"probe_{sub}_{stamp}.txt"
        path.write_text("\n".join(result.probe_lines) + "\n", encoding="utf-8")
        print(f"\n[saved] probe report -> {path}")
        return [path]

    posts, dup_posts = dedupe(result.posts, "post ID")
    comments, dup_comments = dedupe(result.comments, "comment ID")
    sd, ed = params["start_date"], params["end_date"]
    date_tag = f"_{sd}_to_{ed}" if sd and ed else ""
    base = output_dir / f"harvest_{sub}_{mode}{date_tag}_{stamp}"

    coverage_complete = None if result.coverage is None else bool(result.coverage.get("complete"))
    metadata = {
        "subreddit": sub,
        "mode": mode,
        "run_id": run_id,
        "start_date_utc": sd,
        "end_date_utc": ed,
        "harvested_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "timestamp": stamp,
        "timezone_note": "All dates are UTC; 'creation timestamp' and 'timestamp' are Unix seconds (UTC).",
        "total_posts": len(posts),
        "total_comments": len(comments),
        "duplicates_removed": {"posts": dup_posts, "comments": dup_comments},
        "coverage_complete": coverage_complete,
        "coverage": result.coverage,
        "run_finished": result.finished and not result.timed_out,
        "errors": result.errors,
    }
    if mode == "date_range":
        metadata["comment_scope"] = (
            f"Up to {params['comment_limit']} comments per post, for posts CREATED inside the window, "
            f"as they were at harvested_at_utc. Comments written inside the window on OLDER posts are not "
            f"included; replies written after the window to in-window posts are."
            if params["include_comments"]
            else "Comments were not requested."
        )
        metadata["comment_limit_per_post"] = params["comment_limit"] if params["include_comments"] else None
        metadata["comment_fetch"] = {
            "posts_fetched": result.posts_fetched,
            "comments_reddit_reports": result.reported_total,
            "comments_returned": result.returned_total,
        }
        metadata["posts_with_failed_comment_fetch"] = result.failed_posts

    payload = {"metadata": metadata, "posts": posts, "comments": comments}
    json_path, txt_path = base.with_suffix(".json"), base.with_suffix(".txt")
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    with txt_path.open("w", encoding="utf-8") as f:
        f.write(f"=== HARVEST REPORT: r/{sub} [{mode.upper()}] {sd or ''} {ed or ''} - {stamp} ===\n\n")
        if posts:
            f.write("--- POSTS ---\n")
            for p in posts:
                f.write(f"[{p.get('post ID')}] {p.get('title')}\n")
                f.write(f"Author: {p.get('author')} | Score: {p.get('score/upvotes')} | Comments: {p.get('number of comments')}\n")
                f.write(f"URL: {p.get('URL/permalink')}\n")
                if p.get("body/self-text"):
                    f.write(f"Body: {p.get('body/self-text')}\n")
                f.write("-" * 50 + "\n")
        if comments:
            f.write("\n--- COMMENTS ---\n")
            for c in comments:
                badge = f" [{str(c.get('role')).upper()}]" if c.get("role") else ""
                f.write(f"[{c.get('comment ID')}]{badge} Post: {c.get('parent post ID')} | Author: {c.get('author')} | Score: {c.get('score/upvotes')}\n")
                f.write(f"Text: {c.get('comment text')}\n")
                f.write("-" * 50 + "\n")
    print(f"\n[saved] {len(posts)} posts, {len(comments)} comments (removed {dup_posts}+{dup_comments} duplicates)")
    print(f"        -> {json_path}")
    print(f"        -> {txt_path}")
    return [json_path, txt_path]


def exit_code_for(result, params):
    if result.errors or result.timed_out or not result.finished:
        return EXIT_ERROR
    if params["mode"] == "date_range" and result.coverage is not None and not result.coverage.get("complete"):
        return EXIT_INCOMPLETE
    return EXIT_OK


def print_summary(result, params, code):
    print("\n" + "=" * 66)
    if result.timed_out:
        print(f"[!] Timed out after {params['timeout_min']} minutes. Partial data was saved.")
    elif not result.finished:
        print("[!] The app stopped printing before the END marker. Partial data was saved.")
    for e in result.errors:
        print(f"[!] App error: {e}")
    if result.failed_posts:
        print(f"[!] Comments could not be fetched for {len(result.failed_posts)} post(s): {result.failed_posts[:5]}")
    if result.coverage is not None:
        c = result.coverage
        if c.get("complete"):
            print(f"[ok] Coverage complete: scanned {c.get('scanned')} posts, reached posts older than the window.")
        else:
            print(
                f"[!] COVERAGE INCOMPLETE ({c.get('reason')}): scanned {c.get('scanned')} posts, oldest reached "
                f"{c.get('oldestScannedUtc')}, window starts {c.get('windowStartUtc')}. Earlier posts are "
                f"unreachable through the live API; use an archive source for them."
            )
    print(f"exit code {code}")
    print("=" * 66)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------


def run_once(params, base_dir):
    target_dir = base_dir / "p01am26-flow"
    output_dir = base_dir / "output"
    if not target_dir.exists():
        print(f"[x] Target folder not found: {target_dir}")
        return EXIT_ERROR

    run_id = new_run_id()
    print("=" * 66)
    print("DEVVIT DATA EXTRACTION LAUNCHER (self-starting)")
    print(f"  subreddit : r/{params['subreddit']}")
    print(f"  mode      : {params['mode']}   run id: {run_id}")
    if params["mode"] == "date_range":
        print(f"  window    : {params['start_date']} -> {params['end_date']} (UTC, whole days)")
        print(f"  comments  : {'yes, up to ' + str(params['comment_limit']) + ' per post' if params['include_comments'] else 'no'}")
    print(f"  time limit: {params['timeout_min']} min")
    for w in params["warnings"]:
        print(f"  [!] {w}")
    print("=" * 66, flush=True)

    write_config(target_dir / "src" / "server" / "config.json", params, run_id)
    command = os.environ.get("DEVVIT_CMD", "npx devvit playtest")
    print(f"\nStarting `{command}` - the app starts the harvest by itself; you do not need to click anything.\n", flush=True)

    proc = subprocess.Popen(
        command, cwd=target_dir, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
        start_new_session=(platform.system() != "Windows"),
    )
    result = Result()
    try:
        deadline = time.time() + params["timeout_min"] * 60
        result = collect(stream_lines(proc, deadline))
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C). Saving what was captured so far...")
        terminate_process(proc)
        save_results(output_dir, params, run_id, result)
        raise
    finally:
        terminate_process(proc)

    save_results(output_dir, params, run_id, result)
    code = exit_code_for(result, params)
    print_summary(result, params, code)
    return code


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = parse_args(argv)
    base_dir = Path(__file__).resolve().parent

    if has_cli_args(args):
        try:
            params = build_params(args)
        except UsageError as e:
            print(f"[x] {e}")
            return EXIT_USAGE
        try:
            return run_once(params, base_dir)
        except KeyboardInterrupt:
            return EXIT_ERROR

    # Interactive mode: ask, run, ask again.
    while True:
        try:
            params = prompt_params()
            run_once(params, base_dir)
            print("\nReady for the next harvest (Ctrl+C to exit).")
        except UsageError as e:
            print(f"[x] {e}")
        except KeyboardInterrupt:
            print("\nBye.")
            return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())