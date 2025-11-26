#!/usr/bin/env python3
"""
yt_polling_v2.py

Lightweight, safe YouTube polling script.

Features:
 - Test mode (--mode test) does one API call to verify credentials and prints an estimated quota
 - Run mode (--mode run) polls videos at an interval (default 60s) for T0 hours and saves per-video CSVs
 - Batches up to 50 ids per videos.list call (efficient)
 - Writes data to data/<video_id>/polls.csv
 - Exponential backoff on transient API errors (429/5xx), with safe retry limits
 - Simple quota estimator and --max_calls safeguard (abort if estimated calls exceed threshold)
 - Uses credentials from config/token.json (preferred) or credentials.json fallback

Usage examples:
  # test single call
  python yt_polling_v2.py --mode test --video <VIDEO_ID>

  # run polling for 3 hours, 60s interval, save under data/, prompt required
  python yt_polling_v2.py --mode run --video <VIDEO_ID> --interval 60 --t0_hours 3 --out_dir data

  # run multiple videos
  python yt_polling_v2.py --mode run --video id1 id2 --interval 60 --t0_hours 3 --out_dir data

Notes:
 - Requires google-api-python-client and google-auth packages in your venv:
   pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
 - Keep credentials private. Do not commit token files to GitHub.
"""
from __future__ import annotations
import argparse
import csv
import datetime
import json
import math
import os
import sys
import time
from typing import List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

# Constants
DEFAULT_INTERVAL = 60         # seconds between polls
DEFAULT_T0_HOURS = 3          # default poll window in hours
MAX_BATCH = 50                # videos.list supports up to 50 ids
DEFAULT_OUTDIR = "data"
# Assume 1 quota unit per videos.list call (typical): confirm in GCP console if you changed quotas
QUOTA_PER_CALL = 1

# credential search order
CREDENTIAL_PATHS = ["config/token.json", "credentials.json"]


def find_credentials_path():
    for p in CREDENTIAL_PATHS:
        if os.path.exists(p):
            return p
    return None


def load_creds(path: str) -> Credentials:
    return Credentials.from_authorized_user_file(path, scopes=["https://www.googleapis.com/auth/youtube.readonly"])


def build_service(creds: Credentials):
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


def videos_list_stats(service, video_ids: List[str]):
    # returns dict video_id -> record
    resp = service.videos().list(part="statistics,snippet,contentDetails", id=",".join(video_ids)).execute()
    out = {}
    for it in resp.get("items", []):
        vid = it.get("id")
        stats = it.get("statistics", {})
        snippet = it.get("snippet", {})
        cd = it.get("contentDetails", {})
        out[vid] = {
            "viewCount": int(stats.get("viewCount", 0)) if stats.get("viewCount") is not None else None,
            "likeCount": int(stats.get("likeCount", 0)) if stats.get("likeCount") is not None else None,
            "commentCount": int(stats.get("commentCount", 0)) if stats.get("commentCount") is not None else None,
            "publishedAt": snippet.get("publishedAt"),
            "title": snippet.get("title"),
            "duration": cd.get("duration"),
        }
    return out


def ensure_out_dirs(out_dir: str, video_ids: List[str]):
    os.makedirs(out_dir, exist_ok=True)
    for vid in video_ids:
        d = os.path.join(out_dir, vid)
        os.makedirs(d, exist_ok=True)


def csv_headers():
    return ["video_id", "ts_utc", "view_count", "like_count", "comment_count", "title", "publishedAt"]


def append_rows_to_csv(out_dir: str, snapshots: dict):
    # snapshots: vid -> record (as returned by videos_list_stats)
    ts = datetime.datetime.utcnow().isoformat()
    for vid, rec in snapshots.items():
        d = os.path.join(out_dir, vid)
        os.makedirs(d, exist_ok=True)
        csv_path = os.path.join(d, "polls.csv")
        header_needed = not os.path.exists(csv_path)
        with open(csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            if header_needed:
                writer.writerow(csv_headers())
            writer.writerow([vid, ts,
                             rec.get("viewCount") if rec.get("viewCount") is not None else "",
                             rec.get("likeCount") if rec.get("likeCount") is not None else "",
                             rec.get("commentCount") if rec.get("commentCount") is not None else "",
                             rec.get("title", ""), rec.get("publishedAt", "")])


def estimate_calls(num_videos: int, poll_interval_sec: int, t0_hours: float) -> int:
    # Because we batch all ids per poll interval into a single videos.list call, calls = number of intervals
    intervals = math.ceil((t0_hours * 3600) / poll_interval_sec)
    return intervals


def do_test_mode(service, video_ids: List[str], interval: int, t0_hours: float):
    # Single API call + estimate
    sample_ids = video_ids[:MAX_BATCH]
    print("TEST MODE: making one videos.list call for sample video(s):", sample_ids)
    try:
        resp = videos_list_stats(service, sample_ids)
    except HttpError as e:
        print("API error during test call:", e)
        raise
    print("Sample response (trimmed):")
    for vid, rec in resp.items():
        print(f"  {vid} -> views={rec.get('viewCount')} publishedAt={rec.get('publishedAt')}")
    est_calls = estimate_calls(len(video_ids), interval, t0_hours)
    est_quota = est_calls * QUOTA_PER_CALL
    print(f"\nEstimated total calls for this run (batching all ids into 1 call per interval): {est_calls}")
    print(f"Estimated quota units (assuming {QUOTA_PER_CALL} per call): {est_quota}")
    print("If you are comfortable, run with --mode run. Also verify your quota in GCP Console -> APIs & Services -> Quotas.")


def run_polling_loop(service, video_ids: List[str], out_dir: str, interval: int, t0_hours: float, max_calls: int | None):
    ensure_out_dirs(out_dir, video_ids)
    start = datetime.datetime.utcnow()
    end = start + datetime.timedelta(hours=t0_hours)
    intervals = estimate_calls(len(video_ids), interval, t0_hours)
    print(f"Polling will start at {start.isoformat()} UTC and stop at {end.isoformat()} UTC")
    print(f"Planned intervals (calls): {intervals}; polling interval: {interval}s; batching up to {MAX_BATCH} IDs/call")
    if max_calls is not None and intervals > max_calls:
        print(f"Aborting: planned calls {intervals} > --max_calls {max_calls}")
        return

    # split into batches of up to MAX_BATCH per call (we'll request all video ids in one call per interval,
    # but if someone passes >50 ids we still support batching across calls)
    vid_batches = [video_ids[i:i + MAX_BATCH] for i in range(0, len(video_ids), MAX_BATCH)]

    call_count = 0
    backoff_initial = 2.0
    max_retries = 6

    while datetime.datetime.utcnow() < end:
        ts_now = datetime.datetime.utcnow().isoformat()
        for batch in vid_batches:
            if datetime.datetime.utcnow() >= end:
                break
            attempt = 0
            while True:
                try:
                    snaps = videos_list_stats(service, batch)
                    append_rows_to_csv(out_dir, snaps)
                    call_count += 1
                    print(f"[{ts_now}] Polled batch ({len(batch)} ids) - call #{call_count}; saved to {out_dir}")
                    break
                except HttpError as e:
                    status = None
                    try:
                        status = e.resp.status
                    except Exception:
                        status = None
                    attempt += 1
                    if attempt > max_retries:
                        print(f"Max retries exceeded for batch {batch}. Skipping this batch for this interval. Error:", e)
                        break
                    wait = backoff_initial * (2 ** (attempt - 1))
                    print(f"HttpError (status={status}) on attempt {attempt}. Backing off {wait}s. Error: {e}")
                    time.sleep(wait)
            # after batch done, continue to next batch (or finish)
        # sleep until next interval
        remaining = (datetime.datetime.utcnow() - start).total_seconds() % interval
        # simple sleep for interval (not perfect scheduling)
        print(f"Sleeping {interval}s until next poll interval...")
        time.sleep(interval)
    print("Polling window complete.")


def main():
    p = argparse.ArgumentParser(prog="yt_polling_v2.py")
    p.add_argument("--mode", choices=["test", "run"], required=True,
                   help="test: one-call sample + estimate; run: perform polling loop")
    p.add_argument("--video", required=True, nargs="+", help="one or more video IDs to monitor")
    p.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="poll interval in seconds (default 60)")
    p.add_argument("--t0_hours", type=float, default=DEFAULT_T0_HOURS, help="how many hours to poll each video")
    p.add_argument("--out_dir", default=DEFAULT_OUTDIR, help="output data directory (default: data/)")
    p.add_argument("--max_calls", type=int, default=None, help="safeguard: abort run if estimated calls exceed this")
    args = p.parse_args()

    # find credentials
    cred_path = find_credentials_path()
    if cred_path is None:
        print("No credentials found. Put your authorized token at one of these paths:", CREDENTIAL_PATHS)
        sys.exit(2)

    creds = load_creds(cred_path)
    service = build_service(creds)

    # ensure video ids look reasonable
    vids = args.video
    if any(len(v) < 8 for v in vids):
        print("Warning: some video IDs look short. Make sure you passed proper YouTube video IDs (usually 11+ chars).")

    # test or run
    if args.mode == "test":
        do_test_mode(service, vids, args.interval, args.t0_hours)
    else:
        est_calls = estimate_calls(len(vids), args.interval, args.t0_hours)
        est_quota = est_calls * QUOTA_PER_CALL
        print("About to start polling with parameters:")
        print(f"  videos: {vids}")
        print(f"  interval: {args.interval}s   t0_hours: {args.t0_hours}")
        print(f"  estimated calls: {est_calls}   estimated quota units: {est_quota}")
        print(f"  output directory: {args.out_dir}")
        print("Important: confirm your quota in GCP Console -> APIs & Services -> Quotas before proceeding.")
        confirm = input("Type YES to proceed (or anything else to cancel): ").strip()
        if confirm != "YES":
            print("Cancelled by user.")
            return
        run_polling_loop(service, vids, args.out_dir, args.interval, args.t0_hours, args.max_calls)


if __name__ == "__main__":
    main()