"""
fetch_gharchive.py

Downloads GH Archive hourly files and uploads them into a Databricks Volume.
It runs on a GitHub Actions server, so nothing depends on your laptop and
Databricks never has to reach the internet.

Modes
  incremental : the last N complete hours (default 24), skipping files already in the Volume.
  backfill    : every hour from --start-hour to --end-hour (inclusive).

Examples
  python scripts/fetch_gharchive.py --mode incremental --catchup-hours 24
  python scripts/fetch_gharchive.py --mode backfill --start-hour 2026-10-01-00 --end-hour 2026-10-05-23
  python scripts/fetch_gharchive.py --mode backfill --start-hour 2026-10-01-00 --end-hour 2026-10-01-02 --local-dir tmp_download

Environment variables (not needed with --local-dir)
  DATABRICKS_HOST    e.g. https://dbc-xxxx.cloud.databricks.com
  DATABRICKS_TOKEN   personal access token
  VOLUME_PATH        optional, default /Volumes/workspace/gharchive/raw
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

GHARCHIVE_URL = "https://data.gharchive.org/{name}.json.gz"
DEFAULT_VOLUME_PATH = "/Volumes/workspace/gharchive/raw"
# GH Archive needs a little time to publish an hour. We stay this many hours behind "now".
PUBLISH_LAG_HOURS = 2


def archive_name(moment):
    """GH Archive file names do NOT zero pad the hour: 2026-10-05-3 (not 2026-10-05-03)."""
    return moment.strftime("%Y-%m-%d") + "-" + str(moment.hour)


def parse_hour(text):
    """'2026-10-05-03' or '2026-10-05-3' -> datetime in UTC."""
    parts = text.strip().split("-")
    if len(parts) != 4:
        raise ValueError("Hour must look like 2026-10-05-03, got: " + text)
    return datetime(int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), tzinfo=timezone.utc)


def hours_between(start, end):
    """Every full hour from start to end, inclusive."""
    hours = []
    current = start
    while current <= end:
        hours.append(current)
        current = current + timedelta(hours=1)
    return hours


def choose_hours(args):
    if args.mode == "backfill":
        if not args.start_hour or not args.end_hour:
            raise ValueError("backfill needs --start-hour and --end-hour")
        return hours_between(parse_hour(args.start_hour), parse_hour(args.end_hour))
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    last_hour = now - timedelta(hours=PUBLISH_LAG_HOURS)
    first_hour = last_hour - timedelta(hours=args.catchup_hours - 1)
    return hours_between(first_hour, last_hour)


class Databricks:
    """Tiny wrapper around the Databricks Files API."""

    def __init__(self, host, token, volume_path):
        self.host = host.rstrip("/")
        self.headers = {"Authorization": "Bearer " + token}
        self.volume_path = volume_path.rstrip("/")

    def file_url(self, file_name):
        return self.host + "/api/2.0/fs/files" + self.volume_path + "/" + file_name

    def exists(self, file_name):
        response = requests.head(self.file_url(file_name), headers=self.headers, timeout=60)
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        raise RuntimeError("Unexpected answer while checking " + file_name + ": " + str(response.status_code))

    def upload(self, file_name, local_path):
        with open(local_path, "rb") as f:
            response = requests.put(
                self.file_url(file_name),
                headers=self.headers,
                params={"overwrite": "true"},
                data=f,
                timeout=(30, 900),
            )
        if response.status_code not in (200, 201, 204):
            raise RuntimeError("Upload failed for " + file_name + ": " + str(response.status_code) + " " + response.text[:300])


def download(name, local_path):
    """Download one hour. Returns True if saved, False if GH Archive does not have it (yet)."""
    url = GHARCHIVE_URL.format(name=name)
    for attempt in range(1, 4):
        try:
            response = requests.get(url, stream=True, timeout=(30, 300))
            if response.status_code == 404:
                return False
            response.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            return True
        except requests.RequestException as error:
            print("  attempt", attempt, "failed:", error)
            time.sleep(5 * attempt)
    raise RuntimeError("Could not download " + name + " after 3 attempts")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    parser.add_argument("--start-hour", default="")
    parser.add_argument("--end-hour", default="")
    parser.add_argument("--catchup-hours", type=int, default=24)
    parser.add_argument("--local-dir", default="", help="Save files here instead of uploading (for testing)")
    args = parser.parse_args()

    hours = choose_hours(args)
    print("Mode:", args.mode, "| hours to check:", len(hours))

    databricks = None
    if args.local_dir == "":
        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if host == "" or token == "":
            sys.exit("DATABRICKS_HOST and DATABRICKS_TOKEN must be set (or use --local-dir)")
        databricks = Databricks(host, token, os.environ.get("VOLUME_PATH", DEFAULT_VOLUME_PATH))
        work_dir = "tmp_download"
    else:
        work_dir = args.local_dir
    os.makedirs(work_dir, exist_ok=True)

    uploaded = 0
    skipped = 0
    not_published = 0

    for moment in hours:
        name = archive_name(moment)
        file_name = name + ".json.gz"
        local_path = os.path.join(work_dir, file_name)

        if databricks is not None and databricks.exists(file_name):
            skipped = skipped + 1
            print("skip (already in Volume):", file_name)
            continue

        found = download(name, local_path)
        if not found:
            not_published = not_published + 1
            print("not published yet:", file_name)
            continue

        size_mb = os.path.getsize(local_path) / 1000000
        if databricks is not None:
            databricks.upload(file_name, local_path)
            os.remove(local_path)
            print("uploaded: %s (%.1f MB)" % (file_name, size_mb))
        else:
            print("saved: %s (%.1f MB)" % (local_path, size_mb))
        uploaded = uploaded + 1

    print("Done. new files:", uploaded, "| already there:", skipped, "| not published yet:", not_published)

    # Tell GitHub Actions how many files were new
    output_file = os.environ.get("GITHUB_OUTPUT", "")
    if output_file != "":
        with open(output_file, "a") as f:
            f.write("new_files=" + str(uploaded) + "\n")


if __name__ == "__main__":
    main()
