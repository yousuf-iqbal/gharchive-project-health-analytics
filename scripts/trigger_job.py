"""
trigger_job.py

Starts the Databricks job "gharchive_pipeline" with parameters and waits until it finishes.
If the job fails, this script exits with an error so the GitHub Actions run turns red.

Environment variables
  DATABRICKS_HOST, DATABRICKS_TOKEN   required
  DATABRICKS_JOB_ID                   optional. If missing, the job is looked up by name.

Example
  python scripts/trigger_job.py --mode incremental
  python scripts/trigger_job.py --mode backfill --start-hour 2026-10-01-00 --end-hour 2026-10-05-23
"""

import argparse
import os
import sys
import time

import requests

JOB_NAME = "gharchive_pipeline"


def api(method, host, token, path, **kwargs):
    url = host.rstrip("/") + path
    headers = {"Authorization": "Bearer " + token}
    response = requests.request(method, url, headers=headers, timeout=60, **kwargs)
    if response.status_code >= 300:
        sys.exit("Databricks API error " + str(response.status_code) + " on " + path + ": " + response.text[:500])
    return response.json()


def find_job_id(host, token):
    from_env = os.environ.get("DATABRICKS_JOB_ID", "")
    if from_env != "":
        return int(from_env)
    answer = api("GET", host, token, "/api/2.1/jobs/list", params={"name": JOB_NAME})
    jobs = answer.get("jobs", [])
    if len(jobs) == 0:
        sys.exit("No Databricks job named " + JOB_NAME + " found. Create it first (see README).")
    return jobs[0]["job_id"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="incremental")
    parser.add_argument("--start-hour", default="")
    parser.add_argument("--end-hour", default="")
    parser.add_argument("--force-reprocess", default="false")
    parser.add_argument("--timeout-minutes", type=int, default=60)
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if host == "" or token == "":
        sys.exit("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    job_id = find_job_id(host, token)
    parameters = {
        "mode": args.mode,
        "start_hour": args.start_hour,
        "end_hour": args.end_hour,
        "force_reprocess": args.force_reprocess,
    }
    print("Starting job", job_id, "with", parameters)
    started = api("POST", host, token, "/api/2.1/jobs/run-now", json={"job_id": job_id, "job_parameters": parameters})
    run_id = started["run_id"]
    print("Run id:", run_id)

    deadline = time.time() + args.timeout_minutes * 60
    while time.time() < deadline:
        run = api("GET", host, token, "/api/2.1/jobs/runs/get", params={"run_id": run_id})
        state = run.get("state", {})
        life_cycle = state.get("life_cycle_state", "")
        print("  state:", life_cycle, state.get("result_state", ""))
        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = state.get("result_state", "")
            print("Run page:", run.get("run_page_url", ""))
            if result == "SUCCESS":
                print("Databricks job finished successfully.")
                return
            sys.exit("Databricks job did not succeed: " + life_cycle + " " + result + " " + state.get("state_message", ""))
        time.sleep(20)
    sys.exit("Timed out waiting for the Databricks job.")


if __name__ == "__main__":
    main()
