"""
test_local_transforms.py

Runs the data logic from notebooks/common.py on your laptop (plain PySpark, no Databricks).
What it checks:
  * file name / hour handling and file selection (backfill vs incremental)
  * explicit schema read, row counts, payload kept as raw JSON text
  * Silver casting, validation, PII hashing, event specific tables
  * hashes are identical when the same file is processed twice (so MERGE would change nothing)
  * a deliberately broken file: new column detected, bad rows quarantined, batch does not crash

What it can NOT check: the Delta MERGE itself, Volumes and the Databricks job. Those only run on Databricks.

Usage (from the repo root):
  pip install pyspark
  python tests/test_local_transforms.py data/samples
"""

import json
import os
import shutil
import sys
import tempfile
from datetime import datetime

from pyspark.sql import SparkSession

SAMPLE_DIR = sys.argv[1] if len(sys.argv) > 1 else "data/samples"
HERE = os.path.dirname(os.path.abspath(__file__))
COMMON_PATH = os.path.join(HERE, "..", "notebooks", "common.py")

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    line = status + "  " + name + ("  (" + str(detail) + ")" if detail != "" else "")
    print(line)
    results.append((status, line))


# ---------------------------------------------------------------- Spark + fake Databricks objects
spark = (
    SparkSession.builder.master("local[2]")
    .appName("gharchive-local-test")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "3g")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


class FakeFileInfo:
    def __init__(self, name):
        self.name = name


class FakeFs:
    def ls(self, path):
        names = []
        for n in sorted(os.listdir(path)):
            names.append(FakeFileInfo(n))
        return names


class FakeDbutils:
    fs = FakeFs()


# Load common.py exactly as the notebooks do (the %run line is only a comment here)
namespace = {"spark": spark, "dbutils": FakeDbutils()}
with open(COMMON_PATH) as f:
    exec(compile(f.read(), "common.py", "exec"), namespace)
hour_key = namespace["hour_key"]
select_files = namespace["select_files"]
list_raw_files = namespace["list_raw_files"]
find_new_columns = namespace["find_new_columns"]
read_raw_file = namespace["read_raw_file"]
prepare_bronze = namespace["prepare_bronze"]
build_silver = namespace["build_silver"]

# Work on a copy with the correct GH Archive file names
work = tempfile.mkdtemp()
sample_names = []
for n in sorted(os.listdir(SAMPLE_DIR)):
    key = hour_key(n.replace("_json.gz", ".json.gz"))
    if key is not None:
        target = os.path.join(work, n.replace("_json.gz", ".json.gz"))
        shutil.copy(os.path.join(SAMPLE_DIR, n), target)
        sample_names.append(os.path.basename(target))
print("Sample files:", sample_names)

# ---------------------------------------------------------------- 1. names and file selection
print("\n--- 1. hour keys and file selection ---")
check("hour_key pads the hour", hour_key("2026-09-25-9.json.gz") == "2026-09-25-09")
check("hour_key accepts padded input", hour_key("2026-09-25-09") == "2026-09-25-09")
check("hour_key rejects other names", hour_key("readme.txt") is None)

files = list_raw_files(work)
check("all sample files are listed in order", [x["hour_key"] for x in files] == sorted([x["hour_key"] for x in files]), len(files))

done = set()
done.add(files[0]["hour_key"])
inc = select_files(files, "incremental", "", "", done, False)
check("incremental skips files already done", len(inc) == len(files) - 1)
inc_force = select_files(files, "incremental", "", "", done, True)
check("force_reprocess includes files already done", len(inc_force) == len(files))
back = select_files(files, "backfill", files[0]["hour_key"], files[0]["hour_key"], done, False)
check("backfill re-processes a done file inside the window", len(back) == 1)

# ---------------------------------------------------------------- 2. Bronze on the real files
print("\n--- 2. Bronze on the real files ---")
bronze_frames = []
for f in files:
    line_count = spark.read.text(f["path"]).count()
    new_cols = find_new_columns(f["path"])
    check(f["name"] + ": no unexpected top level keys", new_cols == [], new_cols)
    raw_df = read_raw_file(f["path"], new_cols)
    good, bad = prepare_bronze(raw_df, new_cols, f["name"], f["hour_key"], "test-run")
    good_count = good.count()
    bad_count = bad.count()
    check(f["name"] + ": every line becomes a Bronze row", good_count == line_count, str(good_count) + " of " + str(line_count))
    check(f["name"] + ": nothing quarantined", bad_count == 0, bad_count)
    bronze_frames.append(good)

first_bronze = bronze_frames[0]
push_row = first_bronze.filter("event_type = 'PushEvent'").select("payload_json").limit(1).collect()[0][0]
check("payload is kept as raw JSON text", push_row.startswith("{") and "push_id" in push_row, push_row[:60])
check("every Bronze column is a string except load_timestamp",
      all(t == "string" for n, t in first_bronze.dtypes if n != "load_timestamp"))

# ---------------------------------------------------------------- 3. Silver on the real files
print("\n--- 3. Silver on the real files ---")
bronze_one = bronze_frames[0]
expected_total = bronze_one.count()
expected_push = bronze_one.filter("event_type = 'PushEvent'").count()
expected_issue = bronze_one.filter("event_type = 'IssuesEvent'").count()
expected_pr = bronze_one.filter("event_type = 'PullRequestEvent'").count()

silver = build_silver(bronze_one, "test-salt")
check("silver_event has one row per event", silver["silver_event"].count() == expected_total, silver["silver_event"].count())
check("silver_push matches PushEvent count", silver["silver_push"].count() == expected_push, silver["silver_push"].count())
check("silver_issue matches IssuesEvent count", silver["silver_issue"].count() == expected_issue, silver["silver_issue"].count())
check("silver_pull_request matches PullRequestEvent count", silver["silver_pull_request"].count() == expected_pr, silver["silver_pull_request"].count())
check("no quarantine rows for clean data", silver["quarantine"].count() == 0)

types = dict(silver["silver_event"].dtypes)
check("event_id is BIGINT", types["event_id"] == "bigint")
check("created_at is TIMESTAMP", types["created_at"] == "timestamp")
check("is_public is BOOLEAN", types["is_public"] == "boolean")
check("raw actor_login is not carried into Silver", "actor_login" not in types and "actor_login_hash" in types)
hash_len = silver["silver_event"].selectExpr("min(length(actor_login_hash)) as a", "max(length(actor_login_hash)) as b").collect()[0]
check("actor_login_hash is a 64 character SHA-256", hash_len["a"] == 64 and hash_len["b"] == 64)
check("load_timestamp exists on every Silver table",
      all("load_timestamp" in silver[k].columns for k in ["silver_event", "silver_push", "silver_issue", "silver_pull_request", "quarantine"]))
dup = silver["silver_event"].groupBy("event_id").count().filter("count > 1").count()
check("no duplicate event_id in silver_event", dup == 0)

# ---------------------------------------------------------------- 4. same input twice gives the same hashes
print("\n--- 4. idempotency (row hashes) ---")
second_run = build_silver(bronze_one, "test-salt")
first_hashes = silver["silver_event"].select("event_id", "row_hash")
second_hashes = second_run["silver_event"].select("event_id", "row_hash")
changed = first_hashes.join(second_hashes, "event_id").filter(first_hashes["row_hash"] != second_hashes["row_hash"]).count()
check("processing the same data twice gives identical row hashes (MERGE would update 0 rows)", changed == 0, changed)
other_salt = build_silver(bronze_one, "other-salt")
diff = silver["silver_event"].select("event_id", "actor_login_hash").join(
    other_salt["silver_event"].select("event_id", "actor_login_hash").withColumnRenamed("actor_login_hash", "h2"), "event_id"
).filter("actor_login_hash != h2").count()
check("a different salt gives different hashes", diff > 0, diff)

# ---------------------------------------------------------------- 5. deliberately broken file
print("\n--- 5. broken file: drift and bad records ---")
real_lines = []
for row in spark.read.text(files[0]["path"]).limit(6).collect():
    real_lines.append(row["value"])
test_lines = []
for i in range(len(real_lines)):
    event = json.loads(real_lines[i])
    event["id"] = "99900000" + str(i)
    if i in [0, 1]:
        event["new_field"] = "hello"
    if i == 2:
        event["created_at"] = "not-a-date"
    if i == 3:
        event["actor"]["id"] = "abc"
    if i == 4:
        test_lines.append('{"id": "999000004", "type": "PushEvent", "actor": "broken')
        continue
    test_lines.append(json.dumps(event))
drift_dir = os.path.join(work, "drift_test")
os.makedirs(drift_dir)
drift_file = os.path.join(drift_dir, "2026-01-01-0.json")
with open(drift_file, "w") as f:
    f.write("\n".join(test_lines) + "\n")

new_cols = find_new_columns(drift_file)
check("new top level column is detected", new_cols == ["new_field"], new_cols)
raw_df = read_raw_file(drift_file, new_cols)
good, bad = prepare_bronze(raw_df, new_cols, "2026-01-01-0.json", "2026-01-01-00", "test-run")
check("Bronze keeps 5 good rows (bad values are still text)", good.count() == 5, good.count())
check("Bronze quarantines the broken JSON line", bad.count() == 1, bad.count())
check("new column reaches the Bronze DataFrame", "new_field" in good.columns)
reason = bad.select("reason").collect()[0][0]
check("quarantine reason is readable", "not valid JSON" in reason, reason)

silver_bad = build_silver(good, "test-salt")
q = silver_bad["quarantine"]
check("Silver quarantines exactly the 2 bad rows", q.count() == 2, q.count())
reasons = []
for r in q.select("reason").collect():
    reasons.append(r[0])
check("bad date reason is present", any("created_at" in r for r in reasons), reasons)
check("bad actor id reason is present", any("actor_id" in r for r in reasons), reasons)
check("the rest of the batch still loads", silver_bad["silver_event"].count() == 3, silver_bad["silver_event"].count())

# ---------------------------------------------------------------- summary
print("\n==== SUMMARY ====")
passed = 0
for status, line in results:
    if status == "PASS":
        passed = passed + 1
print(passed, "of", len(results), "checks passed")
out_dir = os.path.join(HERE, "..", "docs")
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, "local_test_results.txt"), "w") as f:
    f.write("Local test run (plain PySpark, no Databricks, no Delta). " + str(datetime.now()) + "\n")
    f.write("Spark version: " + spark.version + "\n\n")
    for status, line in results:
        f.write(line + "\n")
    f.write("\n" + str(passed) + " of " + str(len(results)) + " checks passed\n")
shutil.rmtree(work, ignore_errors=True)
spark.stop()
sys.exit(0 if passed == len(results) else 1)
