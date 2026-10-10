# Databricks notebook source
# MAGIC %md
# MAGIC # 03_prove_it
# MAGIC This notebook produces the evidence for the Phase 2 rubric. Run the cells in order and take screenshots.
# MAGIC
# MAGIC | Proof | Where |
# MAGIC |---|---|
# MAGIC | Idempotency (same data twice, no duplicates) | Part A |
# MAGIC | Parameterized backfill vs incremental | Part B |
# MAGIC | Schema drift and quarantine | Part C |
# MAGIC | Audit log | Part D |

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: idempotency
# MAGIC 1. Run the cell below and note the counts.
# MAGIC 2. Run notebook `01_raw_to_bronze` with `mode=incremental`, `force_reprocess=true`.
# MAGIC 3. Run notebook `02_bronze_to_silver` with `mode=incremental`, `force_reprocess=true`.
# MAGIC 4. Run the cell below again. The counts must be identical, and the log must show 0 inserted, 0 updated.

# COMMAND ----------

TABLES_TO_COUNT = [BRONZE_TABLE, BRONZE_QUARANTINE, SILVER_EVENT, SILVER_PUSH, SILVER_ISSUE, SILVER_PR, SILVER_QUARANTINE]

print("Row counts at", datetime.now())
for t in TABLES_TO_COUNT:
    n = spark.table(t).count()
    print("%-45s %d" % (t, n))

# COMMAND ----------

# Duplicate check: every query below must return 0 rows
for t in [BRONZE_TABLE, SILVER_EVENT, SILVER_PUSH, SILVER_ISSUE, SILVER_PR]:
    dups = spark.sql("SELECT event_id, COUNT(*) AS n FROM " + t + " GROUP BY event_id HAVING COUNT(*) > 1").count()
    print("%-45s duplicate event_ids: %d" % (t, dups))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: backfill vs incremental
# MAGIC * Standard incremental load: run notebooks 01 and 02 with `mode=incremental` and leave the hours empty.
# MAGIC * Backfill: run them with `mode=backfill`, `start_hour=2026-09-25-10`, `end_hour=2026-09-25-12`.
# MAGIC The same code handles both. Only the parameters change.
# MAGIC From GitHub: Actions tab, "GH Archive pipeline", "Run workflow", then type the same values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: schema drift and bad records
# MAGIC The next cell builds a small test file from 6 real events and breaks them on purpose:
# MAGIC
# MAGIC | Event | What is wrong | Expected result |
# MAGIC |---|---|---|
# MAGIC | 0, 1 | New top level key `new_field` | Logged in `schema_drift_log`, Bronze gets a new column |
# MAGIC | 2 | `created_at` = "not-a-date" | Bronze accepts it, Silver quarantines it |
# MAGIC | 3 | `actor.id` = "abc" | Bronze accepts it, Silver quarantines it |
# MAGIC | 4 | Broken JSON line | Bronze quarantines it |
# MAGIC | 5 | Nothing | Loads normally |
# MAGIC
# MAGIC The file gets the hour 2026-01-01-00 (an old date) so it cannot touch real data.

# COMMAND ----------

import json

DRIFT_FOLDER = DEFAULT_RAW_PATH + "/drift_test"

source_files = list_raw_files(DEFAULT_RAW_PATH)
first_file = source_files[0]["path"]
real_lines = []
for row in spark.read.text(first_file).limit(6).collect():
    real_lines.append(row["value"])

test_lines = []
for i in range(len(real_lines)):
    event = json.loads(real_lines[i])
    event["id"] = "99900000" + str(i)          # new ids so they are really new rows
    if i in [0, 1]:
        event["new_field"] = "hello"            # a new top level key
    if i == 2:
        event["created_at"] = "not-a-date"
    if i == 3:
        event["actor"]["id"] = "abc"
    if i == 4:
        test_lines.append('{"id": "999000004", "type": "PushEvent", "actor": "broken')   # invalid JSON
        continue
    test_lines.append(json.dumps(event))

dbutils.fs.mkdirs(DRIFT_FOLDER)
dbutils.fs.put(DRIFT_FOLDER + "/2026-01-01-0.json", "\n".join(test_lines) + "\n", True)
print("Test file written to", DRIFT_FOLDER)

# COMMAND ----------

# MAGIC %md
# MAGIC Now run the two notebooks (widgets exactly like this):
# MAGIC 1. `01_raw_to_bronze`: mode=backfill, start_hour=2026-01-01-00, end_hour=2026-01-01-00, raw_path=(the drift_test folder printed above)
# MAGIC 2. `02_bronze_to_silver`: mode=backfill, start_hour=2026-01-01-00, end_hour=2026-01-01-00
# MAGIC
# MAGIC Then run the checks below and take screenshots.

# COMMAND ----------

print("Schema drift log")
display(spark.sql("SELECT * FROM " + DRIFT_LOG + " ORDER BY load_timestamp DESC LIMIT 10"))

# COMMAND ----------

print("Bronze now has the new column:")
display(spark.sql("DESCRIBE TABLE " + BRONZE_TABLE))

# COMMAND ----------

print("Bronze quarantine (expect the broken JSON line)")
display(spark.sql("SELECT reason, source_file, raw_record FROM " + BRONZE_QUARANTINE + " WHERE ingestion_hour = '2026-01-01-00'"))

# COMMAND ----------

print("Silver quarantine (expect the bad date and the bad actor id)")
display(spark.sql("SELECT event_id, reason, source_file FROM " + SILVER_QUARANTINE + " WHERE ingestion_hour = '2026-01-01-00'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup of the test rows
# MAGIC Take your screenshots first. This removes the fake rows and the test file.
# MAGIC The Bronze table keeps the `new_field` column (that is the proof that the schema evolved).
# MAGIC The execution log and drift log are kept as evidence.

# COMMAND ----------

for t in [BRONZE_TABLE, BRONZE_QUARANTINE, SILVER_EVENT, SILVER_PUSH, SILVER_ISSUE, SILVER_PR, SILVER_QUARANTINE]:
    spark.sql("DELETE FROM " + t + " WHERE ingestion_hour = '2026-01-01-00'")
dbutils.fs.rm(DRIFT_FOLDER, True)
print("Test rows and test file removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: audit log

# COMMAND ----------

display(spark.sql("""
    SELECT layer, target_table, parameter_processed, load_type, start_time, end_time,
           status, rows_inserted, rows_updated, rows_quarantined, error_message
    FROM """ + EXEC_LOG + """
    ORDER BY start_time DESC
    LIMIT 50
"""))
