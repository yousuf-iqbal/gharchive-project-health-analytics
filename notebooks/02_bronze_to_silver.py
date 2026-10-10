# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze_to_silver
# MAGIC Reads `bronze_events` one hour at a time, casts and validates it, hashes the username, and MERGEs into:
# MAGIC `silver_event`, `silver_push`, `silver_issue`, `silver_pull_request`. Bad rows go to `silver_quarantine`.
# MAGIC
# MAGIC Parameters are the same as notebook 01. Here the "file" is an hour of Bronze data (`ingestion_hour`).
# MAGIC `raw_path` is not used in this notebook.

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])
dbutils.widgets.text("start_hour", "")
dbutils.widgets.text("end_hour", "")
dbutils.widgets.dropdown("force_reprocess", "false", ["false", "true"])
dbutils.widgets.text("raw_path", DEFAULT_RAW_PATH)   # not used here, kept so one job can pass the same parameters
dbutils.widgets.text("hash_salt", "gharchive-demo-salt")

mode, start_hour, end_hour, force, raw_path = read_parameters()
salt = dbutils.widgets.get("hash_salt")
print("mode =", mode, "| start =", start_hour, "| end =", end_hour, "| force =", force)

# COMMAND ----------

# Which hours exist in Bronze?
hour_rows = spark.sql("SELECT DISTINCT ingestion_hour FROM " + BRONZE_TABLE + " ORDER BY ingestion_hour").collect()
bronze_hours = []
for row in hour_rows:
    bronze_hours.append({"name": row["ingestion_hour"], "hour_key": row["ingestion_hour"]})

already_done = get_done_hours(LAYER_SILVER, SILVER_EVENT)
hours_to_process = select_files(bronze_hours, mode, start_hour, end_hour, already_done, force)

print("Hours in Bronze:", len(bronze_hours))
print("Hours to process:", len(hours_to_process))
for h in hours_to_process:
    print("  ", h["hour_key"])

# COMMAND ----------

# Process each hour. One hour = one log row per Silver table.
run_id = str(uuid.uuid4())
failed_hours = []

# (table name, key in the dictionary returned by build_silver, temp view name)
SILVER_TARGETS = [
    (SILVER_EVENT, "silver_event", "silver_event_updates"),
    (SILVER_PUSH, "silver_push", "silver_push_updates"),
    (SILVER_ISSUE, "silver_issue", "silver_issue_updates"),
    (SILVER_PR, "silver_pull_request", "silver_pr_updates"),
]

for h in hours_to_process:
    hour = h["hour_key"]
    start_time = datetime.now()
    try:
        bronze_df = spark.table(BRONZE_TABLE).filter(F.col("ingestion_hour") == hour)
        frames = build_silver(bronze_df, salt)

        # Bad rows first (insert only, so reruns add nothing)
        quarantine_df = frames["quarantine"]
        quarantined = quarantine_df.count()
        if quarantined > 0:
            run_merge(SILVER_QUARANTINE, quarantine_df, "quarantine_id", "silver_quarantine_updates",
                      schema_evolution=False, use_hash=False)

        # Good rows: one MERGE per Silver table. Collect the counts, log after everything worked.
        results = []
        for table_name, key, view_name in SILVER_TARGETS:
            inserted, updated = run_merge(table_name, frames[key], "event_id", view_name,
                                          schema_evolution=False, use_hash=True)
            results.append((table_name, inserted, updated))

        for table_name, inserted, updated in results:
            q = quarantined if table_name == SILVER_EVENT else 0
            write_log(run_id, LAYER_SILVER, table_name, hour, mode, start_time,
                      "Success", inserted, updated, q)
            print("OK  ", hour, table_name, "inserted =", inserted, "updated =", updated)

    except Exception as e:
        write_log(run_id, LAYER_SILVER, SILVER_EVENT, hour, mode, start_time,
                  "Failure", 0, 0, 0, str(e)[:2000])
        failed_hours.append(hour)
        print("FAIL", hour, str(e)[:300])

if len(failed_hours) > 0:
    raise Exception("Bronze-to-Silver failed for: " + ", ".join(failed_hours))

# COMMAND ----------

display(spark.sql("SELECT * FROM " + EXEC_LOG + " WHERE run_id = '" + run_id + "' ORDER BY start_time"))
