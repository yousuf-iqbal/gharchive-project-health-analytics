# Databricks notebook source
# MAGIC %md
# MAGIC # 01_raw_to_bronze
# MAGIC Reads the raw hourly files from the Volume and loads them into `bronze_events`.
# MAGIC
# MAGIC | Parameter | Meaning |
# MAGIC |---|---|
# MAGIC | `mode` | `incremental` = every file that has not succeeded yet. `backfill` = every file between start_hour and end_hour, again |
# MAGIC | `start_hour`, `end_hour` | Format `2026-09-25-10`. Required for backfill, optional window for incremental |
# MAGIC | `force_reprocess` | `true` = incremental also re-runs files that already succeeded (used to prove idempotency) |
# MAGIC | `raw_path` | Folder with the files. Default is the Volume. Change it to test with another folder |

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])
dbutils.widgets.text("start_hour", "")
dbutils.widgets.text("end_hour", "")
dbutils.widgets.dropdown("force_reprocess", "false", ["false", "true"])
dbutils.widgets.text("raw_path", DEFAULT_RAW_PATH)

mode, start_hour, end_hour, force, raw_path = read_parameters()
print("mode =", mode, "| start =", start_hour, "| end =", end_hour, "| force =", force)
print("raw_path =", raw_path)

# COMMAND ----------

# Decide which files to process
all_files = list_raw_files(raw_path)
already_done = get_done_hours(LAYER_BRONZE, BRONZE_TABLE)
files_to_process = select_files(all_files, mode, start_hour, end_hour, already_done, force)

print("Files in folder:", len(all_files))
print("Files to process:", len(files_to_process))
for f in files_to_process:
    print("  ", f["name"])

# COMMAND ----------

# Process each file. One file = one log row.
run_id = str(uuid.uuid4())
failed_files = []

for f in files_to_process:
    start_time = datetime.now()
    try:
        # 1. Schema drift check (new top level keys)
        new_cols = find_new_columns(f["path"])
        for col_name in new_cols:
            write_drift_log(run_id, f["name"], f["hour_key"], col_name,
                            "New top level key found in the raw file. Added to Bronze as a STRING column.")

        # 2. Read with the explicit schema and prepare Bronze rows
        raw_df = read_raw_file(f["path"], new_cols)
        good_df, bad_df = prepare_bronze(raw_df, new_cols, f["name"], f["hour_key"], run_id)

        # 3. MERGE good rows into Bronze (schema evolution lets new columns in)
        inserted, updated = run_merge(BRONZE_TABLE, good_df, "event_id", "bronze_updates",
                                      schema_evolution=True, use_hash=True)

        # 4. MERGE bad rows into the quarantine table (insert only, so reruns add nothing)
        quarantined = bad_df.count()
        if quarantined > 0:
            run_merge(BRONZE_QUARANTINE, bad_df, "quarantine_id", "bronze_quarantine_updates",
                      schema_evolution=False, use_hash=False)

        write_log(run_id, LAYER_BRONZE, BRONZE_TABLE, f["name"], mode, start_time,
                  "Success", inserted, updated, quarantined)
        print("OK  ", f["name"], "inserted =", inserted, "updated =", updated, "quarantined =", quarantined)

    except Exception as e:
        write_log(run_id, LAYER_BRONZE, BRONZE_TABLE, f["name"], mode, start_time,
                  "Failure", 0, 0, 0, str(e)[:2000])
        failed_files.append(f["name"])
        print("FAIL", f["name"], str(e)[:300])

# If any file failed, fail the notebook so the Databricks job and the GitHub run show red.
if len(failed_files) > 0:
    raise Exception("Raw-to-Bronze failed for: " + ", ".join(failed_files))

# COMMAND ----------

# Latest log rows for this run
display(spark.sql("SELECT * FROM " + EXEC_LOG + " WHERE run_id = '" + run_id + "' ORDER BY start_time"))
