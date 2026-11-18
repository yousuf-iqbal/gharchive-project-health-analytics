# GH Archive Pipeline (Bronze and Silver)

DS-3001 Data Analysis and Visualization, Phase 2.

A PySpark pipeline on Databricks that loads public GitHub activity from GH Archive (one file per hour) into a Bronze layer and a Silver layer. It runs without anyone's laptop: GitHub Actions downloads the files, uploads them to Databricks, and starts the Databricks job.

```
GH Archive (hourly .json.gz)
        |  GitHub Actions: scripts/fetch_gharchive.py
        v
Databricks Volume  /Volumes/workspace/gharchive/raw
        |  Job task 1: notebooks/01_raw_to_bronze
        v
bronze_events  (+ bronze_quarantine, schema_drift_log)
        |  Job task 2: notebooks/02_bronze_to_silver
        v
silver_event, silver_push, silver_issue, silver_pull_request  (+ silver_quarantine)

Every file / hour processed writes a row to pipeline_execution_logs.
```

## Repository layout

```
.github/workflows/gharchive_pipeline.yml   schedule + manual trigger
scripts/fetch_gharchive.py                 download from GH Archive, upload to the Volume
scripts/trigger_job.py                     start the Databricks job and wait for the result
notebooks/common.py                        names, explicit schemas, helpers (loaded with %run)
notebooks/00_setup.py                      create schema, volume, tables (run once)
notebooks/01_raw_to_bronze.py              raw files to Bronze
notebooks/02_bronze_to_silver.py           Bronze to Silver
notebooks/03_prove_it.py                   idempotency, backfill, drift and quarantine checks
tests/test_local_transforms.py             local test of the data logic (plain PySpark)
docs/local_test_results.txt                output of the local test
data/samples/                              sample hourly files from Phase 1
```

## One time setup

1. **Databricks.** In Catalog, check the catalog name (normally `workspace`). If it is different, change `CATALOG` at the top of `notebooks/common.py`.
2. **Git folder.** In Databricks, Workspace, Create, Git folder, paste this repository URL. Notebooks are then committed to GitHub from the notebook UI.
3. **Create the tables.** Run `notebooks/00_setup`. It creates the schema, the raw Volume and all tables.
4. **Create the job.** Workflows, Create job, name it exactly `gharchive_pipeline`.
   * Task 1: name `raw_to_bronze`, type Notebook, path `notebooks/01_raw_to_bronze`, serverless compute.
   * Task 2: name `bronze_to_silver`, type Notebook, path `notebooks/02_bronze_to_silver`, depends on `raw_to_bronze`.
   * Job parameters (they reach both notebooks): `mode` = `incremental`, `start_hour` = (empty), `end_hour` = (empty), `force_reprocess` = `false`. If the form does not accept an empty value, type a single space.
5. **Token.** Databricks, your profile, Settings, Developer, Access tokens, Generate new token.
6. **GitHub secrets.** Repository, Settings, Secrets and variables, Actions, add `DATABRICKS_HOST` (like `https://dbc-xxxx.cloud.databricks.com`, no slash at the end) and `DATABRICKS_TOKEN`.
7. **Test.** Actions tab, GH Archive pipeline, Run workflow.

## Execution guide

### Parameters

| Parameter | Values | Meaning |
|---|---|---|
| `mode` | `incremental` or `backfill` | `incremental`: every file that has not succeeded yet. `backfill`: every file in the hour range, processed again |
| `start_hour` | `2026-10-01-00` | Start of the range. Required for backfill |
| `end_hour` | `2026-10-05-23` | End of the range (inclusive). Required for backfill |
| `force_reprocess` | `true` or `false` | Incremental only. `true` also processes files that already succeeded (used to prove idempotency) |
| `raw_path` | a folder | Where the files are. Default is the Volume. Used for tests |

The same notebooks run both modes. Only the parameters change.

### Standard incremental load

* Automatic: the workflow runs every 6 hours. It checks the last 24 hours on GH Archive, uploads the files that are not yet in the Volume, then runs the job.
* Manual: Actions tab, GH Archive pipeline, Run workflow, leave `mode` as `incremental`.
* Inside Databricks: run notebook 01, then notebook 02, with the default widgets.

### Backfill (historical load)

* From GitHub: Actions, Run workflow, `mode` = `backfill`, `start_hour` = `2026-10-01-00`, `end_hour` = `2026-10-05-23`. This downloads the 120 hourly files, uploads them and processes them.
* Inside Databricks: run notebook 01 and notebook 02 with the same three widgets.
* A backfill can be repeated at any time for any range. MERGE makes it safe.

### Running locally without Databricks

```
pip install requests pyspark
python scripts/fetch_gharchive.py --mode backfill --start-hour 2026-10-01-00 --end-hour 2026-10-01-02 --local-dir tmp_download
python tests/test_local_transforms.py data/samples
```

## How the requirements are met

| Requirement | How |
|---|---|
| No schema inference | `BRONZE_RAW_SCHEMA` and the Silver payload schemas are written with `StructType` and `StructField` in `common.py`. Files are read with `spark.read.schema(...)` |
| Types and casting | Bronze keeps everything as text. Silver uses `try_cast` for BIGINT, TIMESTAMP, BOOLEAN |
| `load_timestamp` | Present on every table, including the log, drift log and both quarantine tables |
| Idempotency | `MERGE INTO` on `event_id`. Insert when the key is new, update only when `row_hash` differs, otherwise do nothing. A rerun inserts 0 and updates 0 |
| Parameterized backfill | Widgets `mode`, `start_hour`, `end_hour`, `force_reprocess`, `raw_path`. No hard coded dates |
| Schema drift | A new top level key is detected, written to `schema_drift_log`, and added to Bronze with `MERGE WITH SCHEMA EVOLUTION`. Broken JSON goes to `bronze_quarantine`. Values that fail a cast go to `silver_quarantine`. The batch keeps running |
| Audit log | `pipeline_execution_logs`, one row per file (Bronze) or per hour and table (Silver) |
| PII | `actor_login` is replaced by a salted SHA-256 hash in Silver. Free text (issue title and body) is not loaded |

## Data models

Primary keys are documented here and in the column comments. Delta does not enforce them, MERGE does.

### Bronze

**bronze_events** (PK `event_id`). Raw envelope, everything as text, payload kept as raw JSON.

| Column | Type | Notes |
|---|---|---|
| event_id | STRING | PK, GH Archive event id |
| event_type | STRING | PushEvent, IssuesEvent, ... |
| actor_id | STRING | |
| actor_login | STRING | raw username (hashed in Silver) |
| repo_id | STRING | |
| repo_name | STRING | |
| org_id | STRING | nullable |
| org_login | STRING | nullable |
| is_public | STRING | |
| created_at | STRING | text in Bronze |
| payload_json | STRING | raw payload, shape depends on event_type |
| row_hash | STRING | SHA-256 of the content columns |
| source_file | STRING | e.g. `2026-09-25-10.json.gz` |
| ingestion_hour | STRING | `YYYY-MM-DD-HH` |
| batch_id | STRING | one per pipeline run |
| load_timestamp | TIMESTAMP | |

New columns found in a file are added automatically (schema evolution).

**bronze_quarantine** (PK `quarantine_id`): quarantine_id STRING, raw_record STRING, reason STRING, source_file STRING, ingestion_hour STRING, batch_id STRING, load_timestamp TIMESTAMP.

### Silver

**silver_event** (PK `event_id`)

| Column | Type |
|---|---|
| event_id | BIGINT |
| event_type | STRING |
| actor_id | BIGINT |
| actor_login_hash | STRING |
| repo_id | BIGINT |
| repo_name | STRING |
| org_id | BIGINT |
| is_public | BOOLEAN |
| created_at | TIMESTAMP |
| event_date | DATE |
| source_file | STRING |
| ingestion_hour | STRING |
| bronze_batch_id | STRING |
| row_hash | STRING |
| load_timestamp | TIMESTAMP |

**silver_push** (PK `event_id`): event_id BIGINT, push_id BIGINT, repository_id BIGINT, ref STRING, head_sha STRING, before_sha STRING, source_file STRING, ingestion_hour STRING, row_hash STRING, load_timestamp TIMESTAMP.

**silver_issue** (PK `event_id`): event_id BIGINT, action STRING, issue_id BIGINT, issue_number BIGINT, state STRING, comment_count BIGINT, issue_created_at TIMESTAMP, issue_updated_at TIMESTAMP, issue_closed_at TIMESTAMP, source_file STRING, ingestion_hour STRING, row_hash STRING, load_timestamp TIMESTAMP.

**silver_pull_request** (PK `event_id`): event_id BIGINT, action STRING, pr_number BIGINT, pull_request_id BIGINT, base_ref STRING, head_ref STRING, source_file STRING, ingestion_hour STRING, row_hash STRING, load_timestamp TIMESTAMP.

**silver_quarantine** (PK `quarantine_id`): quarantine_id STRING, event_id STRING, event_type STRING, reason STRING, source_file STRING, ingestion_hour STRING, bronze_batch_id STRING, load_timestamp TIMESTAMP.

### Operational tables

**pipeline_execution_logs**: run_id STRING, layer STRING, target_table STRING, parameter_processed STRING, load_type STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, rows_inserted BIGINT, rows_updated BIGINT, rows_quarantined BIGINT, error_message STRING, load_timestamp TIMESTAMP.

**schema_drift_log**: run_id STRING, source_file STRING, ingestion_hour STRING, drift_type STRING, column_name STRING, details STRING, load_timestamp TIMESTAMP.

## Testing

* `tests/test_local_transforms.py` runs the data logic on the real sample files with plain PySpark. The result is in `docs/local_test_results.txt`. It does not run the Delta MERGE.
* `notebooks/03_prove_it.py` runs the idempotency, backfill, drift and quarantine checks on Databricks and shows the log table.

## Known limits

* Drift detection looks at top level keys. Changes inside `payload` are not lost (the raw payload is stored in Bronze) but they are not reported as drift.
* Databricks Free Edition has daily compute limits, so large backfills should be split into a few days at a time.
* The salt for the actor hash is a notebook widget (`hash_salt`). Change it for real use and do not commit the real value.
