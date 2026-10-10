# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup
# MAGIC Run this notebook ONCE. It is safe to run again (everything uses IF NOT EXISTS).
# MAGIC
# MAGIC It creates:
# MAGIC * the schema and the raw Volume (the landing folder for the hourly files)
# MAGIC * Bronze and Silver tables, both quarantine tables, the schema drift log and the execution log
# MAGIC
# MAGIC Every column type is written out by hand. Primary keys are documented in the column comments.

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS " + CATALOG + "." + SCHEMA)
spark.sql("CREATE VOLUME IF NOT EXISTS " + CATALOG + "." + SCHEMA + "." + RAW_VOLUME)
print("Schema and volume ready. Raw folder:", DEFAULT_RAW_PATH)

# COMMAND ----------

TABLE_DEFINITIONS = {}

# ---------- BRONZE ----------
TABLE_DEFINITIONS[BRONZE_TABLE] = """
    event_id        STRING    COMMENT 'PRIMARY KEY. GH Archive event id (a string in the raw JSON)',
    event_type      STRING    COMMENT 'PushEvent, IssuesEvent, ...',
    actor_id        STRING,
    actor_login     STRING    COMMENT 'Raw public username. Hashed in Silver',
    repo_id         STRING,
    repo_name       STRING,
    org_id          STRING,
    org_login       STRING,
    is_public       STRING,
    created_at      STRING    COMMENT 'Kept as text in Bronze. Cast to TIMESTAMP in Silver',
    payload_json    STRING    COMMENT 'Raw payload exactly as received (shape depends on event_type)',
    row_hash        STRING    COMMENT 'SHA-256 of the content columns. Used by MERGE to detect changes',
    source_file     STRING,
    ingestion_hour  STRING    COMMENT 'Hour of the source file, format YYYY-MM-DD-HH',
    batch_id        STRING    COMMENT 'One id per pipeline run',
    load_timestamp  TIMESTAMP COMMENT 'When this record was loaded'
"""

TABLE_DEFINITIONS[BRONZE_QUARANTINE] = """
    quarantine_id   STRING    COMMENT 'PRIMARY KEY. Hash of source_file + raw_record',
    raw_record      STRING,
    reason          STRING,
    source_file     STRING,
    ingestion_hour  STRING,
    batch_id        STRING,
    load_timestamp  TIMESTAMP
"""

# ---------- SILVER ----------
TABLE_DEFINITIONS[SILVER_EVENT] = """
    event_id          BIGINT    COMMENT 'PRIMARY KEY',
    event_type        STRING,
    actor_id          BIGINT,
    actor_login_hash  STRING    COMMENT 'Salted SHA-256 of the username (PII removed)',
    repo_id           BIGINT,
    repo_name         STRING,
    org_id            BIGINT,
    is_public         BOOLEAN,
    created_at        TIMESTAMP,
    event_date        DATE,
    source_file       STRING,
    ingestion_hour    STRING,
    bronze_batch_id   STRING,
    row_hash          STRING,
    load_timestamp    TIMESTAMP
"""

TABLE_DEFINITIONS[SILVER_PUSH] = """
    event_id        BIGINT    COMMENT 'PRIMARY KEY. Links to silver_event',
    push_id         BIGINT,
    repository_id   BIGINT,
    ref             STRING,
    head_sha        STRING,
    before_sha      STRING,
    source_file     STRING,
    ingestion_hour  STRING,
    row_hash        STRING,
    load_timestamp  TIMESTAMP
"""

TABLE_DEFINITIONS[SILVER_ISSUE] = """
    event_id          BIGINT    COMMENT 'PRIMARY KEY. Links to silver_event',
    action            STRING,
    issue_id          BIGINT,
    issue_number      BIGINT,
    state             STRING,
    comment_count     BIGINT,
    issue_created_at  TIMESTAMP,
    issue_updated_at  TIMESTAMP,
    issue_closed_at   TIMESTAMP,
    source_file       STRING,
    ingestion_hour    STRING,
    row_hash          STRING,
    load_timestamp    TIMESTAMP
"""

TABLE_DEFINITIONS[SILVER_PR] = """
    event_id         BIGINT    COMMENT 'PRIMARY KEY. Links to silver_event',
    action           STRING,
    pr_number        BIGINT,
    pull_request_id  BIGINT,
    base_ref         STRING,
    head_ref         STRING,
    source_file      STRING,
    ingestion_hour   STRING,
    row_hash         STRING,
    load_timestamp   TIMESTAMP
"""

TABLE_DEFINITIONS[SILVER_QUARANTINE] = """
    quarantine_id    STRING    COMMENT 'PRIMARY KEY. Hash of source_file + event_id + reason',
    event_id         STRING,
    event_type       STRING,
    reason           STRING,
    source_file      STRING,
    ingestion_hour   STRING,
    bronze_batch_id  STRING,
    load_timestamp   TIMESTAMP
"""

# ---------- OPERATIONAL TABLES ----------
TABLE_DEFINITIONS[DRIFT_LOG] = """
    run_id          STRING,
    source_file     STRING,
    ingestion_hour  STRING,
    drift_type      STRING,
    column_name     STRING,
    details         STRING,
    load_timestamp  TIMESTAMP
"""

TABLE_DEFINITIONS[EXEC_LOG] = """
    run_id               STRING,
    layer                STRING    COMMENT 'Raw-to-Bronze or Bronze-to-Silver',
    target_table         STRING,
    parameter_processed  STRING    COMMENT 'File name (Bronze) or hour (Silver)',
    load_type            STRING    COMMENT 'incremental or backfill',
    start_time           TIMESTAMP,
    end_time             TIMESTAMP,
    status               STRING    COMMENT 'Success or Failure',
    rows_inserted        BIGINT,
    rows_updated         BIGINT,
    rows_quarantined     BIGINT,
    error_message        STRING,
    load_timestamp       TIMESTAMP
"""

# COMMAND ----------

for table_name in TABLE_DEFINITIONS:
    ddl = "CREATE TABLE IF NOT EXISTS " + table_name + " (" + TABLE_DEFINITIONS[table_name] + ") USING DELTA"
    spark.sql(ddl)
    print("ready:", table_name)

# COMMAND ----------

# Quick check: list what exists
display(spark.sql("SHOW TABLES IN " + CATALOG + "." + SCHEMA))
