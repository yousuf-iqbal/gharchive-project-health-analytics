# Databricks notebook source
# MAGIC %md
# MAGIC # common
# MAGIC Shared names, explicit schemas and helper functions.
# MAGIC Every other notebook loads this one with `%run ./common`.
# MAGIC
# MAGIC Sections:
# MAGIC 1. Names (catalog, schema, tables)
# MAGIC 2. Explicit schemas (no inferSchema anywhere)
# MAGIC 3. Logging helper
# MAGIC 4. File selection helpers (parameters, backfill vs incremental)
# MAGIC 5. Bronze helpers
# MAGIC 6. Silver helpers
# MAGIC 7. MERGE helper

# COMMAND ----------

import re
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Keep every timestamp in UTC so the same file always gives the same result.
try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Names
# MAGIC Check your catalog name in the Catalog explorer. On Databricks Free Edition it is normally `workspace`.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "gharchive"
RAW_VOLUME = "raw"

# Folder where the GitHub Action uploads the hourly .json.gz files
DEFAULT_RAW_PATH = "/Volumes/" + CATALOG + "/" + SCHEMA + "/" + RAW_VOLUME


def full_name(table_name):
    return CATALOG + "." + SCHEMA + "." + table_name


BRONZE_TABLE = full_name("bronze_events")
BRONZE_QUARANTINE = full_name("bronze_quarantine")
SILVER_EVENT = full_name("silver_event")
SILVER_PUSH = full_name("silver_push")
SILVER_ISSUE = full_name("silver_issue")
SILVER_PR = full_name("silver_pull_request")
SILVER_QUARANTINE = full_name("silver_quarantine")
DRIFT_LOG = full_name("schema_drift_log")
EXEC_LOG = full_name("pipeline_execution_logs")

LAYER_BRONZE = "Raw-to-Bronze"
LAYER_SILVER = "Bronze-to-Silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explicit schemas
# MAGIC Bronze reads every field as a STRING on purpose. Nothing can fail on a type change.
# MAGIC The real types are applied (cast) when data moves into Silver.
# MAGIC `payload` is kept as one raw JSON string because every event type has a different payload shape.

# COMMAND ----------

ACTOR_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("login", StringType(), True),
    StructField("display_login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("avatar_url", StringType(), True),
])

REPO_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
])

ORG_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("login", StringType(), True),
    StructField("gravatar_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("avatar_url", StringType(), True),
])

# Schema used to read the raw GH Archive file (one JSON object per line)
BRONZE_RAW_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", ACTOR_SCHEMA, True),
    StructField("repo", REPO_SCHEMA, True),
    StructField("org", ORG_SCHEMA, True),
    StructField("payload", StringType(), True),
    StructField("public", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# Top level keys we expect. Any other key in a file is reported as schema drift.
EXPECTED_TOP_LEVEL_KEYS = ["id", "type", "actor", "repo", "org", "payload", "public", "created_at"]

# Columns used to build the row hash in Bronze
BRONZE_HASH_COLUMNS = [
    "event_id", "event_type", "actor_id", "actor_login", "repo_id", "repo_name",
    "org_id", "org_login", "is_public", "created_at", "payload_json",
]

# Payload schemas used in Silver (one per event type we model).
# Every field is read as STRING and cast afterwards so that bad values can be quarantined.
PUSH_PAYLOAD_SCHEMA = StructType([
    StructField("push_id", StringType(), True),
    StructField("repository_id", StringType(), True),
    StructField("ref", StringType(), True),
    StructField("head", StringType(), True),
    StructField("before", StringType(), True),
])

ISSUE_INNER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("number", StringType(), True),
    StructField("state", StringType(), True),
    StructField("comments", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
])
ISSUE_PAYLOAD_SCHEMA = StructType([
    StructField("action", StringType(), True),
    StructField("issue", ISSUE_INNER_SCHEMA, True),
])

PR_REF_SCHEMA = StructType([
    StructField("ref", StringType(), True),
])
PR_INNER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("number", StringType(), True),
    StructField("base", PR_REF_SCHEMA, True),
    StructField("head", PR_REF_SCHEMA, True),
])
PR_PAYLOAD_SCHEMA = StructType([
    StructField("action", StringType(), True),
    StructField("number", StringType(), True),
    StructField("pull_request", PR_INNER_SCHEMA, True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Logging helper
# MAGIC Every file or hour that is processed writes one row to `pipeline_execution_logs`.

# COMMAND ----------

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("parameter_processed", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_quarantined", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("load_timestamp", TimestampType(), True),
])


def write_log(run_id, layer, target_table, parameter, load_type, start_time,
              status, inserted=0, updated=0, quarantined=0, error=None):
    """Append one audit row to pipeline_execution_logs."""
    end_time = datetime.now()
    row = [(run_id, layer, target_table, parameter, load_type, start_time, end_time,
            status, int(inserted), int(updated), int(quarantined), error, end_time)]
    spark.createDataFrame(row, LOG_SCHEMA).write.mode("append").saveAsTable(EXEC_LOG)


def write_drift_log(run_id, source_file, hour, column_name, details):
    """Append one row to schema_drift_log."""
    now = datetime.now()
    row = [(run_id, source_file, hour, "new_column", column_name, details, now)]
    schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_hour", StringType(), True),
        StructField("drift_type", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("details", StringType(), True),
        StructField("load_timestamp", TimestampType(), True),
    ])
    spark.createDataFrame(row, schema).write.mode("append").saveAsTable(DRIFT_LOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. File selection (parameters)
# MAGIC GH Archive names files like `2026-09-25-9.json.gz` (the hour is NOT zero padded).
# MAGIC To compare hours correctly we convert every name to a padded key like `2026-09-25-09`.

# COMMAND ----------

def hour_key(text):
    """'2026-09-25-9.json.gz' or '2026-09-25-09' -> '2026-09-25-09'. Returns None if not a valid name."""
    name = text
    for suffix in [".json.gz", ".json"]:
        if name.endswith(suffix):
            name = name[:len(name) - len(suffix)]
    parts = name.split("-")
    if len(parts) != 4:
        return None
    for part in parts:
        if not part.isdigit():
            return None
    return "%04d-%02d-%02d-%02d" % (int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]))


def get_hour_value(file_info):
    return file_info["hour_key"]


def list_raw_files(raw_path):
    """List the .json.gz / .json files in the raw folder, sorted by hour."""
    files = []
    for item in dbutils.fs.ls(raw_path):
        key = hour_key(item.name)
        if key is not None:
            files.append({"name": item.name, "path": raw_path + "/" + item.name, "hour_key": key})
    return sorted(files, key=get_hour_value)


def get_done_hours(layer, target_table):
    """Hours that already have a Success row in the log for this layer."""
    rows = spark.sql(
        "SELECT DISTINCT parameter_processed FROM " + EXEC_LOG +
        " WHERE layer = '" + layer + "' AND target_table = '" + target_table +
        "' AND status = 'Success'"
    ).collect()
    done = set()
    for row in rows:
        key = hour_key(row["parameter_processed"])
        if key is not None:
            done.add(key)
    return done


def select_files(all_files, mode, start_hour, end_hour, already_done, force):
    """
    backfill    : every file inside [start_hour, end_hour], processed again even if done before.
    incremental : every file not yet marked Success (optionally limited to the window).
    force=True  : incremental also re-processes files that were already done.
    """
    chosen = []
    for f in all_files:
        key = f["hour_key"]
        if start_hour != "" and key < start_hour:
            continue
        if end_hour != "" and key > end_hour:
            continue
        if mode == "incremental" and (not force) and (key in already_done):
            continue
        chosen.append(f)
    return chosen


def read_parameters():
    """Read the notebook widgets and validate them."""
    mode = dbutils.widgets.get("mode").strip().lower()
    start_hour = hour_key(dbutils.widgets.get("start_hour").strip()) if dbutils.widgets.get("start_hour").strip() != "" else ""
    end_hour = hour_key(dbutils.widgets.get("end_hour").strip()) if dbutils.widgets.get("end_hour").strip() != "" else ""
    force = dbutils.widgets.get("force_reprocess").strip().lower() == "true"
    raw_path = dbutils.widgets.get("raw_path").strip()
    if raw_path == "":
        raw_path = DEFAULT_RAW_PATH
    if mode not in ["incremental", "backfill"]:
        raise Exception("mode must be 'incremental' or 'backfill', got: " + mode)
    if mode == "backfill" and (start_hour == "" or end_hour == ""):
        raise Exception("backfill mode needs both start_hour and end_hour, e.g. 2026-09-25-00")
    if start_hour is None or end_hour is None:
        raise Exception("start_hour / end_hour must look like 2026-09-25-10")
    return mode, start_hour, end_hour, force, raw_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze helpers

# COMMAND ----------

def clean_column_name(name):
    """Delta does not allow some characters in column names."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def add_row_hash(df, columns):
    """row_hash = SHA-256 of the chosen columns. Used to decide if a row really changed."""
    pieces = []
    for c in columns:
        pieces.append(F.col(c).cast("string"))
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *pieces), 256))


def find_new_columns(file_path):
    """
    Schema drift check. Read the file as plain text, turn every line into a map of
    top level keys and report keys we did not expect.
    """
    text_df = spark.read.text(file_path)
    keys_df = text_df.select(
        F.explode(F.map_keys(F.from_json(F.col("value"), "map<string,string>"))).alias("k")
    ).distinct()
    new_cols = []
    for row in keys_df.collect():
        if row["k"] not in EXPECTED_TOP_LEVEL_KEYS:
            new_cols.append(row["k"])
    return new_cols


def read_raw_file(file_path, new_cols):
    """Read one raw file with the EXPLICIT schema (plus any new top level keys as strings)."""
    fields = list(BRONZE_RAW_SCHEMA.fields)
    for c in new_cols:
        fields.append(StructField(c, StringType(), True))
    schema = StructType(fields)
    return (
        spark.read
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(file_path)
    )


def prepare_bronze(raw_df, new_cols, source_file, hour, batch_id):
    """
    Flatten the envelope, add metadata and split into good rows and bad rows.
    Returns (good_df, bad_df). Bad rows go to bronze_quarantine.
    """
    select_list = [
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_login"),
        F.col("public").alias("is_public"),
        F.col("created_at").alias("created_at"),
        F.col("payload").alias("payload_json"),
        F.col("_corrupt_record").alias("corrupt_record"),
    ]
    for c in new_cols:
        select_list.append(F.col("`" + c + "`").alias(clean_column_name(c)))
    flat = raw_df.select(*select_list)

    # Good rows: valid JSON that fits the schema and has an event id
    good = flat.filter(F.col("corrupt_record").isNull() & F.col("event_id").isNotNull())
    good = good.drop("corrupt_record")
    good = add_row_hash(good, BRONZE_HASH_COLUMNS)
    good = good.withColumn("source_file", F.lit(source_file))
    good = good.withColumn("ingestion_hour", F.lit(hour))
    good = good.withColumn("batch_id", F.lit(batch_id))
    good = good.withColumn("load_timestamp", F.current_timestamp())
    good = good.dropDuplicates(["event_id"])   # a MERGE needs one source row per key

    # Bad rows: broken JSON, wrong structure or no event id
    bad = flat.filter(F.col("corrupt_record").isNotNull() | F.col("event_id").isNull())
    reason = F.when(
        F.col("corrupt_record").isNotNull(),
        F.lit("Line is not valid JSON or does not fit the explicit schema")
    ).otherwise(F.lit("event id is missing"))
    raw_text = F.coalesce(
        F.col("corrupt_record"),
        F.to_json(F.struct("event_type", "created_at", "repo_name"))
    )
    bad = bad.select(
        raw_text.alias("raw_record"),
        reason.alias("reason"),
    )
    bad = bad.withColumn("source_file", F.lit(source_file))
    bad = bad.withColumn("ingestion_hour", F.lit(hour))
    bad = bad.withColumn("batch_id", F.lit(batch_id))
    bad = bad.withColumn(
        "quarantine_id",
        F.sha2(F.concat_ws("||", F.col("source_file"), F.coalesce(F.col("raw_record"), F.lit(""))), 256)
    )
    bad = bad.withColumn("load_timestamp", F.current_timestamp())
    bad = bad.dropDuplicates(["quarantine_id"])
    return good, bad

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver helpers
# MAGIC Silver does three things: cast to real types, validate, hash the actor login (PII).
# MAGIC Rows that fail a check go to `silver_quarantine` with a reason. They never stop the batch.

# COMMAND ----------

def make_quarantine(df):
    """Add the key and load_timestamp to a quarantine DataFrame."""
    df = df.withColumn(
        "quarantine_id",
        F.sha2(F.concat_ws("||", F.col("source_file"), F.coalesce(F.col("event_id"), F.lit("")), F.col("reason")), 256)
    )
    df = df.withColumn("load_timestamp", F.current_timestamp())
    return df.dropDuplicates(["quarantine_id"])


def child_quarantine(df, reason_text):
    """Quarantine rows from an event specific table (push, issue, pull request)."""
    out = df.select(
        F.col("event_id").cast("string").alias("event_id"),
        F.col("event_type"),
        F.col("source_file"),
        F.col("ingestion_hour"),
        F.col("bronze_batch_id"),
        F.lit(reason_text).alias("reason"),
    )
    return make_quarantine(out)


def build_silver(bronze_df, salt):
    """
    Turn Bronze rows into Silver DataFrames.
    Returns a dict: silver_event, silver_push, silver_issue, silver_pull_request, quarantine.
    """
    # Step 1: cast with try_cast. A bad value becomes NULL instead of crashing the job.
    typed = bronze_df.select(
        F.col("event_id").alias("event_id_raw"),
        F.expr("try_cast(event_id AS BIGINT)").alias("event_id"),
        F.col("event_type"),
        F.col("actor_id").alias("actor_id_raw"),
        F.expr("try_cast(actor_id AS BIGINT)").alias("actor_id"),
        F.col("actor_login"),
        F.col("repo_id").alias("repo_id_raw"),
        F.expr("try_cast(repo_id AS BIGINT)").alias("repo_id"),
        F.col("repo_name"),
        F.col("org_id").alias("org_id_raw"),
        F.expr("try_cast(org_id AS BIGINT)").alias("org_id"),
        F.col("is_public").alias("is_public_raw"),
        F.expr("try_cast(is_public AS BOOLEAN)").alias("is_public"),
        F.col("created_at").alias("created_at_raw"),
        F.expr("try_cast(created_at AS TIMESTAMP)").alias("created_at"),
        F.col("payload_json"),
        F.col("source_file"),
        F.col("ingestion_hour"),
        F.col("batch_id").alias("bronze_batch_id"),
    )

    # Step 2: validation. Each check adds a reason when a raw value is present but the cast failed.
    reasons = F.concat_ws(
        "; ",
        F.when(F.col("event_id").isNull(), F.lit("event_id is not a valid number")),
        F.when(F.col("event_type").isNull(), F.lit("event_type is missing")),
        F.when(F.col("created_at").isNull(), F.lit("created_at is not a valid timestamp")),
        F.when(F.col("actor_id_raw").isNotNull() & F.col("actor_id").isNull(), F.lit("actor_id is not numeric")),
        F.when(F.col("repo_id_raw").isNotNull() & F.col("repo_id").isNull(), F.lit("repo_id is not numeric")),
        F.when(F.col("org_id_raw").isNotNull() & F.col("org_id").isNull(), F.lit("org_id is not numeric")),
        F.when(F.col("is_public_raw").isNotNull() & F.col("is_public").isNull(), F.lit("public is not true/false")),
    )
    typed = typed.withColumn("reject_reason", F.when(reasons == "", F.lit(None)).otherwise(reasons))

    good = typed.filter(F.col("reject_reason").isNull())
    rejected = typed.filter(F.col("reject_reason").isNotNull())

    quarantine = make_quarantine(rejected.select(
        F.col("event_id_raw").alias("event_id"),
        F.col("event_type"),
        F.col("source_file"),
        F.col("ingestion_hour"),
        F.col("bronze_batch_id"),
        F.col("reject_reason").alias("reason"),
    ))

    # Step 3: silver_event. actor_login is replaced by a salted SHA-256 hash (PII rule from Phase 1).
    events = good.select(
        F.col("event_id"),
        F.col("event_type"),
        F.col("actor_id"),
        F.when(
            F.col("actor_login").isNotNull(),
            F.sha2(F.concat(F.lit(salt), F.col("actor_login")), 256)
        ).alias("actor_login_hash"),
        F.col("repo_id"),
        F.col("repo_name"),
        F.col("org_id"),
        F.col("is_public"),
        F.col("created_at"),
        F.to_date(F.col("created_at")).alias("event_date"),
        F.col("source_file"),
        F.col("ingestion_hour"),
        F.col("bronze_batch_id"),
    ).dropDuplicates(["event_id"])
    events = add_row_hash(events, [
        "event_id", "event_type", "actor_id", "actor_login_hash", "repo_id",
        "repo_name", "org_id", "is_public", "created_at",
    ])
    events = events.withColumn("load_timestamp", F.current_timestamp())

    # Step 4: event specific tables built from the raw payload string.
    base = good.select("event_id", "event_type", "payload_json", "source_file", "ingestion_hour", "bronze_batch_id")

    # PushEvent
    push_raw = base.filter(F.col("event_type") == "PushEvent")
    push_raw = push_raw.withColumn("p", F.from_json(F.col("payload_json"), PUSH_PAYLOAD_SCHEMA))
    push_all = push_raw.select(
        "event_id", "event_type", "source_file", "ingestion_hour", "bronze_batch_id",
        F.expr("try_cast(p.push_id AS BIGINT)").alias("push_id"),
        F.expr("try_cast(p.repository_id AS BIGINT)").alias("repository_id"),
        F.col("p.ref").alias("ref"),
        F.col("p.head").alias("head_sha"),
        F.col("p.before").alias("before_sha"),
    )
    push_ok = push_all.filter(F.col("push_id").isNotNull()).dropDuplicates(["event_id"])
    push_bad = push_all.filter(F.col("push_id").isNull())
    push_ok = push_ok.select("event_id", "push_id", "repository_id", "ref", "head_sha", "before_sha",
                             "source_file", "ingestion_hour")
    push_ok = add_row_hash(push_ok, ["event_id", "push_id", "repository_id", "ref", "head_sha", "before_sha"])
    push_ok = push_ok.withColumn("load_timestamp", F.current_timestamp())

    # IssuesEvent (title, body and user are dropped on purpose: free text and personal data)
    issue_raw = base.filter(F.col("event_type") == "IssuesEvent")
    issue_raw = issue_raw.withColumn("p", F.from_json(F.col("payload_json"), ISSUE_PAYLOAD_SCHEMA))
    issue_all = issue_raw.select(
        "event_id", "event_type", "source_file", "ingestion_hour", "bronze_batch_id",
        F.col("p.action").alias("action"),
        F.expr("try_cast(p.issue.id AS BIGINT)").alias("issue_id"),
        F.expr("try_cast(p.issue.number AS BIGINT)").alias("issue_number"),
        F.col("p.issue.state").alias("state"),
        F.expr("try_cast(p.issue.comments AS BIGINT)").alias("comment_count"),
        F.expr("try_cast(p.issue.created_at AS TIMESTAMP)").alias("issue_created_at"),
        F.expr("try_cast(p.issue.updated_at AS TIMESTAMP)").alias("issue_updated_at"),
        F.expr("try_cast(p.issue.closed_at AS TIMESTAMP)").alias("issue_closed_at"),
    )
    issue_ok = issue_all.filter(F.col("issue_id").isNotNull()).dropDuplicates(["event_id"])
    issue_bad = issue_all.filter(F.col("issue_id").isNull())
    issue_ok = issue_ok.select("event_id", "action", "issue_id", "issue_number", "state", "comment_count",
                               "issue_created_at", "issue_updated_at", "issue_closed_at",
                               "source_file", "ingestion_hour")
    issue_ok = add_row_hash(issue_ok, ["event_id", "action", "issue_id", "issue_number", "state",
                                       "comment_count", "issue_created_at", "issue_updated_at", "issue_closed_at"])
    issue_ok = issue_ok.withColumn("load_timestamp", F.current_timestamp())

    # PullRequestEvent
    pr_raw = base.filter(F.col("event_type") == "PullRequestEvent")
    pr_raw = pr_raw.withColumn("p", F.from_json(F.col("payload_json"), PR_PAYLOAD_SCHEMA))
    pr_all = pr_raw.select(
        "event_id", "event_type", "source_file", "ingestion_hour", "bronze_batch_id",
        F.col("p.action").alias("action"),
        F.expr("try_cast(p.number AS BIGINT)").alias("pr_number"),
        F.expr("try_cast(p.pull_request.id AS BIGINT)").alias("pull_request_id"),
        F.col("p.pull_request.base.ref").alias("base_ref"),
        F.col("p.pull_request.head.ref").alias("head_ref"),
    )
    pr_ok = pr_all.filter(F.col("pull_request_id").isNotNull()).dropDuplicates(["event_id"])
    pr_bad = pr_all.filter(F.col("pull_request_id").isNull())
    pr_ok = pr_ok.select("event_id", "action", "pr_number", "pull_request_id", "base_ref", "head_ref",
                         "source_file", "ingestion_hour")
    pr_ok = add_row_hash(pr_ok, ["event_id", "action", "pr_number", "pull_request_id", "base_ref", "head_ref"])
    pr_ok = pr_ok.withColumn("load_timestamp", F.current_timestamp())

    # Step 5: collect every quarantine row into one DataFrame
    quarantine = quarantine.unionByName(child_quarantine(push_bad, "PushEvent payload: push_id missing or not numeric"))
    quarantine = quarantine.unionByName(child_quarantine(issue_bad, "IssuesEvent payload: issue.id missing or not numeric"))
    quarantine = quarantine.unionByName(child_quarantine(pr_bad, "PullRequestEvent payload: pull_request.id missing or not numeric"))

    return {
        "silver_event": events,
        "silver_push": push_ok,
        "silver_issue": issue_ok,
        "silver_pull_request": pr_ok,
        "quarantine": quarantine,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. MERGE helper
# MAGIC MERGE is what makes the pipeline idempotent:
# MAGIC * new key: INSERT
# MAGIC * existing key and the content changed (different row_hash): UPDATE
# MAGIC * existing key and same content: nothing happens
# MAGIC Running the same file twice therefore inserts 0 rows and updates 0 rows.

# COMMAND ----------

def run_merge(target_table, source_df, key_col, view_name, schema_evolution=False, use_hash=True):
    """MERGE source_df into target_table. Returns (rows_inserted, rows_updated)."""
    source_df.createOrReplaceTempView(view_name)
    evolve = "WITH SCHEMA EVOLUTION " if schema_evolution else ""
    matched = ""
    if use_hash:
        matched = "WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET * "
    sql = (
        "MERGE " + evolve + "INTO " + target_table + " AS t "
        "USING " + view_name + " AS s "
        "ON t." + key_col + " = s." + key_col + " "
        + matched +
        "WHEN NOT MATCHED THEN INSERT *"
    )
    result = spark.sql(sql).collect()[0].asDict()
    inserted = result.get("num_inserted_rows", 0)
    updated = result.get("num_updated_rows", 0)
    return inserted, updated
