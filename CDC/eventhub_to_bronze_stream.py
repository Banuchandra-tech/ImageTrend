# Databricks notebook source
# MAGIC %md
# MAGIC # eventhub_to_bronze_stream — CDC stream → Bronze, directly, for one client/hub
# MAGIC
# MAGIC Reads the Debezium CDC stream from Event Hub (`debezium-cdc`) and MERGEs each change into its
# MAGIC per-table `medallion.bronze.<bronze_table>` by `[tenant_id] + pk`. One self-contained stage — no
# MAGIC parquet landing zone, no dependency on the team's land/fan-out notebooks. Runs as a normal
# MAGIC Unity-Catalog job (jairo has WRITE on el_bronze, so the checkpoint under `bronze/_checkpoints` and
# MAGIC the managed-table writes both work without the storage account key).
# MAGIC
# MAGIC ### What it depends on (and what it does NOT assume)
# MAGIC * The Debezium connector is configured with `transforms.unwrap.add.fields=op,source.table,source.db,
# MAGIC   source.schema,...`, so each message body carries `__op` / `__source_table` / `__source_db` /
# MAGIC   `__source_schema` / `__source_commit_lsn` alongside the flat business columns. (Routing falls back
# MAGIC   to `__table` if a deployment uses the bare-field form.) Messages with no table id — e.g. Debezium
# MAGIC   heartbeats `{"ts_ms":..}` — are skipped and counted, never written.
# MAGIC * The ingest set + PKs + bronze names come from `medallion.control.bronze_table_registry WHERE
# MAGIC   is_active = true`. A table not active/registered is skipped (never auto-created here).
# MAGIC * `tenant_id` identifies the client. This hub is one source DB = one tenant (`Elite_Develop` = 1);
# MAGIC   a row whose tenant can't be resolved is **dropped, not written** (NULL-tenant rule).
# MAGIC
# MAGIC ### Operating notes
# MAGIC * **DEV only.** Secrets only from the Key-Vault scope. Source is read-only (we only consume the stream).
# MAGIC * `trigger(availableNow=True)` — each run drains everything queued since the last checkpoint, then
# MAGIC   exits. Schedule it (e.g. every 15 min) and the checkpoint makes it an exactly-once micro-batch.
# MAGIC * Deletes: `__op = 'd'` (Debezium `delete.handling.mode=rewrite`) → the row is DELETEd from Bronze by
# MAGIC   `[tenant_id]+pk`; everything else is an upsert.
# MAGIC * **Per-table isolation.** If one table fails to parse/MERGE, its RAW rows are parked in the dead-letter
# MAGIC   table (`medallion.bronze._cdc_stream_deadletter`) and the batch still commits — a single bad table never
# MAGIC   stalls the whole stream. Failures surface as `status=PARTIAL` in the audit log plus rows in the DLQ
# MAGIC   (replay from there after fixing the cause). Only a DLQ-write failure is fatal (batch retries, no loss).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from datetime import datetime, timezone
import json, uuid, time
from collections import defaultdict

# COMMAND ----------

# DBTITLE 1,Feed the team's canonical Bronze control tables (feedback #4) + the Silver keystone
def write_bronze_watermarks(*, success_updates):
    """PER MICRO-BATCH (chunked across the batch's tables, consistent with #7): advance Silver's keystone
    control.batch_watermark + the team's control.bronze_table_watermark for the tables merged in THIS micro-
    batch. Doing this per micro-batch (not only at drain end) means Silver picks up CDC changes as they land,
    even while a long availableNow drain is still catching up. Explicit-column MERGE (omit the identity id);
    idempotent upsert by (tenant_id, schema_name, source_table)."""
    if not success_updates:
        return
    wm = (spark.createDataFrame(success_updates,
             "tenant_id int, schema_name string, source_table string, last_batch_id bigint")
          .withColumn("updated_on", F.current_timestamp()))
    for tbl in ("medallion.control.batch_watermark", "medallion.control.bronze_table_watermark"):
        try:
            (DeltaTable.forName(spark, tbl).alias("t").merge(wm.alias("s"),
                "t.tenant_id=s.tenant_id AND t.schema_name=s.schema_name AND t.source_table=s.source_table")
                .whenMatchedUpdate(set={"last_batch_id": "s.last_batch_id", "updated_on": "s.updated_on"})
                .whenNotMatchedInsert(values={"tenant_id": "s.tenant_id", "schema_name": "s.schema_name",
                      "source_table": "s.source_table", "last_batch_id": "s.last_batch_id",
                      "updated_on": "s.updated_on"}).execute())
        except Exception as e:
            print(f"{tbl} skipped:", str(e)[:150])


def write_bronze_run_audit(*, tenant_id, run_batch, run_start_ts, run_end_ts, pipeline_version,
                           detail_rows, success_updates):
    """RUN-LEVEL (once after the drain): the team's audit ledger control.bronze_pipeline_execution(_detail) and
    the run-level promotion gate control.bronze_batch_watermark. Watermark cursors are NOT written here — they
    are advanced per micro-batch by write_bronze_watermarks. execution_id = yyyymmddHHMMSS+micros. Returns it.
    Mirrors imagetrend-pipelines/.../landing-bronze full load pipeline.py schemas."""
    from decimal import Decimal
    execution_id = datetime.now().strftime("%Y%m%d%H%M%S%f")
    rb = int(run_batch); dur = int((run_end_ts - run_start_ts).total_seconds())
    created = datetime.now(timezone.utc).replace(tzinfo=None)

    n_ok = sum(1 for r in detail_rows if r["status"] == "SUCCESS")
    n_fail = sum(1 for r in detail_rows if r["status"] == "FAILED")
    n_skip = sum(1 for r in detail_rows if r["status"] == "SKIPPED")

    detail_ddl = ("execution_id string, tenant_id int, schema_name string, source_table string, "
                  "batch_id bigint, status string, inserted_rows bigint, error_message string, "
                  "start_ts timestamp, end_ts timestamp, duration_secs bigint, created_ts timestamp")
    drows = [(execution_id, int(tenant_id), r["schema_name"], r["source_table"],
              (rb if r["status"] != "SKIPPED" else None), r["status"], int(r.get("inserted_rows") or 0),
              (r.get("error_message") or None), run_start_ts, run_end_ts, dur, created) for r in detail_rows]
    try:
        if drows:
            spark.createDataFrame(drows, schema=detail_ddl).write.mode("append").saveAsTable(
                "medallion.control.bronze_pipeline_execution_detail")
            print(f"bronze_pipeline_execution_detail: +{len(drows)} (exec {execution_id})")
    except Exception as e:
        print("bronze_pipeline_execution_detail skipped:", str(e)[:150])

    status = "FAILED" if n_fail else "SUCCESS"
    exec_ddl = ("execution_id string, start_ts timestamp, end_ts timestamp, duration_secs bigint, "
                "duration_mins decimal(18,2), total_tables int, total_tenants int, successful_tables int, "
                "failed_tables int, skipped_tables int, successful_batches int, failed_batches int, "
                "skipped_batches int, watermark_updates int, total_inserted_rows bigint, status string, "
                "pipeline_version string, created_ts timestamp")
    erow = (execution_id, run_start_ts, run_end_ts, dur, Decimal(str(round(dur / 60.0, 2))),
            len(detail_rows), 1, n_ok, n_fail, n_skip, n_ok, n_fail, n_skip, len(success_updates),
            sum(int(r.get("inserted_rows") or 0) for r in detail_rows), status, pipeline_version, created)
    try:
        spark.createDataFrame([erow], schema=exec_ddl).write.mode("append").saveAsTable(
            "medallion.control.bronze_pipeline_execution")
        print(f"bronze_pipeline_execution: header (exec {execution_id}, {status})")
    except Exception as e:
        print("bronze_pipeline_execution skipped:", str(e)[:150])

    try:
        if n_fail == 0 and success_updates:
            last = spark.sql("SELECT MAX(batch_id) m FROM medallion.control.bronze_batch_watermark").first()["m"]
            if rb > (last if last is not None else -1):
                bw = spark.createDataFrame([(rb, created, execution_id)],
                        "batch_id bigint, completed_ts timestamp, execution_id string")
                (DeltaTable.forName(spark, "medallion.control.bronze_batch_watermark").alias("t")
                    .merge(bw.alias("s"), "t.batch_id=s.batch_id")
                    .whenNotMatchedInsert(values={"batch_id": "s.batch_id", "completed_ts": "s.completed_ts",
                          "execution_id": "s.execution_id"}).execute())
                print(f"bronze_batch_watermark: promoted {rb}")
    except Exception as e:
        print("bronze_batch_watermark skipped:", str(e)[:150])
    return execution_id

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("eh_namespace", "eus1delteh01")
dbutils.widgets.text("eh_topic", "debezium-cdc")
dbutils.widgets.text("consumer_group", "databricks-cg")
dbutils.widgets.text("eh_secret_key", "eventhub-connection-string")
dbutils.widgets.text("secret_scope", "kv-imgtrend-dev-eus")
dbutils.widgets.text("registry_table", "medallion.control.bronze_table_registry")
dbutils.widgets.text("target_catalog", "medallion")
dbutils.widgets.text("target_schema", "bronze")
dbutils.widgets.text("source_schema", "EmsEvent")        # registry scope for this hub
dbutils.widgets.text("tenant_id", "1")                   # this hub = Elite_Develop = tenant 1
dbutils.widgets.text("expected_source_db", "Elite_Develop")  # cross-check __source_db; mismatch => drop
dbutils.widgets.text("checkpoint_location", "abfss://bronze@eus1deltadls01.dfs.core.windows.net/_checkpoints/eventhub_to_bronze_stream")
dbutils.widgets.text("starting_offsets", "latest")       # 'latest' (default) skips the pre-fix backlog that has no metadata
# HARDENING (2026-06-30 incident): a wide schema-load (~460 tables in one micro-batch) made a 50k-offset batch
# do ~460 Iceberg MERGEs that could not commit within the 60-min timeout -> the checkpoint never advanced ->
# the stream death-spiralled (stuck for ~17h, backlog growing). Bounding the micro-batch keeps the per-batch
# MERGE count small enough to COMMIT, so a wide load degrades to "lagging" (progress survives every batch)
# instead of "stuck forever". 5000 was validated to commit + recover during that incident.
dbutils.widgets.text("max_offsets_per_trigger", "5000")
dbutils.widgets.text("merge_max_workers", "16")  # concurrent per-table MERGEs; higher = faster wide-batch drain
dbutils.widgets.text("wide_batch_warn", "200")   # log a WARNING when a micro-batch touches more tables than this
dbutils.widgets.text("audit_table", "medallion.control.streaming_audit_log")
dbutils.widgets.text("deadletter_table", "medallion.bronze._cdc_stream_deadletter")  # failed tables parked here, not lost
dbutils.widgets.text("reset_checkpoint", "false")        # true = wipe the checkpoint and re-read from starting_offsets (recovery/re-init)
dbutils.widgets.text("batch_id", "")                     # shared yyyymmddhhmmss; orchestrator passes one common id, empty self-mints

g = lambda k: dbutils.widgets.get(k).strip()
EH_NS=g("eh_namespace"); TOPIC=g("eh_topic"); CG=g("consumer_group"); EH_KEY=g("eh_secret_key")
SCOPE=g("secret_scope"); REG=g("registry_table"); TCAT=g("target_catalog"); TSCH=g("target_schema")
SRC_SCHEMA=g("source_schema"); TENANT=int(g("tenant_id")); EXPECT_DB=g("expected_source_db")
CKPT=g("checkpoint_location"); START=g("starting_offsets"); MAXOFF=g("max_offsets_per_trigger")
MERGE_WORKERS=int(g("merge_max_workers") or "16"); WIDE_WARN=int(g("wide_batch_warn") or "200")
AUDIT=g("audit_table"); DLQ=g("deadletter_table")
RESET=g("reset_checkpoint").lower() in ("true","1","yes")
RUN_ID=str(uuid.uuid4())
# run-level batch_id (yyyymmddhhmmss): the orchestrator passes one common id; empty self-mints. Stamped on the
# bronze rows so a whole orchestrated run shares one id (the foreachBatch micro-batch id is still used internally).
_bid = g("batch_id")
if not _bid:  # in the orchestrator, read the ONE common id the init task minted; standalone => self-mint
    try: _bid = (dbutils.jobs.taskValues.get(taskKey="init", key="batch_id", default="") or "").strip()
    except Exception: _bid = ""
RUN_BATCH=int(_bid or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))

# Accumulate per-table results ACROSS the whole availableNow drain so we can write the team's run-level control
# tables ONCE after the drain (feedback #4). foreachBatch runs micro-batches sequentially, and we update ACC in
# process_batch's main thread (not the inner pool), so this needs no locking. status FAILED = the table dead-lettered.
ACC = defaultdict(lambda: {"upserts": 0, "deletes": 0, "status": "SUCCESS", "error": None})

# metadata columns the pipeline writes — everything else in a Bronze table is a business column we parse from JSON
META_COLS={"tenant_id","_op","_ingest_date","ingest_date","batch_id","run_id","source_log_position",
           "ingestion_time","ingest_ts","after_json","debezium_ts_ms","eh_enqueued_time","commit_lsn",
           "operation_type","source_db","source_schema","source_table","_deleted","__deleted"}

print(f"hub={TOPIC} tenant={TENANT} src_schema={SRC_SCHEMA} target={TCAT}.{TSCH} start={START} run_id={RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Registry snapshot (active tables for this hub's schema) — keyed by lower(source_table)
# source_schema: CSV of schemas, or "*" for every registered schema. Debezium captures whatever has CDC
# enabled at the source (20 schemas for a full client), so a single-schema consumer strands the rest.
SRC_SCHEMAS = None if SRC_SCHEMA.strip() == "*" else {
    s.strip().lower() for s in SRC_SCHEMA.split(",") if s.strip()}
_reg = spark.table(REG).filter("is_active = true")
if SRC_SCHEMAS:
    _reg = _reg.filter(F.lower(F.trim("source_schema")).isin(list(SRC_SCHEMAS)))
reg_rows = _reg.select("source_schema", "source_table", "bronze_table", "pk_columns").collect()
REGISTRY = {}   # keyed by (schema, table): a name-only key collides across schemas
for r in reg_rows:
    pk = [c.strip() for c in (r["pk_columns"] or "").split(",") if c.strip()]
    if r["bronze_table"] and pk:
        REGISTRY[((r["source_schema"] or "").strip().lower(), r["source_table"].lower())] = \
            {"bronze_table": r["bronze_table"], "pk": pk}
_scope = "ALL schemas" if SRC_SCHEMAS is None else ",".join(sorted(SRC_SCHEMAS))
print(f"active registered tables with PK [{_scope}]: {len(REGISTRY)} "
      f"across {len({k[0] for k in REGISTRY})} schema(s)")

# cache each target table's business schema (parsed from JSON) once
_schema_cache = {}
def business_schema(bronze_fqn):
    """Temporal columns are parsed as STRING, not as their bronze type: Debezium emits SQL Server
    date/time columns as epoch integers, and from_json against a TIMESTAMP/DATE field silently
    NULLs a JSON number. decode_temporals() converts them back. (Same fix as cdc/stage_compact.py.)"""
    if bronze_fqn not in _schema_cache:
        tbl = spark.table(bronze_fqn)
        fields, temporal = [], []
        for f in tbl.schema.fields:
            if f.name.lower() in META_COLS:
                continue
            kind = f.dataType.typeName()
            if kind in ("timestamp", "timestamp_ntz", "date"):
                temporal.append((f.name, kind))
                fields.append(StructField(f.name, StringType(), True))
            else:
                fields.append(f)
        _schema_cache[bronze_fqn] = (StructType(fields), set(tbl.columns), temporal)
    return _schema_cache[bronze_fqn]

STAMPED_COLS = {"tenant_id", "_op", "source_log_position", "batch_id", "run_id", "_ingest_date", "ingestion_time"}

def payload_columns(rows, json_col="v"):
    """Columns the Debezium payload actually carries. The SQL Server capture instances are frozen at
    the schema they were created with, so columns added later (ModifiedOn/CreatedOn/CreatedBy/
    GlobalIdentifier/FormID on ~all Elite_Mississippi tables) are ABSENT from the JSON. They parse as
    NULL, and updateAll would then SET them to NULL, destroying the full-load value. Merge only what
    the payload carries. (A *captured* null is serialized as `"col":null`, so absence == not captured.)"""
    keys = (rows.select(F.explode(F.map_keys(F.from_json(json_col, "map<string,string>"))).alias("k"))
                .distinct().collect())
    return {r["k"].lower() for r in keys}

def decode_temporals(df, temporal):
    """Debezium epoch-int (or ISO string) -> real timestamp/date; unit inferred from magnitude."""
    for name, kind in temporal:
        v = F.col(f"`{name}`")
        is_int = v.rlike("^-?[0-9]+$")
        n = v.cast("decimal(38,0)")
        if kind == "date":
            conv = F.when(is_int, F.date_add(F.lit("1970-01-01").cast("date"), n.cast("int"))) \
                    .otherwise(F.to_date(v))
        else:
            a = F.abs(n)
            conv = (F.when(is_int & (a < F.lit(100000000000)),       F.timestamp_seconds(n.cast("long")))
                     .when(is_int & (a < F.lit(100000000000000)),    F.timestamp_millis(n.cast("long")))
                     .when(is_int & (a < F.lit(100000000000000000)), F.timestamp_micros(n.cast("long")))
                     .when(is_int,                                   F.timestamp_micros((n / F.lit(1000)).cast("long")))
                     .otherwise(F.to_timestamp(v)))
        df = df.withColumn(name, conv.cast(kind))
    return df

# COMMAND ----------

# DBTITLE 1,Event Hub source
conn = dbutils.secrets.get(SCOPE, EH_KEY)
sasl = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn}";')
kafka_opts = {
    "kafka.bootstrap.servers": f"{EH_NS}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": sasl, "subscribe": TOPIC, "kafka.group.id": CG,
    "startingOffsets": START, "maxOffsetsPerTrigger": MAXOFF, "failOnDataLoss": "false",
}

# COMMAND ----------

# DBTITLE 1,Dead-letter: park a table's raw rows when it fails, so one bad table never stalls the stream
def ensure_deadletter():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {DLQ} (
        dead_lettered_at TIMESTAMP, run_id STRING, batch_id BIGINT, source_schema STRING,
        source_table STRING, source_db STRING, op STRING, commit_lsn STRING, error STRING, raw_value STRING
    ) USING DELTA""")

def dead_letter(raw_tbl, tbl, batch_id, err):
    """Append a failed table's RAW messages (the full JSON value + metadata) to the DLQ. Nothing is lost:
    the rows can be replayed from here after the cause (schema drift, bad PK, …) is fixed."""
    ensure_deadletter()
    (raw_tbl.select(
        F.current_timestamp().alias("dead_lettered_at"), F.lit(RUN_ID).alias("run_id"),
        F.lit(int(batch_id)).cast("bigint").alias("batch_id"), F.col("__sch").alias("source_schema"),
        F.lit(tbl).alias("source_table"), F.col("__db").alias("source_db"), F.col("__op").alias("op"),
        F.col("__lsn").alias("commit_lsn"), F.lit(err).alias("error"), F.col("v").alias("raw_value"))
     .write.format("delta").mode("append").saveAsTable(DLQ))

# COMMAND ----------

# DBTITLE 1,Per-batch: parse → route by __source_table → MERGE (upsert / delete) by [tenant_id]+pk
def process_batch(batch_df, batch_id):
    base = (batch_df.select(F.col("value").cast("string").alias("v"))
            .withColumn("__op", F.get_json_object("v", "$.__op"))
            .withColumn("__tbl", F.coalesce(F.get_json_object("v", "$.__source_table"),
                                            F.get_json_object("v", "$.__table")))
            .withColumn("__sch", F.coalesce(F.get_json_object("v", "$.__source_schema"), F.lit(SRC_SCHEMA)))
            .withColumn("__db",  F.get_json_object("v", "$.__source_db"))
            .withColumn("__lsn", F.coalesce(F.get_json_object("v", "$.__source_commit_lsn"),
                                            F.get_json_object("v", "$.__commit_lsn")))
            .cache())
    total = base.count()
    routable = base.filter(F.col("__tbl").isNotNull())
    # tenant cross-check: this hub is one DB; drop rows whose __source_db contradicts the expected client
    if EXPECT_DB:
        routable = routable.filter((F.col("__db").isNull()) | (F.lower("__db") == EXPECT_DB.lower()))

    routable = routable.cache()
    skipped_no_table = total - routable.count()
    # __source_db is cross-checked above; __source_schema needs the same guard. REGISTRY is scoped to
    # SRC_SCHEMA but keyed by table NAME, and 10 names (incident, patient, fieldvalue, ...) exist in 2-3
    # source schemas -- so a sibling schema's row would resolve to THIS schema's bronze table and MERGE
    # into the wrong destination. Report what is dropped; never mis-route it.
    _by_sch = {r["__sch"]: r["n"] for r in
               routable.groupBy("__sch").count().withColumnRenamed("count", "n").collect()}
    _foreign = {} if SRC_SCHEMAS is None else {
        k: v for k, v in _by_sch.items() if (k or "").strip().lower() not in SRC_SCHEMAS}
    if _foreign:
        print(f"OUT-OF-SCOPE SCHEMA: dropping {sum(_foreign.values())} rows not in scope -> {_foreign} "
              f"(wrong hub, or a same-named table from a sibling schema)")
        routable = routable.filter(F.lower(F.trim("__sch")).isin(list(SRC_SCHEMAS))).cache()
    tables_in_batch = [(r["__sch"], r["__tbl"]) for r in
                       routable.select("__sch", "__tbl").distinct().collect()]
    # HARDENING: per-batch MERGE cost is O(distinct tables). Surface batch WIDTH so a schema-wide load (the
    # 2026-06-30 failure mode) is visible early — before it can slow commits — rather than only as a timeout.
    if len(tables_in_batch) > WIDE_WARN:
        print(f"WIDE_BATCH WARNING: micro-batch touches {len(tables_in_batch)} tables (> {WIDE_WARN}); "
              f"{len(tables_in_batch)} MERGEs this batch. If commits slow, lower max_offsets_per_trigger.")
    # pre-warm the business-schema cache single-threaded so worker threads only READ it (no dict race)
    for _sch, _t in tables_in_batch:
        _m = REGISTRY.get(((_sch or "").strip().lower(), (_t or "").lower()))
        if _m:
            try: business_schema(f"{TCAT}.{TSCH}.{_m['bronze_table']}")
            except Exception: pass

    from pyspark.sql.window import Window
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def process_one_table(sch_tbl):
        sch, tbl = sch_tbl
        meta = REGISTRY.get(((sch or "").strip().lower(), (tbl or "").lower()))
        if not meta:
            return {"tbl": tbl, "unregistered": True}
        bronze_fqn = f"{TCAT}.{TSCH}.{meta['bronze_table']}"
        rows = routable.filter((F.lower("__tbl") == (tbl or "").lower()) &
                               (F.lower(F.trim("__sch")) == (sch or "").strip().lower()))
        try:
            schema, target_cols, temporal = business_schema(bronze_fqn)
            parsed = rows.withColumn("d", F.from_json("v", schema)).select("d.*", "__op", "__lsn")
            parsed = (decode_temporals(parsed, temporal)   # else every date col lands NULL
                      .withColumn("tenant_id", F.lit(TENANT).cast("int"))
                      .withColumn("_op", F.coalesce(F.col("__op"), F.lit("u")))
                      .withColumn("source_log_position", F.col("__lsn"))
                      .withColumn("batch_id", F.lit(RUN_BATCH).cast("bigint"))
                      .withColumn("run_id", F.lit(RUN_ID))
                      .withColumn("_ingest_date", F.current_date())
                      .withColumn("ingestion_time", F.current_timestamp())
                      .drop("__op", "__lsn"))
            # only columns the payload carries (+ the ones we stamp): an uncaptured column parses as
            # NULL and updateAll would null out the full-load value. Excluded => left untouched.
            present = payload_columns(rows)
            parsed = parsed.select(*[c for c in parsed.columns if c in target_cols
                                     and (c.lower() in present or c.lower() in STAMPED_COLS)])
            keys = ["tenant_id"] + meta["pk"]
            if any(k not in parsed.columns for k in keys):
                raise Exception(f"missing key col(s) {[k for k in keys if k not in parsed.columns]}")
            # dedupe to the latest row per key within the batch (highest commit_lsn wins)
            w = Window.partitionBy(*[F.col(f"`{k}`") for k in keys]).orderBy(F.col("source_log_position").desc_nulls_last())
            parsed = parsed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn").cache()
            try:
                # Case-fold STRING keys like the full load (source collation is case-INsensitive; JDBC
                # UPPERCASE vs landing lowercase GUIDs would otherwise MERGE-miss and insert a duplicate).
                _kt = {f.name.lower(): f.dataType.typeName() for f in parsed.schema.fields}
                cond = " AND ".join(
                    (f"upper(t.`{k}`) <=> upper(s.`{k}`)" if _kt.get(k.lower()) == "string"
                     else f"t.`{k}` <=> s.`{k}`") for k in keys)
                dt = DeltaTable.forName(spark, bronze_fqn)
                upserts = parsed.filter("lower(coalesce(_op,'u')) <> 'd'")
                deletes = parsed.filter("lower(_op) = 'd'")
                nu = upserts.count(); nd = deletes.count()
                if nu:
                    dt.alias("t").merge(upserts.alias("s"), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                if nd:
                    dt.alias("t").merge(deletes.alias("s"), cond).whenMatchedDelete().execute()
                return {"tbl": tbl, "upserts": nu, "deletes": nd}
            finally:
                parsed.unpersist()
        except Exception as e:
            # per-table isolation: park this table's RAW rows in the DLQ; the rest of the batch is unaffected.
            try:
                dead_letter(rows, tbl, batch_id, str(e)[:300])
                return {"tbl": tbl, "dead_lettered": str(e)[:160]}
            except Exception as de:
                return {"tbl": tbl, "fatal": f"{tbl}: DLQ write failed: {str(de)[:140]} | orig: {str(e)[:120]}"}

    stats = {"batch_id": batch_id, "total": total, "tables": 0, "upserts": 0, "deletes": 0,
             "skipped_no_table": skipped_no_table, "skipped_unregistered": [],
             "dead_lettered": [], "fatal": []}
    # MERGE the batch's tables CONCURRENTLY — sequential per-table MERGEs (50+ tables, Iceberg overhead each)
    # dominated runtime. Each table is an independent target, so this is safe.
    mb_success = []   # tables that merged OK in THIS micro-batch -> advance their Silver cursor now
    with ThreadPoolExecutor(max_workers=MERGE_WORKERS) as ex:   # configurable; more concurrent MERGEs drain a wide batch faster
        for fut in as_completed([ex.submit(process_one_table, t) for t in tables_in_batch]):
            r = fut.result()
            if r.get("unregistered"):   stats["skipped_unregistered"].append(r["tbl"])
            elif r.get("fatal"):        stats["fatal"].append(r["fatal"])
            elif "dead_lettered" in r:
                stats["dead_lettered"].append({"table": r["tbl"], "error": r["dead_lettered"]})
                ACC[r["tbl"]]["status"] = "FAILED"; ACC[r["tbl"]]["error"] = r["dead_lettered"]
            else:
                stats["tables"] += 1; stats["upserts"] += r["upserts"]; stats["deletes"] += r["deletes"]
                ACC[r["tbl"]]["upserts"] += r["upserts"]; ACC[r["tbl"]]["deletes"] += r["deletes"]
                mb_success.append(r["tbl"])
    routable.unpersist()

    # Advance Silver's keystone for the tables merged in THIS micro-batch (one chunked MERGE, #7), so Silver
    # sees CDC as it lands even while a long drain is still catching up. Audit stays run-level (after the drain).
    if mb_success:
        write_bronze_watermarks(success_updates=[(int(TENANT), SRC_SCHEMA, t, int(RUN_BATCH)) for t in mb_success])

    base.unpersist()
    status = "FATAL" if stats["fatal"] else ("PARTIAL" if stats["dead_lettered"] else "SUCCESS")
    print("BATCH " + json.dumps({**stats, "status": status,
          "skipped_unregistered": stats["skipped_unregistered"][:15]}, default=str))
    # Per-table results accumulate into ACC; the team's run-level control tables are written ONCE after the
    # drain (below), not per micro-batch — that is feedback #4 ("use the team's existing control tables").
    # A table that failed to parse/MERGE is parked in the DLQ and we keep going (one bad table never stalls CDC).
    # Only a dead-letter WRITE failure is truly fatal — raise so the batch is retried and nothing is lost.
    if stats["fatal"]:
        raise Exception(f"batch {batch_id} FATAL — could not dead-letter: {stats['fatal'][:5]}")

# COMMAND ----------

# DBTITLE 1,Run one drain (availableNow) — schedule this notebook to make it a micro-batch stream
if RESET:
    print(f"reset_checkpoint=true → wiping {CKPT} and re-reading from startingOffsets={START}")
    try: dbutils.fs.rm(CKPT, True)
    except Exception as e: print(f"checkpoint wipe note: {str(e)[:120]}")

drain_start_ts = datetime.now(timezone.utc).replace(tzinfo=None)
q = (spark.readStream.format("kafka").options(**kafka_opts).load()
     .writeStream.foreachBatch(process_batch)
     .option("checkpointLocation", CKPT).trigger(availableNow=True).start())
q.awaitTermination()

# COMMAND ----------

# DBTITLE 1,After the drain: write the team's run-level AUDIT ledger (watermark cursors were written per micro-batch)
# Silver's keystone (control.batch_watermark) was already advanced per micro-batch by write_bronze_watermarks,
# so Silver sees CDC even if this drain timed out mid-way. Here we add the run-level audit + promotion gate.
drain_end_ts = datetime.now(timezone.utc).replace(tzinfo=None)
if ACC:
    detail_rows = [{"schema_name": SRC_SCHEMA, "source_table": tbl, "status": st["status"],
                    "inserted_rows": st["upserts"] + st["deletes"], "error_message": st["error"]}
                   for tbl, st in ACC.items()]
    success_updates = [(int(TENANT), SRC_SCHEMA, tbl, int(RUN_BATCH))
                       for tbl, st in ACC.items() if st["status"] == "SUCCESS"]
    exec_id = write_bronze_run_audit(tenant_id=TENANT, run_batch=RUN_BATCH,
                  run_start_ts=drain_start_ts, run_end_ts=drain_end_ts, pipeline_version="bronze-cdc-1.0",
                  detail_rows=detail_rows, success_updates=success_updates)
    print(f"CDC drain audit written: {len(detail_rows)} tables, {len(success_updates)} succeeded "
          f"@ batch_id={RUN_BATCH} (execution_id={exec_id})")
else:
    print("CDC drain processed 0 changed registry tables — no control-table writes this run.")
print("drain complete")
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "status": "ok"}))
