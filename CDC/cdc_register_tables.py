# Databricks notebook source
# MAGIC %md
# MAGIC # cdc_register_tables  —  "catch the tables that have CDC"
# MAGIC
# MAGIC This is the driver that makes the CDC stream→Bronze path actually work for a client. The landing notebook
# MAGIC (`EventHub_To_Bronze_Landing`) **inner-joins** every Debezium event against `bronze_table_registry`, and the
# MAGIC fan-out (`Dynamic_CDC_Bronze_FanOut_FINAL`) **skips any table whose PK is not registered**. So until a client's
# MAGIC tables are registered, the CDC path lands nothing. Nothing else in the repo populates that registry — this
# MAGIC notebook does.
# MAGIC
# MAGIC For one `Elite_<client>` database it:
# MAGIC 1. discovers every `EmsEvent` table and whether each currently has **CDC enabled** (`is_tracked_by_cdc`),
# MAGIC 2. reads each table's **primary-key columns** (in key order),
# MAGIC 3. MERGEs them into `medallion.control.bronze_table_registry`. **Curated columns (`is_active`,
# MAGIC    `bronze_table`, `pk_columns`) are NEVER overwritten on an existing row** — they are the ingest gate,
# MAGIC    the target-table name, and the merge key, all owned by the team's curation. A match only refreshes
# MAGIC    operational metadata (`batch_group`, `has_cdc`, `updated_on`). A brand-new table is INSERTED with
# MAGIC    `bronze_table = lower(schema)_lower(table)`, PKs from the source, and **`is_active = false`** so it is
# MAGIC    surfaced for review without silently expanding ingest scope.
# MAGIC
# MAGIC Re-run it whenever CDC is enabled on more tables: it surfaces newly-CDC tables (and any new tables) without
# MAGIC disturbing curated mappings. CDC tables → owned by the fan-out/stream; non-CDC → `incremental_non_cdc_load`.
# MAGIC
# MAGIC **DEV only. Source is READ-ONLY** — this notebook only runs catalog SELECTs (`sys.tables`, `sys.indexes`, …);
# MAGIC it never enables CDC (a privileged DBA action) and never alters the source. Secrets come only from the Key
# MAGIC Vault scope.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()

dbutils.widgets.text("source_mode", "JDBC")          # JDBC = read SQL MI catalog; DELTA = read provided catalog tables (testing)
dbutils.widgets.text("source_database", "")          # the Elite_<client> database
dbutils.widgets.text("source_schema", "EmsEvent")
dbutils.widgets.text("register_mode", "ALL")         # ALL | CDC_ONLY | NON_CDC_ONLY
dbutils.widgets.text("registry_table", "medallion.control.bronze_table_registry")
dbutils.widgets.text("bronze_lowercase", "false")    # lowercase the derived bronze_table name
dbutils.widgets.text("default_batch_group", "1")
dbutils.widgets.text("dry_run", "false")

# connection (JDBC). password ALWAYS from the scope.
dbutils.widgets.text("secret_scope", "kv-imgtrend-dev-eus")
dbutils.widgets.text("jdbc_host", "")
dbutils.widgets.text("jdbc_port", "1433")
dbutils.widgets.text("jdbc_user", "")
dbutils.widgets.text("secret_key_password", "debezium-db-password")
dbutils.widgets.text("secret_key_host", "")
dbutils.widgets.text("secret_key_user", "")
dbutils.widgets.text("trust_server_certificate", "true")  # SQL MI public endpoint (…public…:3342) needs trust=true

# DELTA test mode: Delta tables shaped like the catalog queries below
dbutils.widgets.text("delta_tables_catalog", "")     # cols: source_schema, source_table, has_cdc
dbutils.widgets.text("delta_pk_catalog", "")         # cols: source_schema, source_table, col, key_ordinal

source_mode      = dbutils.widgets.get("source_mode").strip().upper()
source_database  = dbutils.widgets.get("source_database").strip()
source_schema    = dbutils.widgets.get("source_schema").strip()
register_mode    = dbutils.widgets.get("register_mode").strip().upper()
registry_table   = dbutils.widgets.get("registry_table").strip()
bronze_lowercase = dbutils.widgets.get("bronze_lowercase").strip().lower() == "true"
default_batch_group = int(dbutils.widgets.get("default_batch_group").strip() or "1")
dry_run          = dbutils.widgets.get("dry_run").strip().lower() == "true"

secret_scope        = dbutils.widgets.get("secret_scope").strip()
jdbc_host           = dbutils.widgets.get("jdbc_host").strip()
jdbc_port           = dbutils.widgets.get("jdbc_port").strip() or "1433"
jdbc_user           = dbutils.widgets.get("jdbc_user").strip()
secret_key_password = dbutils.widgets.get("secret_key_password").strip()
secret_key_host     = dbutils.widgets.get("secret_key_host").strip()
secret_key_user     = dbutils.widgets.get("secret_key_user").strip()
trust_server_cert   = dbutils.widgets.get("trust_server_certificate").strip().lower() in ("true", "1", "yes")
delta_tables_catalog = dbutils.widgets.get("delta_tables_catalog").strip()
delta_pk_catalog     = dbutils.widgets.get("delta_pk_catalog").strip()

assert source_mode in ("JDBC", "DELTA")
assert register_mode in ("ALL", "CDC_ONLY", "NON_CDC_ONLY")
assert source_schema, "source_schema is required"

run_id = str(uuid.uuid4())
now_ts = datetime.now(timezone.utc).replace(tzinfo=None)
print(f"source_mode={source_mode}  source={source_database}.{source_schema}  register_mode={register_mode}  dry_run={dry_run}")

# COMMAND ----------

# DBTITLE 1,Connection helper (password only from the scope, never printed)
def _read_jdbc_query(query):
    host = dbutils.secrets.get(secret_scope, secret_key_host) if secret_key_host else jdbc_host
    user = dbutils.secrets.get(secret_scope, secret_key_user) if secret_key_user else jdbc_user
    password = dbutils.secrets.get(secret_scope, secret_key_password)
    assert host and user and source_database, "jdbc_host, jdbc_user and source_database are required in JDBC mode"
    url = (f"jdbc:sqlserver://{host}:{jdbc_port};databaseName={source_database};"
           f"encrypt=true;trustServerCertificate={'true' if trust_server_cert else 'false'};loginTimeout=30;")
    return (spark.read.format("jdbc")
            .option("url", url).option("user", user).option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("query", query).load())

# COMMAND ----------

# DBTITLE 1,Discover EmsEvent tables (+ CDC flag) and primary keys from the source catalog
if source_mode == "JDBC":
    tables_q = (
        "SELECT s.name AS source_schema, t.name AS source_table, "
        "CAST(t.is_tracked_by_cdc AS INT) AS has_cdc "
        "FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
        # never register SQL Server CDC change tables (cdc schema, or a *_CT shadow) — not business data
        f"WHERE s.name = '{source_schema}' AND s.name <> 'cdc' AND RIGHT(t.name, 3) <> '_CT'"
    )
    pk_q = (
        "SELECT s.name AS source_schema, t.name AS source_table, c.name AS col, ic.key_ordinal AS key_ordinal "
        "FROM sys.indexes i "
        "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "JOIN sys.tables t ON t.object_id = i.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        f"WHERE i.is_primary_key = 1 AND s.name = '{source_schema}'"
    )
    tables_df = _read_jdbc_query(tables_q)
    pk_df = _read_jdbc_query(pk_q)
else:  # DELTA test mode
    assert delta_tables_catalog and delta_pk_catalog, "delta_tables_catalog and delta_pk_catalog required in DELTA mode"
    tables_df = spark.table(delta_tables_catalog).select("source_schema", "source_table", F.col("has_cdc").cast("int").alias("has_cdc"))
    pk_df = spark.table(delta_pk_catalog).select("source_schema", "source_table", "col", F.col("key_ordinal").cast("int").alias("key_ordinal"))

tables_df = tables_df.withColumn("has_cdc", F.col("has_cdc").cast("boolean"))

# pk_columns as an ordered CSV per table
pk_csv_df = (
    pk_df.groupBy(F.lower("source_schema").alias("sk"), F.lower("source_table").alias("tk"))
    .agg(F.sort_array(F.collect_list(F.struct(F.col("key_ordinal"), F.col("col")))).alias("pk_struct"))
    .withColumn("pk_columns", F.concat_ws(",", F.expr("transform(pk_struct, x -> x.col)")))
    .select("sk", "tk", "pk_columns")
)

# COMMAND ----------

# DBTITLE 1,Build the registry rows (derive bronze_table, attach PK + CDC flag, apply register_mode)
# bronze_table convention = lower(schema)_lower(table), matching the actual managed tables
# (e.g. EmsEvent.Incident -> emsevent_incident). NOTE: this is only used when INSERTING a brand-new
# table; an existing row's curated bronze_table is preserved (never overwritten) — see the MERGE below.
bronze_expr = (F.lower(F.col("source_table")) if bronze_lowercase
               else F.concat_ws("_", F.lower(F.col("source_schema")), F.lower(F.col("source_table"))))

candidates = (
    tables_df
    .withColumn("sk", F.lower("source_schema"))
    .withColumn("tk", F.lower("source_table"))
    .join(pk_csv_df, ["sk", "tk"], "left")
    .withColumn("bronze_table", bronze_expr)
    # is_active is the curated ingest gate — a newly discovered table is registered INACTIVE so it is
    # never auto-ingested / never silently expands scope. The team activates it deliberately.
    .withColumn("is_active", F.lit(False))
    .withColumn("batch_group", F.lit(default_batch_group).cast("int"))
    .withColumn("updated_on", F.lit(now_ts).cast("timestamp"))
    .select("source_schema", "source_table", "bronze_table", "pk_columns", "has_cdc",
            "is_active", "batch_group", "updated_on")
)

if register_mode == "CDC_ONLY":
    candidates = candidates.filter(F.col("has_cdc") == True)  # noqa: E712
elif register_mode == "NON_CDC_ONLY":
    candidates = candidates.filter((F.col("has_cdc") == False) | F.col("has_cdc").isNull())  # noqa: E712

# tables with no PK cannot be merged by the fan-out or the non-CDC loader — register them inactive + log
no_pk = candidates.filter(F.col("pk_columns").isNull() | (F.length(F.trim(F.col("pk_columns"))) == 0))
to_register = candidates.filter(F.col("pk_columns").isNotNull() & (F.length(F.trim(F.col("pk_columns"))) > 0))

n_total = candidates.count()
n_cdc = candidates.filter(F.col("has_cdc") == True).count()  # noqa: E712
n_nopk = no_pk.count()
n_reg = to_register.count()
print(f"discovered={n_total}  cdc_enabled={n_cdc}  non_cdc={n_total - n_cdc}  no_pk(skipped)={n_nopk}  to_register={n_reg}")
if n_nopk:
    print("tables skipped (no primary key):")
    display(no_pk.select("source_schema", "source_table", "has_cdc"))
print("rows to register:")
display(to_register.orderBy("has_cdc", "source_table"))

# COMMAND ----------

# DBTITLE 1,MERGE into bronze_table_registry (only the columns the registry actually has)
def ensure_registry():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {registry_table} (
            is_active     BOOLEAN,
            source_schema STRING,
            source_table  STRING,
            bronze_table  STRING,
            pk_columns    STRING,
            batch_group   INT,
            topic_group   STRING,
            tenant_group  STRING,
            has_cdc       BOOLEAN,
            updated_on    TIMESTAMP
        ) USING DELTA
    """)

if dry_run:
    print("dry_run=true — nothing written. The rows above are what WOULD be registered.")
else:
    ensure_registry()
    reg_cols = {c.lower() for c in spark.table(registry_table).columns}
    # only set columns the registry actually has, so we never break the existing landing/fan-out readers
    owned = [c for c in ["is_active", "bronze_table", "pk_columns", "batch_group", "has_cdc", "updated_on"] if c in reg_cols]
    # CURATED columns — never overwrite these on an existing row. is_active is the ingest gate; bronze_table
    # is the target table name the landing/fan-out/stream all route on; pk_columns is the merge key. Clobbering
    # any of them silently breaks ingestion (re-activating disabled tables, pointing at a non-existent bronze
    # table, or changing the merge key). They are set ONLY when inserting a brand-new table. On a match we
    # refresh just the operational metadata (batch_group, has_cdc, updated_on).
    CURATED = {"is_active", "bronze_table", "pk_columns"}
    update_cols = [c for c in owned if c not in CURATED]
    src = to_register.select("source_schema", "source_table",
                             *[c for c in ["bronze_table", "pk_columns", "is_active", "batch_group", "has_cdc", "updated_on"] if c in reg_cols])
    (DeltaTable.forName(spark, registry_table).alias("t")
        .merge(src.alias("s"),
               "lower(t.source_schema) = lower(s.source_schema) AND lower(t.source_table) = lower(s.source_table)")
        .whenMatchedUpdate(set={c: f"s.{c}" for c in update_cols})
        .whenNotMatchedInsert(values={**{"source_schema": "s.source_schema", "source_table": "s.source_table"},
                                      **{c: f"s.{c}" for c in owned}})
        .execute())
    print(f"registered/updated {n_reg} tables in {registry_table}")
    display(spark.table(registry_table)
            .filter(F.lower("source_schema") == source_schema.lower())
            .select("source_schema", "source_table", "bronze_table", "pk_columns",
                    *[c for c in ["has_cdc", "is_active"] if c in reg_cols])
            .orderBy("source_table"))
