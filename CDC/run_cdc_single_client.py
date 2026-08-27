# Databricks notebook source
# MAGIC %md
# MAGIC # run_cdc_single_client  —  CDC stream→Bronze, end to end, for ONE client
# MAGIC
# MAGIC Orchestrates the three CDC-path steps in order, passing consistent parameters so they coordinate:
# MAGIC
# MAGIC 1. **Register** (`cdc_register_tables`) — catch the client's CDC-enabled `EmsEvent` tables + PKs into
# MAGIC    `bronze_table_registry`. Without this the next two steps land nothing.
# MAGIC 2. **Land** (`EventHub_To_Bronze_Landing`) — drain the Debezium stream from Event Hub into
# MAGIC    `medallion.bronze.incident_cdc` (registry-filtered).
# MAGIC 3. **Fan out** (`Dynamic_CDC_Bronze_FanOut_FINAL`) — MERGE the landed batch into per-table
# MAGIC    `medallion.bronze.<table>` by PK (insert/update/delete).
# MAGIC
# MAGIC Each step runs via `dbutils.notebook.run`. Set the three `*_notebook_path` widgets to wherever those
# MAGIC notebooks live in your workspace / Git folder (the landing + fan-out already exist; the register notebook is
# MAGIC the sibling in this folder). **DEV only.** This notebook starts/stops nothing destructive and writes no
# MAGIC secrets — every step reads them from the Key Vault scope itself.

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()

# step notebook paths (relative to this notebook's folder, or absolute workspace paths)
dbutils.widgets.text("register_notebook_path", "cdc_register_tables")
dbutils.widgets.text("landing_notebook_path", "../EventHub_To_Bronze_Landing_Final.py")
dbutils.widgets.text("fanout_notebook_path", "../Dynamic_CDC_Bronze_FanOut_FINAL (1)")
dbutils.widgets.text("step_timeout_seconds", "3600")

# which steps to run (skip any that aren't ready, e.g. land if the live stream is down)
dbutils.widgets.text("do_register", "true")
dbutils.widgets.text("do_land", "true")
dbutils.widgets.text("do_fanout", "true")

# shared parameters
dbutils.widgets.text("source_database", "")          # Elite_<client>
dbutils.widgets.text("source_schema", "EmsEvent")
dbutils.widgets.text("registry_table", "medallion.control.bronze_table_registry")
dbutils.widgets.text("landing_table", "medallion.bronze.incident_cdc")
dbutils.widgets.text("secret_scope", "kv-imgtrend-dev-eus")
dbutils.widgets.text("jdbc_host", "")
dbutils.widgets.text("jdbc_user", "")
dbutils.widgets.text("secret_key_password", "debezium-db-password")

g = lambda k: dbutils.widgets.get(k).strip()
register_nb = g("register_notebook_path"); landing_nb = g("landing_notebook_path"); fanout_nb = g("fanout_notebook_path")
timeout = int(g("step_timeout_seconds") or "3600")
do_register = g("do_register").lower() == "true"
do_land = g("do_land").lower() == "true"
do_fanout = g("do_fanout").lower() == "true"
source_database = g("source_database"); source_schema = g("source_schema")
registry_table = g("registry_table"); landing_table = g("landing_table")
secret_scope = g("secret_scope"); jdbc_host = g("jdbc_host"); jdbc_user = g("jdbc_user"); secret_key_password = g("secret_key_password")

# COMMAND ----------

# DBTITLE 1,Run the steps in order, stopping on a hard failure
results = []

def run_step(name, path, params):
    print(f"\n===== {name}  ({path}) =====")
    try:
        out = dbutils.notebook.run(path, timeout, params)
        print(f"{name} OK: {out}")
        results.append((name, "SUCCESS", str(out)[:300]))
        return True
    except Exception as e:
        print(f"{name} FAILED: {str(e)[:500]}")
        results.append((name, "FAILED", str(e)[:300]))
        return False


ok = True
if ok and do_register:
    ok = run_step("1-register", register_nb, {
        "source_mode": "JDBC", "source_database": source_database, "source_schema": source_schema,
        "register_mode": "CDC_ONLY", "registry_table": registry_table,
        "secret_scope": secret_scope, "jdbc_host": jdbc_host, "jdbc_user": jdbc_user,
        "secret_key_password": secret_key_password, "dry_run": "false",
    })

if ok and do_land:
    ok = run_step("2-land", landing_nb, {
        "registry_table": registry_table, "landing_table": landing_table,
        "eh_secret_scope": secret_scope, "starting_offsets": "earliest",
    })

if ok and do_fanout:
    ok = run_step("3-fanout", fanout_nb, {
        "registry_table": registry_table, "landing_table": landing_table,
        "source_schema_filter": source_schema, "latest_batch_only": "true",
    })

# COMMAND ----------

# DBTITLE 1,Summary
summary = spark.createDataFrame(results, "step STRING, status STRING, detail STRING")
display(summary)
failed = [r for r in results if r[1] == "FAILED"]
if failed:
    raise Exception(f"CDC single-client run failed at: {[r[0] for r in failed]}")
print("CDC single-client run complete: register -> land -> fan-out all succeeded.")
