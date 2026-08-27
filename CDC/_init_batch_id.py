# Databricks notebook source
# MAGIC %md
# MAGIC # _init_batch_id — mint the ONE common batch_id for an orchestrated run
# MAGIC
# MAGIC First task of `Bronze-Ingestion-Orchestrator`. It computes a single `yyyymmddhhmmss` batch_id for the whole
# MAGIC execution and broadcasts it via `dbutils.jobs.taskValues`, so Full Load + Incremental + CDC all stamp the
# MAGIC SAME id. This is what gives the Silver layer cross-table consistency (one run = one batch_id).
# MAGIC
# MAGIC A DAG can pin its own id by passing the `batch_id` job parameter (e.g. to correlate upstream/downstream);
# MAGIC an empty parameter (the default) self-mints here. Either way there is exactly ONE id per run.

# COMMAND ----------

from datetime import datetime, timezone

dbutils.widgets.text("batch_id", "")          # job parameter; empty => mint here
try:
    passed = dbutils.widgets.get("batch_id").strip()
except Exception:
    passed = ""

batch_id = passed or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

# broadcast to every downstream task (they read {{tasks.init.values.batch_id}})
dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
print(f"orchestration batch_id = {batch_id}  (source: {'job parameter' if passed else 'minted'})")
dbutils.notebook.exit(batch_id)
