# Databricks notebook source
# MAGIC %md
# MAGIC # test_cdc_register  (DEV proof harness)
# MAGIC
# MAGIC Proves `cdc_register_tables` without needing live SQL MI, by feeding it sandbox "source catalog" tables
# MAGIC (shaped exactly like the `sys.tables` / `sys.indexes` queries it runs in JDBC mode) and asserting the
# MAGIC registry it produces. Verifies: CDC vs non-CDC flagging, ordered composite-PK CSV, no-PK tables skipped,
# MAGIC and the `CDC_ONLY` filter. Everything lives under a throwaway schema and can be dropped.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("test_catalog", "medallion")
dbutils.widgets.text("test_schema", "dev_cdc_test")
dbutils.widgets.text("register_notebook", "cdc_register_tables")  # sibling
tc = dbutils.widgets.get("test_catalog").strip(); ts = dbutils.widgets.get("test_schema").strip()
register_nb = dbutils.widgets.get("register_notebook").strip()
SCH = f"{tc}.{ts}"
TABLES = f"{SCH}.src_tables_catalog"
PKS    = f"{SCH}.src_pk_catalog"
REG    = f"{SCH}.bronze_table_registry"

# COMMAND ----------

# DBTITLE 1,Seed the sandbox source catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCH}")
for t in (TABLES, PKS, REG):
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql(f"""CREATE TABLE {TABLES} (source_schema STRING, source_table STRING, has_cdc INT) USING DELTA""")
spark.sql(f"""INSERT INTO {TABLES} VALUES
    ('EmsEvent','IncidentCdcTbl',1),
    ('EmsEvent','IncidentNonCdcTbl',0),
    ('EmsEvent','NoPkTbl',1)""")

spark.sql(f"""CREATE TABLE {PKS} (source_schema STRING, source_table STRING, col STRING, key_ordinal INT) USING DELTA""")
# composite PK seeded out of order to prove ordering by key_ordinal
spark.sql(f"""INSERT INTO {PKS} VALUES
    ('EmsEvent','IncidentCdcTbl','IncidentID',1),
    ('EmsEvent','IncidentNonCdcTbl','TenantID',2),
    ('EmsEvent','IncidentNonCdcTbl','RecordID',1)""")
print("seeded source catalog (NoPkTbl intentionally has no PK rows)")

CHECKS = []
def check(name, ok, detail=""):
    CHECKS.append((name, bool(ok), detail)); print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")

def reg_row(tbl):
    rows = spark.table(REG).filter(f"lower(source_table)=lower('{tbl}')").collect()
    return rows[0].asDict() if rows else None

# COMMAND ----------

# DBTITLE 1,Run register in DELTA mode (register_mode=ALL)
dbutils.notebook.run(register_nb, 600, {
    "source_mode": "DELTA", "source_schema": "EmsEvent", "register_mode": "ALL",
    "registry_table": REG, "delta_tables_catalog": TABLES, "delta_pk_catalog": PKS, "dry_run": "false",
})

cdc = reg_row("IncidentCdcTbl"); noncdc = reg_row("IncidentNonCdcTbl"); nopk = reg_row("NoPkTbl")
check("CDC table registered", cdc is not None, "")
check("CDC table flagged has_cdc=true", bool(cdc and cdc.get("has_cdc")) is True, f"has_cdc={cdc and cdc.get('has_cdc')}")
check("CDC table pk_columns correct", bool(cdc) and cdc["pk_columns"] == "IncidentID", f"pk={cdc and cdc['pk_columns']}")
check("non-CDC table registered", noncdc is not None, "")
check("non-CDC flagged has_cdc=false", bool(noncdc) and not noncdc.get("has_cdc"), f"has_cdc={noncdc and noncdc.get('has_cdc')}")
check("composite PK ordered by key_ordinal", bool(noncdc) and noncdc["pk_columns"] == "RecordID,TenantID", f"pk={noncdc and noncdc['pk_columns']}")
check("no-PK table SKIPPED (not registered)", nopk is None, "NoPkTbl must be absent")
check("registered exactly 2 tables", spark.table(REG).count() == 2, f"count={spark.table(REG).count()}")
display(spark.table(REG).orderBy("source_table"))

# COMMAND ----------

# DBTITLE 1,Run register in CDC_ONLY mode — only the CDC table should land
spark.sql(f"DROP TABLE IF EXISTS {REG}")
dbutils.notebook.run(register_nb, 600, {
    "source_mode": "DELTA", "source_schema": "EmsEvent", "register_mode": "CDC_ONLY",
    "registry_table": REG, "delta_tables_catalog": TABLES, "delta_pk_catalog": PKS, "dry_run": "false",
})
only = [r["source_table"] for r in spark.table(REG).select("source_table").collect()]
check("CDC_ONLY registered just the CDC table", only == ["IncidentCdcTbl"], f"registered={only}")

# COMMAND ----------

# DBTITLE 1,Verdict
fails = [c for c in CHECKS if not c[1]]
print("=" * 70)
for n, ok, d in CHECKS: print(f"[{'PASS' if ok else 'FAIL'}] {n}  {d}")
print("=" * 70)
print(f"{len(CHECKS)-len(fails)}/{len(CHECKS)} checks passed")
if fails:
    raise Exception(f"REGISTER PROOF FAILED: {[c[0] for c in fails]}")
print("REGISTER PROOF PASSED — discovery, CDC flagging, ordered PK CSV, no-PK skip, and CDC_ONLY filter all verified.")
