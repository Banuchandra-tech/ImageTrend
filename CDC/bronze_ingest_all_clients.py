# Databricks notebook source
# =====================================================
# BRONZE INGESTION ORCHESTRATOR - ALL ACTIVE CLIENTS
# =====================================================
# ADF's "Bronze Ingestion Orchestartor" node calls THIS job.
#
# Why it exists: green's ADF node used to call Bronze-Full-Load-Green, a full-load job that
# does NOT write control.bronze_batch_watermark. Silver selects work with
#     WHERE bronze_batch.batch_id > COALESCE(registry.batch_id, -1)
# so with no new watermark every silver table returned "No batch available" and was SKIPPED:
# 215 skipped / 0 successful / 0 rows, while the job still reported SUCCESS. Blue's node
# calls the real per-client bronze pipeline, which does write the watermark.
#
# Blue runs a single client. Green runs ALL ACTIVE clients, discovered from the control
# table each run, so onboarding a client needs no edit here.
#
# Fails loudly: raises if any client's pipeline fails, and raises if an active client has no
# bronze job - a client silently not ingesting is the failure mode this must never hide.
import json, re, time
from datetime import timedelta, datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("secret_scope", "kv-eus1-p-dat-plt-kvt-02")
dbutils.widgets.text("server", "eus1pdatpltsql02.database.windows.net")
dbutils.widgets.text("database", "Elite_ADF_Metadata_DB")
dbutils.widgets.text("sql_user", "imagetrend")
dbutils.widgets.text("job_prefix", "Bronze-Pipeline-Green-")
dbutils.widgets.text("dry_run", "true")
# The SDK's .result() defaults to a 20-MINUTE timeout. Bronze pipelines routinely run
# longer, and the default made this job report FAILED while both client pipelines were
# still running perfectly. Wait long enough to actually observe the outcome.
dbutils.widgets.text("wait_timeout_minutes", "240")
# ONE batch_id for the whole "start bronze" run, minted here and passed to EVERY client pipeline (and,
# inside each, to every ingestion method). This is the invariant: one run = one batch_id across all
# methods x clients, so Silver has a single gate value. A DAG may pin its own id via this parameter
# (default empty -> mint once here). Previously each method self-minted -> 3 methods x N clients = many
# batch_ids per run, and the non-max ones never reached bronze_batch_watermark (orphaned from Silver).
dbutils.widgets.text("batch_id", "")
# The promotion gate Silver reads: a batch_id present here is eligible for Silver, absent = orphaned in
# Bronze forever. This orchestrator OWNS the run identity, so it registers the one run batch_id here
# once every client pipeline has succeeded - guaranteeing the id is promotable no matter which methods
# (CDC / incremental / full-reload) actually produced rows. green = medallion2; blue = medallion.
dbutils.widgets.text("promo_gate_table", "medallion2.control.bronze_batch_watermark")
# Optional client-activation control. DEFAULT (both empty) = leave client_source_config exactly
# as-is and launch whatever is already active. Otherwise, BEFORE reading the active set, flip
# is_active for the named clients. Comma-separated lists, matched case-insensitively and tolerant
# of the "Elite_" prefix ("mississippi" == "Elite_Mississippi"). Unknown names fail loud (typo
# protection); a name in BOTH lists is rejected.
dbutils.widgets.text("enable_clients", "")    # CSV -> set is_active=1 before launch
dbutils.widgets.text("disable_clients", "")   # CSV -> set is_active=0 before launch

SCOPE  = dbutils.widgets.get("secret_scope")
SERVER = dbutils.widgets.get("server").strip()
DB     = dbutils.widgets.get("database").strip()
USER   = dbutils.widgets.get("sql_user").strip()
PREFIX = dbutils.widgets.get("job_prefix").strip()
DRY    = dbutils.widgets.get("dry_run").strip().lower() == "true"
WAIT_MIN = int(dbutils.widgets.get("wait_timeout_minutes"))
PROMO_GATE = dbutils.widgets.get("promo_gate_table").strip()
BATCH_ID = dbutils.widgets.get("batch_id").strip() or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
print(f"orchestration batch_id = {BATCH_ID}  (source: {'pinned parameter' if dbutils.widgets.get('batch_id').strip() else 'minted once here'})")

PWD = dbutils.secrets.get(SCOPE, "sql-admin")
URL = (f"jdbc:sqlserver://{SERVER}:1433;databaseName={DB};"
       "encrypt=true;trustServerCertificate=false;loginTimeout=30")

# ---------- 1. active clients, from the control table ----------
ENABLE  = [c.strip() for c in dbutils.widgets.get("enable_clients").split(",") if c.strip()]
DISABLE = [c.strip() for c in dbutils.widgets.get("disable_clients").split(",") if c.strip()]

def _read_csc():
    return (spark.read.format("jdbc").option("url", URL).option("dbtable", "dbo.client_source_config")
            .option("user", USER).option("password", PWD)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())

csc = _read_csc()
cols = {c.lower(): c for c in csc.columns}
print("client_source_config columns:", csc.columns)

name_col = next((cols[c] for c in ("client_name", "clientname", "name", "client") if c in cols), None)
act_col  = next((cols[c] for c in ("is_active", "isactive", "active") if c in cols), None)
if not name_col or not act_col:
    raise Exception(f"cannot locate name/is_active columns in client_source_config: {csc.columns}")

rows = csc.collect()

# ---------- 1a. optional pre-launch activation (default: no write, config used as-is) ----------
if ENABLE or DISABLE:
    _norm = lambda s: re.sub(r"^elite[_\s]*", "", str(s).strip().lower())
    both = {_norm(x) for x in ENABLE} & {_norm(x) for x in DISABLE}
    if both:
        raise Exception(f"client(s) in BOTH enable and disable lists: {sorted(both)}")
    stored = {r.asDict()[name_col]: _norm(r.asDict()[name_col]) for r in rows}   # full name -> normalized
    def _resolve(tokens):
        want = {_norm(t) for t in tokens}
        hit  = [full for full, n in stored.items() if n in want]
        return hit, want - {stored[f] for f in hit}                              # (matched full names, unknown tokens)
    to_enable,  miss_e = _resolve(ENABLE)
    to_disable, miss_d = _resolve(DISABLE)
    if miss_e or miss_d:
        raise Exception(f"unknown client name(s) not in client_source_config - enable-miss={sorted(miss_e)} "
                        f"disable-miss={sorted(miss_d)}; known={sorted(stored.values())}")
    conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(URL, USER, PWD)   # SINGLE_USER cluster
    try:
        stmt = conn.createStatement()
        for full in to_enable:
            stmt.executeUpdate(f"UPDATE dbo.client_source_config SET [{act_col}]=1 "
                               f"WHERE [{name_col}]=N'{full.replace(chr(39), chr(39)*2)}'")
        for full in to_disable:
            stmt.executeUpdate(f"UPDATE dbo.client_source_config SET [{act_col}]=0 "
                               f"WHERE [{name_col}]=N'{full.replace(chr(39), chr(39)*2)}'")
        stmt.close()
    finally:
        conn.close()
    print(f"pre-launch activation applied -> enabled={to_enable}  disabled={to_disable}")
    rows = _read_csc().collect()                                                 # re-read so launch reflects the change
active = []
for r in rows:
    d = r.asDict()
    if str(d[act_col]).strip().lower() in ("1", "true"):
        active.append(str(d[name_col]).strip())
print(f"\nACTIVE CLIENTS ({len(active)}): {active}")
print(f"inactive       : {[str(r.asDict()[name_col]).strip() for r in rows if str(r.asDict()[act_col]).strip().lower() not in ('1','true')]}")

# ---------- 2. match each active client to its bronze job ----------
w = WorkspaceClient()
jobs = {}
for j in w.jobs.list(expand_tasks=False):
    nm = j.settings.name or ""
    if nm.startswith(PREFIX):
        jobs[nm[len(PREFIX):].strip().lower()] = (j.job_id, nm)
print(f"\nbronze jobs matching '{PREFIX}*': { {k: v[0] for k, v in jobs.items()} }")

# The control table names clients "Elite_Mississippi" / "Elite_VBEMS"; the Databricks jobs
# use the bare client name ("mississippi", "vbems"). Normalise both sides rather than
# hard-coding a lookup, so a new client onboarded as "Elite_Foo" finds Bronze-...-foo.
def norm(x):
    x = str(x).strip().lower()
    x = re.sub(r"^elite[_\-\s]*", "", x)
    return re.sub(r"[^a-z0-9]", "", x)

jobs_norm = {norm(k): v for k, v in jobs.items()}
plan, missing = [], []
for c in active:
    hit = jobs.get(c.lower()) or jobs_norm.get(norm(c))
    (plan.append((c, hit[0], hit[1])) if hit else missing.append(c))

print("\nPLAN:")
for c, jid, nm in plan:
    print(f"   {c:<18} -> {nm} ({jid})")
if missing:
    print(f"   NO BRONZE JOB for active client(s): {missing}")

if not plan:
    raise Exception(f"No bronze job found for any active client {active}. Nothing would ingest.")

if DRY:
    print("\nDRY RUN - not triggering. Set dry_run=false to execute.")
    dbutils.notebook.exit(json.dumps(
        {"dry_run": True, "active": active, "planned": [p[0] for p in plan], "missing": missing}))

# ---------- 3. run them, wait, aggregate ----------
# The trigger (run_now) and the wait (.result()) are SEPARATE phases with different failure
# semantics, so they must be handled separately:
#   * TRIGGER failure  -> the run was never created (run_now raised). Retriable, and idempotent:
#     a raise means nothing was launched, so re-calling cannot duplicate a run. This is the bug
#     this function fixes -- previously a single fire-time hiccup (concurrent run_now on the
#     shared client racing / a transient API error) silently dropped that client for the whole
#     orchestration, so only one of N clients would launch.
#   * EXECUTION failure -> the run WAS created and then failed (.result() raises OperationFailed).
#     A genuine failure, NOT a trigger problem -> reported, never retried (retry would re-run it).
TRIGGER_ATTEMPTS = 4

def _trigger(tw, jid, nm):
    """run_now with retry. Returns the Wait[Run] waiter on success; raises after all attempts."""
    last = None
    for attempt in range(1, TRIGGER_ATTEMPTS + 1):
        try:
            return tw.jobs.run_now(job_id=jid, job_parameters={"batch_id": BATCH_ID})
        except Exception as e:
            last = e
            print(f"   run_now attempt {attempt}/{TRIGGER_ATTEMPTS} failed for {nm} ({jid}): {str(e)[:140]}")
            time.sleep(3 * attempt)   # linear backoff
    raise last

def run(item):
    client, jid, nm = item
    print(f"   -> triggering {nm} ({jid})")
    # Own WorkspaceClient per thread: the module-level `w` is shared across the ThreadPool and
    # concurrent run_now on one client is what raced and dropped clients. A fresh client is cheap.
    tw = WorkspaceClient()
    try:
        waiter = _trigger(tw, jid, nm)                       # create the run (retried)
    except Exception as e:
        return {"client": client, "job": nm, "job_id": jid,
                "error": f"could not launch after {TRIGGER_ATTEMPTS} attempts: {str(e)[:280]}", "ok": False}
    try:
        # .result() BLOCKS until the run is terminal and RAISES (OperationFailed) if it did not
        # succeed. A clean return IS success -- do NOT inspect the Run object's state afterward
        # (this SDK's Run has .state, not .status; reading .status raised AttributeError and
        # falsely reported SUCCEEDED children as FAILED). Trust the raise/return contract.
        res = waiter.result(timeout=timedelta(minutes=WAIT_MIN))
        return {"client": client, "job": nm, "job_id": jid,
                "run_id": getattr(res, "run_id", None), "state": "SUCCESS", "ok": True}
    except Exception as e:
        return {"client": client, "job": nm, "job_id": jid, "error": str(e)[:400], "ok": False}

print(f"\nrunning {len(plan)} client pipeline(s)...")
with ThreadPoolExecutor(max_workers=max(1, len(plan))) as ex:
    results = list(ex.map(run, plan))

print("\n" + "=" * 80)
for r in results:
    print(f"   {r['client']:<18} {'OK' if r['ok'] else 'FAILED'}  {r.get('state') or r.get('error','')[:120]}")
print("=" * 80)

failed = [r for r in results if not r["ok"]]
summary = {"active": active, "results": results, "missing_jobs": missing,
           "failed": [r["client"] for r in failed]}

if missing:
    raise Exception(f"Active client(s) with NO bronze job - not ingested: {missing}. {json.dumps(summary)[:1500]}")
if failed:
    raise Exception(f"Bronze failed for {[r['client'] for r in failed]}. {json.dumps(summary)[:1500]}")

print("ALL ACTIVE CLIENTS INGESTED SUCCESSFULLY")

# ---------- 4. register the ONE run batch_id in the promotion gate ----------
# Every client succeeded, so all Bronze rows stamped with BATCH_ID are settled. Register that id here
# so Silver will promote them. Idempotent MERGE on batch_id: re-registering an id already written by a
# client's stage_compact is a no-op. This is the authoritative write - it fires even for runs whose
# only data came from non-CDC methods (which never touch the gate themselves).
try:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    bw = (spark.range(1)
             .select(F.lit(int(BATCH_ID)).cast("bigint").alias("batch_id"),
                     F.current_timestamp().alias("completed_ts"),
                     F.lit(f"allclients-{BATCH_ID}").alias("execution_id")))
    (DeltaTable.forName(spark, PROMO_GATE).alias("t")
        .merge(bw.alias("s"), "t.batch_id = s.batch_id")
        .whenNotMatchedInsert(values={"batch_id": "s.batch_id", "completed_ts": "s.completed_ts",
                                      "execution_id": "s.execution_id"}).execute())
    print(f"registered batch_id {BATCH_ID} in promotion gate {PROMO_GATE}")
    summary["promo_gate_registered"] = {"table": PROMO_GATE, "batch_id": BATCH_ID}
except Exception as e:
    # Do NOT fail the run for this: the per-client stage_compact also registers the same id, so the
    # gate is still populated. Surface it loudly so the gap is visible if BOTH paths ever miss.
    print(f"WARNING: orchestrator gate registration failed for batch_id {BATCH_ID}: {e}")
    summary["promo_gate_error"] = str(e)[:400]

dbutils.notebook.exit(json.dumps(summary, default=str)[:60000])
