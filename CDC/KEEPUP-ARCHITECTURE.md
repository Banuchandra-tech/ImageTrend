# CDC keep-up: the per-table-MERGE fan-out and how to scale it past wide loads

## The failure mode (2026-06-30)
`eventhub_to_bronze_stream.py` processes each micro-batch by routing rows to their
`__source_table` and doing **one Delta/Iceberg MERGE per distinct table** in the batch
(`process_one_table`, an 8-→16-wide `ThreadPoolExecutor`). Per-batch cost is therefore
**O(distinct tables)**.

On 2026-06-30 a schema-wide incident load put **~460 tables in a single 50k-offset
micro-batch** → ~460 MERGEs → the batch could not commit within the 60-min task timeout
→ the checkpoint never advanced → the stream was **stuck ~17h** (every run timing out,
backlog growing). See `project_cdc_incident_2026-06-30` in memory.

## What's already hardened (commit 2e6f30c) — prevents the *stuck* failure
- `max_offsets_per_trigger` default **5000** (was 50000): fewer tables/MERGEs per batch,
  so every micro-batch **commits**. A wide load now degrades to **lagging** (progress
  survives every batch and every timeout) instead of **stuck forever**.
- `merge_max_workers` (default 16): more concurrent MERGEs → faster wide-batch drain.
- `WIDE_BATCH WARNING` log when a batch exceeds `wide_batch_warn` (200) tables → early
  signal of a schema-wide load.

This makes the pipeline **safe** (no death-spiral). It does **not** make it *keep up* with
a sustained wide load — the fan-out is still O(tables) per batch.

## The durable fix (to actually keep up with wide loads): stage-append + periodic MERGE
Decouple ingest throughput (cheap, append-bound) from MERGE cost (amortized, scalable):

1. **Ingest (foreachBatch) → ONE append, no per-table MERGE.** Parse the micro-batch and
   append all rows to a single staging Delta table, e.g. `medallion.bronze._cdc_stage`
   (`source_table, op, pk_json, after_json, commit_lsn, tenant_id, batch_id, ingest_ts`).
   One append per micro-batch is O(1) in table-width → commits fast regardless of how many
   tables the batch spans. This removes the O(tables) cost from the hot path.
2. **Compact (separate scheduled job) → bulk MERGE.** Periodically (e.g. every few min, or
   triggered), for each table with staged rows: dedupe to the latest row per `[tenant]+pk`
   by `commit_lsn`, MERGE into `medallion.bronze.<table>`, then delete the consumed staged
   rows. MERGE work is now batched + can be parallelized / scaled on its own cluster, off
   the ingestion path.
3. **Silver signal moves to the compaction step** — write `batch_watermark` /
   `bronze_pipeline_execution` after the compaction MERGE (not in `foreachBatch`).

**Trade-offs:** adds Silver latency (data is in Bronze only after compaction); the staging
table needs retention/vacuum; dedup + ordering logic lives in the compaction job.

## Cheaper interim levers (no re-architecture)
- **Shard the stream by Debezium batch group.** The connector fleet already splits DBs into
  batch groups, each → its own Event Hub. Running one stream per hub reduces tables-per-batch
  per stream (narrower fan-out each).
- **Bigger / Photon cluster + higher `merge_max_workers`** — more concurrent MERGEs (linear
  headroom, not a structural fix).
- **`max_offsets_per_trigger` even lower** during a known bulk load (more, smaller commits).

## Implementation (2026-06-30) — built

Two notebooks (the old `eventhub_to_bronze_stream.py` stays as the documented **rollback path only** — never dual-run; same EH checkpoint + consumer group):

- **`cdc/stage_ingest.py`** — readStream EH → `foreachBatch` → **ONE Delta append** into `medallion.bronze._cdc_stage`. Cost is O(rows), not O(distinct tables). No parse / registry / MERGE. Sole checkpoint + consumer-group owner. Trigger `availableNow` (cron/on-demand, timeout-proof now) or `processingTime` (continuous).
- **`cdc/stage_compact.py`** — pin `boundary = max(stage_seq)`; per-table (parallel, distinct Delta targets) **single-pass latest-op-wins** MERGE into `bronze.<table>` (dedup by `commit_lsn desc, stage_seq desc`; deletes branch on `_op`; drift-safe explicit colmap); then **one batched** stage-delete (predicate ⊆ the read predicate) + cursor + `batch_watermark` + `bronze_pipeline_execution(_detail)`/`bronze_batch_watermark`. Its own cluster, on-demand / depth-triggered.
- Staging = Delta, `CLUSTER BY (tenant_id, source_schema, source_table)`, deletion vectors, `stage_seq` IDENTITY; companion `control.cdc_stage_compaction_cursor`.

**Exactly-once:** at-least-once append collapses on `commit_lsn` dedup; bounded claim (delete predicate is a strict subset of the read) = no loss; single MERGE = no delete/upsert reorder; MERGE-then-DELETE = idempotent replay; per-table isolation + a promotion gate that fires only when 0 tables FAILED.

**Why the 2026-06-30 stall is structurally impossible:** the checkpoint advances on the **append** (O(1) in width, commits in seconds); the O(tables) MERGE moved to the compactor, which has **no EH offset to advance** — a slow run only makes Bronze one cycle stale, never freezes the checkpoint.

**Jobs:** `Bronze-CDC-Ingest` (append, checkpoint owner) + `Bronze-CDC-Compact` (MERGE, own cluster). Both `max_concurrent_runs=1`. The orchestrator's cdc node stays a `run_job_task` → the single owner (single-writer rule preserved).

**Cutover:** stop old stream → let it drain → start `stage_ingest` from latest → start `stage_compact`. **Rollback:** repoint the orchestrator `run_job_task` back to the old job, stop the pair, resume the old stream. Do a bounded proof (capture a small real slice into the stage, run compact, verify bronze + `batch_watermark`) before flipping production.

## Deployed 2026-07-01 — proven + cut over

**Job:** `Bronze-CDC-Ingest-Compact-Dev` (id `119094520906808`), GIT source `bronze-notebooks@main`,
`max_concurrent_runs=1`. Two tasks: `ingest` (`cdc/stage_ingest`, single-node `ingest_jc`,
`trigger=availableNow`, checkpoint `.../_checkpoints/cdc_stage_ingest`, `consumer_group=databricks-cg`)
→ `compact` (`cdc/stage_compact`, autoscale-1-4 `compact_jc`, `merge_max_workers=16`). Job-level
parameter `batch_id` auto-binds to the ingest widget.

**Loss-free handoff:** the old stream `eventhub_to_bronze_stream` had `committed == latest_planned`
(fully drained) at `{debezium-cdc: {0:564695, 1:689784, 2:557619, 3:572793}}`. `stage_ingest`'s
`starting_offsets` was pinned to exactly that on the fresh checkpoint — no gap, no overlap. (Used only
on first run; the checkpoint governs thereafter.)

**Proof 1 — bounded wide slice (compactor logic):** captured a real 281-table / 6,875-row EH slice into
`_cdc_stage_proof`, ran `stage_compact` → **277 ok / 0 failed / 4 skipped** (skipped = unregistered,
correctly left in stage); `control.batch_watermark` advanced for all 277; stage drained to exactly the
4 skipped tables' rows (boundary claim = strict subset). This is the 2026-06-30 wide-load shape that
stalled the old stream for 17h — now compacts cleanly.

**Proof 2 — the wired job end-to-end (real clusters + GIT + handoff):** ran the job with `batch_id=20260701024439`.
`ingest` drained the real committed→latest backlog (977 rows / 42 tables) → `compact` **42 ok / 0 failed**;
real `_cdc_stage` drained to **0**; `batch_watermark` advanced for all 42; `run_batch` == passed batch_id
(param binding confirmed).

**DAG repoint (the cutover):** `Bronze-Ingestion-Orchestrator` (id `985777479078184`) node `cdc_stream`
`run_job_task.job_id` `349011798571237` (old stream) → `119094520906808` (new). `depends_on` and
`batch_id={{tasks.init.values.batch_id}}` unchanged. Because the orchestrator has **no schedule**
(on-demand, feedback #2), this takes effect on the next manual DAG trigger — nothing auto-runs.

**Rollback (intact):** old `Bronze-CDC-Stream-Dev` (id `349011798571237`) is **PAUSED**, not deleted —
repoint the DAG node back to it and unpause to revert. Safe even after the new pipeline ran: the MERGE is
idempotent (`commit_lsn` dedup, latest-op-wins), so re-consuming from the old checkpoint re-applies without
dupes/loss.
