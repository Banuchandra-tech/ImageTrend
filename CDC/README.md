# CDC stream → Bronze (Mode 1) — single client

The CDC path streams row changes from the source into per-table Bronze in near-real-time. The two heavy notebooks
already existed in the parent folder; what was missing — and what made the path inert — is the **driver that
catches and registers a client's CDC tables**. This folder adds that driver, an orchestrator that runs the path
end to end for one client, and proofs. Nothing in the parent folder is modified.

```
Source (Elite_<client>, EmsEvent)
  │  CDC-enabled tables
  ▼
Debezium ──► Event Hub  debezium-cdc
                 │
                 ▼
   EventHub_To_Bronze_Landing  ──► medallion.bronze.incident_cdc      (registry-filtered landing)
                 │
                 ▼
   Dynamic_CDC_Bronze_FanOut_FINAL ──► medallion.bronze.<table>       (MERGE by PK; insert/update/delete)
```

## The gap this closes

Both existing notebooks are **driven by `medallion.control.bronze_table_registry`**:

- the landing notebook **inner-joins** every Debezium event against the registry — unregistered tables are dropped;
- the fan-out **skips any table whose PK is not in the registry**.

Nothing in the repo populated that registry, so for a fresh client the CDC path landed **nothing**. This is the
*"catch the tables that have CDC"* mechanism from the design discussion: discover which tables have CDC for the
client and feed them in automatically, never a hardcoded list.

## Files

| File | What it is |
|---|---|
| `cdc_register_tables.py` | **The driver.** For one `Elite_<client>` DB, discovers `EmsEvent` tables + CDC flag (`is_tracked_by_cdc`) + primary keys from the source catalog, and MERGEs them into `bronze_table_registry`. Re-run when CDC is enabled on more tables. |
| `run_cdc_single_client.py` | Orchestrator: runs **register → land → fan-out** in order with consistent parameters, via `dbutils.notebook.run`. |
| `test_cdc_register.py` | DEV proof for the driver (sandbox source catalog → registry); verifies CDC flagging, ordered composite-PK CSV, no-PK skip, and the `CDC_ONLY` filter. No live SQL MI needed. |

## How to run it for a client

1. **Register** — run `cdc_register_tables` with `source_database=Elite_<client>`, `jdbc_host`, `jdbc_user`
   (password key defaults to `debezium-db-password` in scope `kv-imgtrend-dev-eus`). Use `register_mode=CDC_ONLY`
   for the CDC path, or `ALL` to also register non-CDC tables' PKs (so `incremental_non_cdc_load` can MERGE them).
   Start with `dry_run=true` to see what it would register.
2. **Land + fan out** — run `run_cdc_single_client` (it calls register, then the landing, then the fan-out). Point
   its `landing_notebook_path` / `fanout_notebook_path` widgets at where those notebooks live in your workspace.
3. **Prove the driver first** — run `test_cdc_register` for a one-click PASS/FAIL with no live source.

> If the live capture side isn't delivering yet (Event Hub shows no incoming messages, or Debezium is down), the
> register step + fan-out logic still work: register the client's tables, and validate the fan-out against rows
> already in `medallion.bronze.incident_cdc` (or a small seeded set of Debezium-shaped rows). The goal — rows reach
> per-table Bronze for one client — is still reachable via full load + incremental while capture is being fixed.

## `cdc_register_tables` parameters

| Widget | Default | Meaning |
|---|---|---|
| `source_mode` | `JDBC` | `JDBC` reads the SQL MI catalog; `DELTA` reads provided catalog tables (testing). |
| `source_database` | — | `Elite_<client>` DB. |
| `source_schema` | `EmsEvent` | Schema to scan. |
| `register_mode` | `ALL` | `ALL` / `CDC_ONLY` / `NON_CDC_ONLY`. |
| `registry_table` | `medallion.control.bronze_table_registry` | Target registry. |
| `bronze_lowercase` | `false` | Lowercase the derived `bronze_table` name. |
| `dry_run` | `false` | Show what would be registered; write nothing. |
| `secret_scope` / `jdbc_host` / `jdbc_port` / `jdbc_user` / `secret_key_password` | `kv-imgtrend-dev-eus` / — / `1433` / — / `debezium-db-password` | Source connection; password only from the scope. |
| `delta_tables_catalog` / `delta_pk_catalog` | — | DELTA-mode inputs (shaped like the `sys.tables` / `sys.indexes` queries). |

It registers only the columns the registry actually has (introspected), so it never breaks the existing landing /
fan-out readers. Tables with no primary key are skipped and listed (they can't be MERGEd by PK).

## How the three modes coordinate

- **One registry drives all three.** Register once per client; the landing/fan-out (CDC) and
  `incremental_non_cdc_load` (Mode 3) all read PKs + routing from `bronze_table_registry`. CDC-enabled tables are
  owned by the fan-out; non-CDC tables by the incremental loader (which refuses any CDC table).
- **Full load → incremental/CDC handoff.** The full load does the initial copy and records where it stopped; the
  incremental/CDC modes resume from there so nothing is re-pulled or missed. See
  `../incremental-non-cdc/README.md` for the watermark detail and its proof.
- **Same Bronze tables.** All three MERGE on `[tenant_id] + pk` into `medallion.bronze.<bronze_table>`, so their
  output is union-compatible.

## Notes for production (from the design discussion — not all in scope for the single-client dev build)

- **Schema drift:** the fan-out can add new STRING columns; schema changes should notify the product team.
- **Data-loss rule:** land everything even if the schema doesn't match — never drop or truncate fields.
- **Truncation detection:** if a large fraction of a table changes after a quiet gap, treat it as a truncate/
  reload rather than per-row change capture (deferred optimization).
- **CDC retention** is ~15 days, so a client's full load must complete before its CDC data ages out.
- **SLA:** source → reporting within ~30 min (up to ~90 min informal ceiling).
