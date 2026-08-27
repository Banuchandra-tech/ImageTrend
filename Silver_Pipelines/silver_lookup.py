# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,CONFIGURATION & PARAMETERS
# =====================================================
# CELL 1 - CONFIGURATION & PARAMETERS
# =====================================================
#
# Purpose:
#   Initializes runtime configuration for the
#   Dependency-Based Silver Pipeline.
#
# Responsibilities:
#   - Import required libraries
#   - Load notebook parameters
#   - Configure Spark settings
#   - Define scheduler configuration
#   - Define audit configuration
#   - Generate execution identifier
#
# Notes:
#   No metadata loading or table execution occurs
#   in this cell.
#
# =====================================================

"""
Dependency-Based Silver Pipeline

Purpose:
    Execute Silver transformations using dependency-based
    orchestration instead of execution-order scheduling.

Key Features:
    - Parent/Child dependency management
    - Dynamic worker allocation
    - Replay mode support
    - Audit logging
    - Failure propagation
    - Watermark tracking

Execution Flow:

    Metadata Load
          |
          v
    Dependency Validation
          |
          v
    Ready Queue Creation
          |
          v
    Dependency Scheduler
          |
          v
    Audit Logging
          |
          v
    Watermark Update
          |
          v
    Pipeline Summary

Version:
    1.0
"""
# =====================================================
# IMPORTS
# =====================================================

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
from pyspark.sql.types import *
from pyspark.sql import functions as F
from collections import defaultdict
from collections import deque
from collections import defaultdict

from pyspark.sql import Row


from datetime import datetime

import uuid
import time
import traceback
import os
import sqlparse

# =====================================================
# NOTEBOOK PARAMETERS
# =====================================================

dbutils.widgets.text("run_mode", "NORMAL")
dbutils.widgets.text("replay_batch_id", "")

run_mode = dbutils.widgets.get("run_mode").strip().upper()
replay_batch_id = dbutils.widgets.get("replay_batch_id").strip()

assert run_mode in ("NORMAL", "REPLAY")
# =====================================================
# SPARK CONFIGURATION
# =====================================================

dbutils.widgets.text("catalog", "medallion")   # green passes medallion2
CATALOG = dbutils.widgets.get("catalog").strip()
# Subset registry for fast smoke runs; default is the real one.
dbutils.widgets.text("registry_table", "control.silver_table_registry")
REGISTRY_TABLE = dbutils.widgets.get("registry_table").strip() or "control.silver_table_registry"
spark.sql(f"USE CATALOG {CATALOG}")

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

spark.conf.set("spark.databricks.delta.retryWriteConflict.enabled", "true")
# =====================================================
# TRANSFORMATION CONFIGURATION
# =====================================================

# Path to the checked-out transformation SQL. Default = blue's Repos checkout so blue is
# unchanged; green passes its own (/Repos/jairo.narvaez@algoworks.com/...).
dbutils.widgets.text("base_sql_path",
    "/Workspace/Repos/vysali.devabhaktuni@algoworks.com/silver-notebooks/transformations-optimized")
BASE_SQL_PATH = dbutils.widgets.get("base_sql_path").strip()
# =====================================================
# SCHEDULER CONFIGURATION
# =====================================================

# Initial worker count used by scheduler.
BASE_WORKERS = 3

# Minimum worker allocation.
MIN_WORKERS = 2

# Maximum worker allocation.
MAX_WORKERS_LIMIT = 8

# Retry attempts per table execution.
MAX_RETRIES = 1
WORKER_EVAL_INTERVAL = 10
# Wait period between retries (seconds).
RETRY_WAIT = 5

# Progress logging interval.
PROGRESS_INTERVAL = 10

current_workers = BASE_WORKERS
# =====================================================
# AUDIT CONFIGURATION
# =====================================================

PIPELINE_EXECUTION_TABLE = "control.silver_pipeline_execution"

PIPELINE_EXECUTION_DETAIL_TABLE = "control.silver_pipeline_execution_detail"
dbutils.widgets.text("dependency_level", "2")

DEPENDENCY_LEVEL = int(
    dbutils.widgets.get("dependency_level")
)
# =====================================================
# EXECUTION CONTEXT
# =====================================================

PIPELINE_VERSION = "1.0"

EXECUTION_ID = datetime.now().strftime("%Y%m%d%H%M%S%f")

PIPELINE_START_TS = datetime.now()
print("=" * 80)
print("DEPENDENCY PIPELINE STARTED")
print("=" * 80)

print(f"Version      : {PIPELINE_VERSION}")
print(f"Execution Id : {EXECUTION_ID}")
print(f"Start Time   : {PIPELINE_START_TS}")
print(f"Run Mode     : {run_mode}")
print(f"Base Workers : {BASE_WORKERS}")
print(f"Max Workers  : {MAX_WORKERS_LIMIT}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,METADATA LOADING & DEPENDENCY VALIDATION
# =====================================================
# CELL 2 - METADATA LOADING & DEPENDENCY VALIDATION
# =====================================================
#
# Purpose:
#   Loads pipeline metadata required for dependency-
#   based scheduling.
#
# Responsibilities:
#   - Load active Silver transformations
#   - Load dependency relationships
#   - Build parent/child maps
#   - Validate dependency integrity
#   - Detect cyclic dependencies
#   - Determine pending batch ids to process
#
# Output:
#   all_tables
#   parents_map
#   children_map
#   batch_map
#
# Notes:
#   No transformation execution occurs in this cell.
#
# =====================================================

# =====================================================
# LOAD ACTIVE TABLES
# =====================================================


# =====================================================
# LOAD ACTIVE TABLES
# =====================================================


def load_active_tables():
    """
    Loads active Silver transformations.

    Returns
    -------
    all_tables      : set
    registry_tables : set
    sql_files       : set
    """

    sql_files = {
        f.replace(".sql", "")
        for f in os.listdir(BASE_SQL_PATH)
        if f.lower().endswith(".sql")
    }

    registry_rows = spark.sql(f"""
        SELECT DISTINCT silver_table,
        dependency_level,
        is_active
        FROM {REGISTRY_TABLE}
    """).collect()

    registry_lookup = {}

    registry_tables = set()

    for row in registry_rows:
        registry_lookup[row["silver_table"]] = {
            "dependency_level": row["dependency_level"],
            "is_active": row["is_active"],
        }

        if row["is_active"] and row["dependency_level"] == DEPENDENCY_LEVEL:
            registry_tables.add(row["silver_table"])

    all_tables = registry_tables.intersection(sql_files)

    print("=" * 80)
    print("ACTIVE TABLE DISCOVERY")
    print("=" * 80)
    print(f"SQL Files           : {len(sql_files)}")
    print(f"Registry Tables     : {len(registry_tables)}")
    print(f"Executable Tables   : {len(all_tables)}")
    print("=" * 80)

    return (all_tables, registry_tables, registry_lookup, sql_files)


# =====================================================
# LOAD DEPENDENCY METADATA
# =====================================================


def load_dependencies():
    """
    Loads active dependency relationships.
    """

    return spark.sql(f"""
        SELECT distinct
            parent_table,
            child_table
        FROM control.silver_table_dependencies
        WHERE is_active = true
          AND dependency_level = {DEPENDENCY_LEVEL}
    """).collect()


# =====================================================
# BUILD DEPENDENCY GRAPH
# =====================================================


def build_dependency_graph(dependencies):
    """
    Builds parent-child relationship maps.
    """

    parents_map = defaultdict(set)

    children_map = defaultdict(set)

    for row in dependencies:
        parent = row["parent_table"]

        child = row["child_table"]

        parents_map[child].add(parent)

        children_map[parent].add(child)

    return parents_map, children_map


# =====================================================
# DEPENDENCY VALIDATION
# =====================================================


def validate_dependency_graph(
    all_tables, parents_map, registry_tables, registry_lookup, sql_files, dependencies
):
    """
    Performs detailed dependency validation.

    Shows exactly WHY a table is missing.
    """

    dependency_tables = set()

    for row in dependencies:
        dependency_tables.add(row["parent_table"])
        dependency_tables.add(row["child_table"])

    validation_errors = []

    missing_tables = sorted(dependency_tables - all_tables)

    if not missing_tables:
        return

    print("=" * 100)
    print("DEPENDENCY VALIDATION FAILED")
    print("=" * 100)

    for table in missing_tables:
        registry_exists = table in registry_tables
        sql_exists = table in sql_files

        is_parent = table in children_map
        is_child = table in parents_map

        print()

        print(f"Table : {table}")
        print("-" * 100)

        print(f"Registry      : {'YES' if registry_exists else 'NO'}")

        print(f"SQL File      : {'YES' if sql_exists else 'NO'}")

        role = []

        if is_parent:
            role.append("Parent")

        if is_child:
            role.append("Child")

        print(f"Dependency    : {', '.join(role)}")

        print()

        if registry_exists and not sql_exists:
            print(f"Reason : SQL file missing\nExpected : {table}.sql")

        elif not registry_exists and sql_exists:
            # ---------------------------------------------------
            # Check whether table exists in registry
            # but under another dependency level
            # ---------------------------------------------------

            if table in registry_lookup:
                actual_level = registry_lookup[table]["dependency_level"]

                active = registry_lookup[table]["is_active"]

                if not active:
                    print("Reason : Registry entry exists but is inactive.")

                elif actual_level != DEPENDENCY_LEVEL:
                    print("=" * 80)

                    print("CROSS DEPENDENCY DETECTED")

                    print("=" * 80)

                    print(f"Parent Table : {table}")

                    print(f"Parent Level : {actual_level}")

                    children = sorted(children_map.get(table, []))

                    for child in children:
                        child_level = registry_lookup.get(child, {}).get(
                            "dependency_level", "UNKNOWN"
                        )

                        print()

                        print(f"Child Table  : {child}")

                        print(f"Child Level  : {child_level}")

                    print()

                    print("Reason:")

                    print("A dependency cannot exist across dependency levels.")

                    print()

                    print("Either")

                    print(" • Move parent and child to same dependency level")

                    print(" • Remove the dependency")

                    print("=" * 80)

                else:
                    print("Reason : Registry filtering excluded table.")

            else:
                print("Reason : Registry entry missing.")

        validation_errors.append(table)

    print()
    print("=" * 100)
    print(f"Total Missing Tables : {len(validation_errors)}")
    print("=" * 100)

    raise Exception(
        f"Dependency validation failed with {len(validation_errors)} issue(s)."
    )


# =====================================================
# CYCLE DETECTION
# =====================================================
def detect_cycles(all_tables, children_map):
    """
    Detects and reports ALL circular dependencies.

    Prints each cycle found and raises a
    single exception at the end.
    """

    visited = set()

    recursion_stack = []

    cycles = set()

    def dfs(node):
        visited.add(node)

        recursion_stack.append(node)

        for child in children_map[node]:
            if child not in visited:
                dfs(child)

            elif child in recursion_stack:
                cycle_start = recursion_stack.index(child)

                cycle = recursion_stack[cycle_start:] + [child]

                # Normalize cycle to avoid duplicates
                cycle_key = tuple(cycle)

                cycles.add(cycle_key)

        recursion_stack.pop()

    # ==========================================
    # RUN DFS
    # ==========================================

    for table in sorted(all_tables):
        if table not in visited:
            dfs(table)

    # ==========================================
    # REPORT CYCLES
    # ==========================================

    if cycles:
        print("=" * 80)
        print("CIRCULAR DEPENDENCIES DETECTED")
        print("=" * 80)

        for idx, cycle in enumerate(sorted(cycles), start=1):
            print()
            print(f"CYCLE #{idx}")
            print("-" * 40)

            for i in range(len(cycle) - 1):
                print(cycle[i])

                print("    ↓")

            print(cycle[-1])

        print()
        print("=" * 80)
        print(f"Total Cycles Found: {len(cycles)}")
        print("=" * 80)

        raise Exception(f"Found {len(cycles)} circular dependencies.")


# =====================================================
# LOAD BATCH METADATA
# =====================================================


def load_batch_metadata():
    """
    Determines batch metadata required
    for transformation execution.

    Returns:

        {
            table_name:
            [
                batch_id_1,
                batch_id_2,
                ...
            ]
        }

    Notes:

        NORMAL mode:
            Returns all pending batches.

        REPLAY mode:
            Returns replay batch for all
            active tables.
    """

    if run_mode == "REPLAY" and replay_batch_id:
        replay_batch = int(replay_batch_id)

        return {table: [replay_batch] for table in all_tables}

    batch_df = spark.sql(f"""
        SELECT distinct
            r.silver_table,
            b.batch_id
        FROM {REGISTRY_TABLE} r
        INNER JOIN control.bronze_batch_watermark b
            ON b.batch_id >
               COALESCE(
                   r.batch_id,
                   -1
               )
        WHERE r.is_active = true
          AND r.dependency_level = {DEPENDENCY_LEVEL}
    """)

    batch_map = defaultdict(list)

    for row in batch_df.collect():
        batch_map[row["silver_table"]].append(row["batch_id"])

    for table in batch_map:
        batch_map[table] = sorted(batch_map[table])

    return dict(batch_map)


# =====================================================
# LOAD METADATA
# =====================================================

(all_tables, registry_tables, registry_lookup, sql_files) = load_active_tables()

if not all_tables:
    raise Exception("No active transformations found.")

dependencies = load_dependencies()

parents_map, children_map = build_dependency_graph(dependencies)

validate_dependency_graph(
    all_tables, parents_map, registry_tables, registry_lookup, sql_files, dependencies
)

detect_cycles(all_tables, children_map)
print("=" * 80)
print("DEPENDENCY GRAPH SUMMARY")
print("=" * 80)

print(f"Parents : {len(children_map)}")
print(f"Children: {len(parents_map)}")

print("=" * 80)
batch_map = load_batch_metadata()


# =====================================================
# METADATA SUMMARY
# =====================================================

root_tables = sum(1 for table in all_tables if len(parents_map[table]) == 0)

leaf_tables = sum(1 for table in all_tables if len(children_map[table]) == 0)

tables_with_batches = sum(1 for batches in batch_map.values() if len(batches) > 0)

total_pending_batches = sum(len(batches) for batches in batch_map.values())

print("=" * 80)
print("METADATA SUMMARY")
print("=" * 80)

print(f"Active Tables      : {len(all_tables)}")

print(f"Dependencies       : {len(dependencies)}")

print(f"Root Tables        : {root_tables}")

print(f"Leaf Tables        : {leaf_tables}")

print(f"Run Mode           : {run_mode}")

print(f"Tables With Batches: {tables_with_batches}")

print(f"Pending Batches    : {total_pending_batches}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,EXECUTION ENGINE
# =====================================================
# CELL 3 - EXECUTION ENGINE
# =====================================================
#
# Purpose:
#   Provides reusable execution functions used by
#   the dependency scheduler.
#
# Responsibilities:
#   - Execute SQL statements
#   - Execute SQL files
#   - Execute table pipelines
#   - Propagate dependency failures
#
# Notes:
#   This cell contains no scheduling logic.
#
# =====================================================
# =====================================================
# SQL EXECUTION
# =====================================================


def execute_sql(stmt, table_name, file_name):
    """
    Executes a single SQL statement.

    Parameters:
        stmt:
            SQL statement to execute.

        table_name:
            Target Silver table.

        file_name:
            Source SQL file name.

    Returns:
        {
            "success": bool,
            "inserted": int,
            "updated": int,
            "deleted": int
        }
    """

    try:
        result = spark.sql(stmt)

        metrics = {"success": True, "inserted": 0, "updated": 0, "deleted": 0}

        try:
            if result is not None and stmt.lstrip().upper().startswith("MERGE"):
                row = result.first()

                if row:
                    metrics["inserted"] = getattr(row, "num_inserted_rows", 0) or 0

                    metrics["updated"] = getattr(row, "num_updated_rows", 0) or 0

                    metrics["deleted"] = getattr(row, "num_deleted_rows", 0) or 0

        except Exception:
            pass

        return metrics

    except Exception as ex:
        traceback.print_exc()
        return {
            "success": False,
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "error": str(ex)[:2000],
        }


# =====================================================
# SQL FILE EXECUTION
# =====================================================


def run_sql_file(file_path, table_name, pipeline_run_id):
    """
    Executes all SQL statements contained
    within a transformation file.

    Parameters:
        file_path:
            Full SQL file path.

        table_name:
            Target Silver table.

        pipeline_run_id:
            Batch identifier being processed.

    Returns:
        (
            failed,
            total_inserted,
            total_updated,
            total_deleted
        )

    Notes:
        Replaces:
            ${pipeline_run_id}

        before execution.
    """
    total_inserted = 0
    total_updated = 0
    total_deleted = 0

    error_message = None

    if not os.path.exists(file_path):
        print(f"⚠️ SQL FILE MISSING | {table_name}")

        return (
            True,
            0,
            0,
            0,
            f"SQL file not found: {file_path}",
        )

    with open(file_path, "r") as f:
        sql_code = f.read()

    print(f"📄 EXECUTING | {table_name} | Batch={pipeline_run_id}")

    sql_code = sql_code.replace(
        "${pipeline_run_id}",
        str(pipeline_run_id),
    )

    statements = [stmt.strip() for stmt in sqlparse.split(sql_code) if stmt.strip()]

    failed = False

    for stmt in statements:
        result = execute_sql(
            stmt,
            table_name,
            os.path.basename(file_path),
        )

        if not result["success"]:
            failed = True

            error_message = result.get(
                "error",
                "Unknown SQL execution error",
            )

            break

        total_inserted += result["inserted"]
        total_updated += result["updated"]
        total_deleted += result["deleted"]

    return (
        failed,
        total_inserted,
        total_updated,
        total_deleted,
        error_message,
    )


# =====================================================
# TABLE PIPELINE EXECUTION
# =====================================================
def run_table_pipeline(table_name):
    """
    Executes a single Silver transformation.

    Execution Flow:

        Determine pending batches
              |
              v
        Execute batches sequentially
              |
              v
        Collect batch audit rows
              |
              v
        Return table execution result

    Returns:

        (
            status,
            table_name,
            last_successful_batch,
            error_message,
            start_ts,
            end_ts,
            retry_count,
            total_inserted,
            total_updated,
            total_deleted,
            batch_audit_rows
        )

    Status Values:

        SUCCESS
            All batches completed successfully.

        PARTIAL_SUCCESS
            Some batches completed successfully,
            then a later batch failed.

        FAILED
            No batches completed successfully.
    """

    start_ts = datetime.now()

    batch_ids = batch_map.get(table_name, [])

    if not batch_ids:
        end_ts = datetime.now()

        return (
            "SKIPPED",
            table_name,
            None,
            "No batch available",
            start_ts,
            end_ts,
            0,
            0,
            0,
            0,
            [],
        )

    total_inserted = 0
    total_updated = 0
    total_deleted = 0

    successful_batches = []

    batch_audit_rows = []

    last_error = None

    for batch_id in batch_ids:
        batch_start_ts = datetime.now()

        try:
            print(f"📦 BATCH START | {table_name} | Batch={batch_id}")

            (
                failed,
                inserted_rows,
                updated_rows,
                deleted_rows,
                sql_error,
            ) = run_sql_file(
                os.path.join(
                    BASE_SQL_PATH,
                    f"{table_name}.sql",
                ),
                table_name,
                batch_id,
            )

            batch_end_ts = datetime.now()

            # ==========================================
            # BATCH FAILURE
            # ==========================================

            if failed:
                last_error = f"Batch={batch_id} | {sql_error}"

                batch_audit_rows.append(
                    Row(
                        execution_id=EXECUTION_ID,
                        table_name=table_name,
                        batch_id=batch_id,
                        start_ts=batch_start_ts,
                        end_ts=batch_end_ts,
                        duration_seconds=int(
                            (batch_end_ts - batch_start_ts).total_seconds()
                        ),
                        status="FAILED",
                        inserted_rows=0,
                        updated_rows=0,
                        deleted_rows=0,
                        error_message=last_error,
                        retry_count=0,
                        created_ts=datetime.now(),
                    )
                )

                print(f"❌ BATCH FAILED | {table_name} | Batch={batch_id}")

                break

            # ==========================================
            # BATCH SUCCESS
            # ==========================================

            successful_batches.append(batch_id)

            total_inserted += inserted_rows
            total_updated += updated_rows
            total_deleted += deleted_rows

            batch_audit_rows.append(
                Row(
                    execution_id=EXECUTION_ID,
                    table_name=table_name,
                    batch_id=batch_id,
                    start_ts=batch_start_ts,
                    end_ts=batch_end_ts,
                    duration_seconds=int(
                        (batch_end_ts - batch_start_ts).total_seconds()
                    ),
                    status="SUCCESS",
                    inserted_rows=inserted_rows,
                    updated_rows=updated_rows,
                    deleted_rows=deleted_rows,
                    error_message=None,
                    retry_count=0,
                    created_ts=datetime.now(),
                )
            )

            print(
                f"✅ BATCH COMPLETE | "
                f"{table_name} | "
                f"Batch={batch_id} | "
                f"Inserted={inserted_rows:,} | "
                f"Updated={updated_rows:,} | "
                f"Deleted={deleted_rows:,}"
            )

        except Exception as ex:
            batch_end_ts = datetime.now()

            last_error = str(ex)

            batch_audit_rows.append(
                Row(
                    execution_id=EXECUTION_ID,
                    table_name=table_name,
                    batch_id=batch_id,
                    start_ts=batch_start_ts,
                    end_ts=batch_end_ts,
                    duration_seconds=int(
                        (batch_end_ts - batch_start_ts).total_seconds()
                    ),
                    status="FAILED",
                    inserted_rows=0,
                    updated_rows=0,
                    deleted_rows=0,
                    error_message=last_error,
                    retry_count=0,
                    created_ts=datetime.now(),
                )
            )

            traceback.print_exc()

            break

    end_ts = datetime.now()

    # =====================================================
    # FINAL STATUS DETERMINATION
    # =====================================================

    if successful_batches and last_error is None:
        print(
            f"✅ TABLE COMPLETE | "
            f"{table_name} | "
            f"Successful Batches={len(successful_batches)} | "
            f"LastBatch={max(successful_batches)} | "
            f"Inserted={total_inserted:,} | "
            f"Updated={total_updated:,} | "
            f"Deleted={total_deleted:,}"
        )

        return (
            "SUCCESS",
            table_name,
            max(successful_batches),
            None,
            start_ts,
            end_ts,
            1,
            total_inserted,
            total_updated,
            total_deleted,
            batch_audit_rows,
        )

    elif successful_batches:
        print(
            f"⚠️ TABLE PARTIAL SUCCESS | "
            f"{table_name} | "
            f"Successful Batches={len(successful_batches)} | "
            f"LastBatch={max(successful_batches)} | "
            f"Error={last_error}"
        )

        return (
            "PARTIAL_SUCCESS",
            table_name,
            max(successful_batches),
            last_error,
            start_ts,
            end_ts,
            1,
            total_inserted,
            total_updated,
            total_deleted,
            batch_audit_rows,
        )

    print(f"❌ TABLE FAILED | {table_name} | Error={last_error}")

    return (
        "FAILED",
        table_name,
        None,
        last_error,
        start_ts,
        end_ts,
        1,
        0,
        0,
        0,
        batch_audit_rows,
    )


# =====================================================
# DEPENDENCY FAILURE PROPAGATION
# =====================================================
def block_descendants(table):
    """
    Recursively blocks downstream tables
    when a parent table fails.

    Example:

        A -> B -> C

        If A fails:

            B = BLOCKED
            C = BLOCKED

    Prevents execution of invalid
    dependency chains.

    Parameters:
        table:
            Failed parent table.
    """
    stack = [table]

    while stack:
        parent = stack.pop()

        for child in children_map[parent]:
            if table_state[child] in ("SUCCESS", "FAILED", "BLOCKED"):
                continue

            table_state[child] = "BLOCKED"

            audit_rows.append(
                Row(
                    execution_id=EXECUTION_ID,
                    table_name=child,
                    batch_id=None,
                    start_ts=datetime.now(),
                    end_ts=datetime.now(),
                    duration_seconds=0,
                    status="BLOCKED",
                    inserted_rows=0,
                    updated_rows=0,
                    deleted_rows=0,
                    error_message=f"Blocked by failed dependency {table}",
                    retry_count=0,
                    created_ts=datetime.now(),
                )
            )

            stack.append(child)

# COMMAND ----------

# DBTITLE 1,DEPENDENCY SCHEDULER
# =====================================================
# CELL 4 - DEPENDENCY SCHEDULER
# =====================================================
#
# Purpose:
#   Executes Silver transformations using
#   dependency-based orchestration.
#
# Responsibilities:
#   - Maintain table execution state
#   - Launch runnable transformations
#   - Monitor running transformations
#   - Unlock dependent tables
#   - Handle failure propagation
#
# Notes:
#   Uses dependency graph loaded in Cell 2.
#
# =====================================================

# =====================================================
# SCHEDULER STATE INITIALIZATION
# =====================================================

table_state = {table: "PENDING" for table in all_tables}

running = {}

audit_rows = []

success_updates = []

failed_tables = []

completed_count = 0

failure_count = 0


# =====================================================
# READY QUEUE INITIALIZATION
# =====================================================

ready_queue = deque(
    sorted(table for table in all_tables if len(parents_map[table]) == 0)
)

for table in ready_queue:
    table_state[table] = "READY"

print("=" * 80)
print("INITIAL READY TABLES")
print("=" * 80)

for table in ready_queue:
    print(f"🔓 ROOT READY | {table}")

print("=" * 80)


# =====================================================
# DEPENDENCY EXECUTION LOOP
# =====================================================

with ThreadPoolExecutor(max_workers=MAX_WORKERS_LIMIT) as executor:
    while True:
        # =====================================================
        # COMPLETION CHECK
        # =====================================================

        if not running and not ready_queue:
            break

        # =====================================================
        # LAUNCH READY TABLES
        # =====================================================

        available_slots = max(0, current_workers - len(running))

        for _ in range(available_slots):
            if not ready_queue:
                break

            table = ready_queue.popleft()

            if table_state[table] != "READY":
                continue

            table_state[table] = "RUNNING"

            print(
                f"🚀 STARTED | {table} | Workers={len(running) + 1}/{current_workers}"
            )

            running[table] = executor.submit(run_table_pipeline, table)

        # =====================================================
        # WAIT FOR COMPLETED TABLES
        # =====================================================

        if not running:
            continue

        done, _ = wait(running.values(), return_when=FIRST_COMPLETED)

        # =====================================================
        # PROCESS COMPLETED TABLES
        # =====================================================

        for tbl, future in list(running.items()):
            if future not in done:
                continue

            (
                status,
                table,
                batch_id,
                error,
                start_ts,
                end_ts,
                retry_count,
                inserted_rows,
                updated_rows,
                deleted_rows,
                batch_audit_rows,
            ) = future.result()

            table_state[table] = status

            print(
                f"✅ COMPLETED | "
                f"{table} | "
                f"Status={status} | "
                f"Batches={len(batch_audit_rows)} | "
                f"Inserted={inserted_rows:,} | "
                f"Updated={updated_rows:,} | "
                f"Deleted={deleted_rows:,}"
            )

            # =====================================================
            # BATCH LEVEL AUDIT
            # =====================================================

            audit_rows.extend(batch_audit_rows)

            # =====================================================
            # SUCCESS PROCESSING
            # =====================================================

            if status in ("SUCCESS", "PARTIAL_SUCCESS"):
                success_updates.append((table, batch_id))
                # ==========================================
                # Track Partial Success
                # ==========================================

                if status == "PARTIAL_SUCCESS":
                    failed_tables.append((table, error))
                    failure_count += 1

                for child in children_map[table]:
                    if table_state[child] != "PENDING":
                        continue

                    if all(
                        table_state[parent] in ("SUCCESS", "PARTIAL_SUCCESS")
                        for parent in parents_map[child]
                    ):
                        table_state[child] = "READY"

                        ready_queue.appendleft(child)

                        print(f"🔓 READY | {child}")

            # =====================================================
            # FAILURE PROCESSING
            # =====================================================

            elif status == "FAILED":
                failed_tables.append((table, error))

                failure_count += 1

                block_descendants(table)

            # =====================================================
            # SKIPPED PROCESSING
            # =====================================================

            elif status == "SKIPPED":
                for child in children_map[table]:
                    if table_state[child] != "PENDING":
                        continue

                    if all(
                        table_state[parent] in ("SUCCESS", "SKIPPED")
                        for parent in parents_map[child]
                    ):
                        table_state[child] = "READY"

                        ready_queue.appendleft(child)

                        print(f"🔓 READY | {child}")

            completed_count += 1

            # =====================================================
            # PROGRESS LOGGING
            # =====================================================

            if (completed_count % PROGRESS_INTERVAL) == 0:
                print(
                    f"📊 Progress: "
                    f"{completed_count}/{len(all_tables)} | "
                    f"Running={len(running) - 1} | "
                    f"Ready={len(ready_queue)} | "
                    f"Workers={current_workers}"
                )

            # =====================================================
            # DYNAMIC WORKER MANAGEMENT
            # =====================================================

            if (completed_count % WORKER_EVAL_INTERVAL) == 0:
                failure_ratio = (
                    failure_count / completed_count if completed_count else 0
                )

                if failure_ratio > 0.30:
                    current_workers = max(MIN_WORKERS, current_workers - 1)

                    print(f"⚠️ Workers Reduced -> {current_workers}")

                elif failure_ratio == 0:
                    current_workers = min(MAX_WORKERS_LIMIT, current_workers + 1)

                    print(f"🚀 Workers Increased -> {current_workers}")

            del running[tbl]

print("=" * 80)
print("DEPENDENCY SCHEDULER COMPLETE")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Watermark Updates & Pipeline Summary
# =====================================================
# CELL 5 - WATERMARK UPDATES & PIPELINE SUMMARY
# =====================================================
#
# Purpose:
#   Finalizes pipeline execution results.
#
# Responsibilities:
#   - Update successful table watermarks
#   - Calculate pipeline metrics
#   - Determine pipeline status
#   - Build pipeline summary record
#
# Notes:
#   Audit persistence occurs in Cell 6.
#
# =====================================================

# =====================================================
# WATERMARK UPDATE
# =====================================================


def update_watermarks(success_updates):
    """
    Updates registry batch_id values
    for successfully completed tables.

    Parameters:
        success_updates:
            List of
            (
                table_name,
                batch_id
            )
    """

    if not success_updates:
        print("⚠️ No successful tables found. Watermark update skipped.")
        return

    df_updates = spark.createDataFrame(
        [Row(silver_table=t[0], batch_id=t[1]) for t in success_updates]
    )

    df_updates.createOrReplaceTempView("watermark_updates")

    spark.sql(f"""
        MERGE INTO {REGISTRY_TABLE} t
        USING watermark_updates s
        ON t.silver_table = s.silver_table

        WHEN MATCHED THEN UPDATE SET
            t.batch_id = s.batch_id
    """)

    print(f"✅ Watermarks Updated : {len(success_updates)}")


# =====================================================
# AUDIT DETAIL SCHEMA
# =====================================================

audit_schema = StructType(
    [
        StructField("execution_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_seconds", LongType(), True),
        StructField("status", StringType(), True),
        StructField("inserted_rows", LongType(), True),
        StructField("updated_rows", LongType(), True),
        StructField("deleted_rows", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("retry_count", IntegerType(), True),
        StructField("created_ts", TimestampType(), True),
    ]
)


def update_silver_batch_watermark():
    """
    Promotes Silver batches that completed successfully
    during the current execution.

    Rules:

      - Promote batch only if NO transformation failed
        for that batch.

      - Tables with no work for a batch do NOT block
        promotion.

      - Only batches greater than the previously
        promoted watermark are inserted.
    """

    # ==========================================
    # BUILD AUDIT DATAFRAME
    # ==========================================

    audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)

    audit_df = audit_df.filter(F.col("batch_id").isNotNull())

    if not audit_df.head(1):
        print("ℹ️ No processed Silver batches found.")
        return

    # ==========================================
    # LAST PROMOTED BATCH
    # ==========================================

    last_promoted_batch = spark.sql(f"""
        SELECT MAX(batch_id) AS batch_id
        FROM control.silver_batch_watermark
        WHERE dependency_level = {DEPENDENCY_LEVEL}
    """).first()["batch_id"]

    last_promoted_batch = last_promoted_batch if last_promoted_batch is not None else -1

    # ==========================================
    # IDENTIFY PROMOTABLE BATCHES
    # ==========================================

    promotable_batches = (
        audit_df.groupBy("batch_id")
        .agg(
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias(
                "failed_count"
            )
        )
        .filter(
            (F.col("failed_count") == 0) & (F.col("batch_id") > last_promoted_batch)
        )
        .select("batch_id")
        .orderBy("batch_id")
    )

    if not promotable_batches.head(1):
        print("ℹ️ No new Silver batches available for promotion.")
        return

    # ==========================================
    # BUILD WATERMARK RECORDS
    # ==========================================

    watermark_df = (
        promotable_batches.withColumn("dependency_level", F.lit(int(DEPENDENCY_LEVEL)))
        .withColumn("completed_ts", F.current_timestamp())
        .withColumn("execution_id", F.lit(EXECUTION_ID))
    )

    watermark_df.createOrReplaceTempView("vw_silver_batch_watermark_updates")

    # ==========================================
    # MERGE WATERMARKS
    # ==========================================

    spark.sql("""
        MERGE INTO control.silver_batch_watermark t
        USING vw_silver_batch_watermark_updates s

        ON t.batch_id = s.batch_id
       AND t.dependency_level = s.dependency_level

        WHEN NOT MATCHED THEN
        INSERT
        (
            batch_id,
            dependency_level,
            completed_ts,
            execution_id
        )
        VALUES
        (
            s.batch_id,
            s.dependency_level,
            s.completed_ts,
            s.execution_id
        )
    """)

    promoted_count = watermark_df.count()

    print(f"✅ Silver Batch Watermark Updated | Promoted={promoted_count}")


update_watermarks(success_updates)
update_silver_batch_watermark()

# =====================================================
# PIPELINE STATUS
# =====================================================
success_count = sum(1 for status in table_state.values() if status == "SUCCESS")

failed_count = sum(1 for status in table_state.values() if status == "FAILED")

blocked_count = sum(1 for status in table_state.values() if status == "BLOCKED")

skipped_count = sum(1 for status in table_state.values() if status == "SKIPPED")

partial_success_count = sum(1 for table, error in failed_tables)

if partial_success_count > 0:
    pipeline_status = "FAILED"

elif blocked_count > 0:
    pipeline_status = "PARTIAL_SUCCESS"

else:
    pipeline_status = "SUCCESS"

# =====================================================
# PIPELINE DURATION
# =====================================================

PIPELINE_END_TS = datetime.now()

pipeline_duration_seconds = int((PIPELINE_END_TS - PIPELINE_START_TS).total_seconds())

pipeline_duration_minutes = round(pipeline_duration_seconds / 60.0, 2)

# =====================================================
# PIPELINE SUMMARY RECORD
# =====================================================

summary_row = Row(
    execution_id=EXECUTION_ID,
    start_ts=PIPELINE_START_TS,
    end_ts=PIPELINE_END_TS,
    duration_secs=pipeline_duration_seconds,
    duration_mins=pipeline_duration_minutes,
    total_tables=len(all_tables),
    successful_tables=success_count,
    failed_tables=failed_count,
    blocked_tables=blocked_count,
    skipped_tables=skipped_count,
    watermark_updates=len(success_updates),
    total_inserted_rows=sum(row.inserted_rows for row in audit_rows),
    total_updated_rows=sum(row.updated_rows for row in audit_rows),
    total_deleted_rows=sum(row.deleted_rows for row in audit_rows),
    status=pipeline_status,
)

# =====================================================
# PIPELINE SUMMARY
# =====================================================

print("=" * 80)
print("PIPELINE SUMMARY")
print("=" * 80)

print(f"Execution Id        : {EXECUTION_ID}")

print(f"Status              : {pipeline_status}")

print(f"Total Tables        : {len(all_tables)}")

print(f"Successful Tables   : {summary_row.successful_tables}")

print(f"Failed Tables       : {summary_row.failed_tables}")

print(f"Blocked Tables      : {summary_row.blocked_tables}")

print(f"Skipped Tables      : {summary_row.skipped_tables}")

print(f"Watermark Updates   : {summary_row.watermark_updates}")

print(f"Inserted Rows       : {summary_row.total_inserted_rows:,}")

print(f"Updated Rows        : {summary_row.total_updated_rows:,}")

print(f"Deleted Rows        : {summary_row.total_deleted_rows:,}")

print(f"Duration (Seconds)  : {pipeline_duration_seconds}")

print(f"Duration (Minutes)  : {pipeline_duration_minutes}")

print("=" * 80)

# COMMAND ----------

# DBTITLE 1,AUDIT PERSISTENCE & FINAL STATUS
# =====================================================
# CELL 6 - AUDIT PERSISTENCE & FINAL STATUS
# =====================================================
#
# Purpose:
#   Persists pipeline execution audit records.
#
# Responsibilities:
#   - Persist table-level audit records
#   - Persist pipeline summary record
#   - Display final completion status
#
# Notes:
#   No transformations are executed in this cell.
#
# =====================================================


# =====================================================
# PIPELINE SUMMARY SCHEMA
# =====================================================

summary_schema = StructType(
    [
        StructField("execution_id", StringType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("duration_secs", LongType(), True),
        StructField("duration_mins", DoubleType(), True),
        StructField("total_tables", LongType(), True),
        StructField("successful_tables", LongType(), True),
        StructField("failed_tables", LongType(), True),
        StructField("blocked_tables", LongType(), True),
        StructField("skipped_tables", LongType(), True),
        StructField("watermark_updates", LongType(), True),
        StructField("total_inserted_rows", LongType(), True),
        StructField("total_updated_rows", LongType(), True),
        StructField("total_deleted_rows", LongType(), True),
        StructField("status", StringType(), True),
    ]
)

# =====================================================
# TABLE LEVEL AUDIT PERSISTENCE
# =====================================================

if audit_rows:
    print(f"📝 Writing {len(audit_rows)} audit detail records")

    try:
        (
            spark.createDataFrame(audit_rows, schema=audit_schema)
            .write.mode("append")
            .saveAsTable(PIPELINE_EXECUTION_DETAIL_TABLE)
        )

        print("✅ Audit detail persisted")

    except Exception as ex:
        print(f"❌ Failed writing audit detail : {str(ex)}")

        raise

else:
    print("⚠️ No audit detail records found.")

# =====================================================
# PIPELINE SUMMARY PERSISTENCE
# =====================================================

print("📝 Writing pipeline summary record")

try:
    summary_df = spark.createDataFrame([summary_row], schema=summary_schema).withColumn(
        "dependency_level", F.lit(DEPENDENCY_LEVEL)
    )
    summary_df.write.mode("append").saveAsTable(PIPELINE_EXECUTION_TABLE)

    print("✅ Pipeline summary persisted")

except Exception as ex:
    print(f"❌ Failed writing pipeline summary : {str(ex)}")

    raise

# =====================================================
# AUDIT STATISTICS
# =====================================================

print("=" * 80)
print("AUDIT STATISTICS")
print("=" * 80)

print(f"Execution Id        : {EXECUTION_ID}")

print(f"Audit Detail Rows   : {len(audit_rows)}")

print(f"Pipeline Status     : {pipeline_status}")

print(f"Inserted Rows       : {summary_row.total_inserted_rows:,}")

print(f"Updated Rows        : {summary_row.total_updated_rows:,}")

print(f"Deleted Rows        : {summary_row.total_deleted_rows:,}")

print("=" * 80)

# =====================================================
# FINAL STATUS
# =====================================================

if pipeline_status == "SUCCESS":
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY")

elif pipeline_status == "PARTIAL_SUCCESS":
    print("⚠️ PIPELINE COMPLETED WITH BLOCKED TABLES")

else:
    print("❌ PIPELINE COMPLETED WITH FAILURES")

print("=" * 80)