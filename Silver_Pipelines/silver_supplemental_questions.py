# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,CONFIGURATION & PARAMETERS
# =====================================================
# CELL 1 - CONFIGURATION & PARAMETERS
# =====================================================
#
# Purpose:
#   Initializes runtime configuration for the
#   Supplemental Questions Framework Pipeline.
#
# Responsibilities:
#   - Import required libraries
#   - Load notebook parameters
#   - Configure Spark settings
#   - Configure scheduler settings
#   - Configure audit settings
#   - Configure pipeline settings
#   - Generate execution context
#
# Notes:
#   No metadata loading occurs in this cell.
#   No entity execution occurs in this cell.
#
# =====================================================

"""
Supplemental Questions Framework Pipeline

Purpose:
    Execute Supplemental Question processing
    for all configured entities using a
    metadata-driven framework.

Supported Entities:

    - Incident
    - Vitals
    - Medication
    - PatientProcedure
    - MedicalDevice
    - AirwayConfirmation
    - CrewMember

Features:

    - Metadata Driven Processing
    - Dynamic Entity Discovery
    - Dynamic Overflow Table Creation
    - Dynamic Pivot Generation
    - Dynamic Merge Processing
    - Parallel Entity Execution
    - Audit Logging
    - Watermark Tracking
    - Failure Isolation

Execution Flow:

        Configuration
              |
              v
        Metadata Load
              |
              v
        Metadata Validation
              |
              v
        Entity Discovery
              |
              v
        Entity Execution
              |
              v
        Dynamic Table Creation
              |
              v
        Pivot + Merge
              |
              v
        Audit Logging
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
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Row


from datetime import datetime

import traceback
import uuid
import time
import os

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
spark.sql(f"USE CATALOG {CATALOG}")

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

spark.conf.set("spark.databricks.delta.retryWriteConflict.enabled", "true")

# =====================================================
# PIPELINE CONFIGURATION
# =====================================================

PIPELINE_NAME = "SupplementalQuestionsFramework"

PIPELINE_VERSION = "1.0"
SILVER_TABLE_REGISTRY = "control.silver_table_registry"

BATCH_WATERMARK_TABLE = "control.bronze_batch_watermark"

PIPELINE_TYPE = "SUPPLEMENTAL_QUESTIONS"

# =====================================================
# METADATA CONFIGURATION
# =====================================================

SQ_ENTITY_REGISTRY_TABLE = "control.Silver_SQEntityRegistry"

SQ_RW_MAPPING_TABLE = "silver.dim_supplementalquestions_rw_mapping"

# =====================================================
# TARGET TABLE CONFIGURATION
# =====================================================

QA_COLUMN_COUNT = 300

SUPPORTED_DATA_TYPES = ["STRING", "DATE", "TIMESTAMP", "INT", "DECIMAL"]

# =====================================================
# SCHEDULER CONFIGURATION
# =====================================================

BASE_WORKERS = 3

MIN_WORKERS = 2

MAX_WORKERS = 8

MAX_RETRIES = 1

RETRY_WAIT_SECONDS = 5

PROGRESS_INTERVAL = 1

current_workers = BASE_WORKERS 

# =====================================================
# AUDIT CONFIGURATION
# =====================================================

PIPELINE_EXECUTION_TABLE = "control.silver_pipeline_execution"

PIPELINE_EXECUTION_DETAIL_TABLE = "control.silver_pipeline_execution_detail"

# =====================================================
# EXECUTION CONTEXT
# =====================================================

EXECUTION_ID = datetime.now().strftime("%Y%m%d%H%M%S%f")

PIPELINE_START_TS = datetime.now()

# =====================================================
# LOGGING HEADER
# =====================================================

print("=" * 100)
print("SUPPLEMENTAL QUESTIONS FRAMEWORK PIPELINE STARTED")
print("=" * 100)

print(f"Pipeline Name    : {PIPELINE_NAME}")
print(f"Pipeline Type    : {PIPELINE_TYPE}")
print(f"Version          : {PIPELINE_VERSION}")
print(f"Execution Id     : {EXECUTION_ID}")
print(f"Start Time       : {PIPELINE_START_TS}")
print(f"Run Mode         : {run_mode}")
print(f"Base Workers     : {BASE_WORKERS}")
print(f"Max Workers      : {MAX_WORKERS}")

print("=" * 100)

# COMMAND ----------

# DBTITLE 1,Metadata Loading & Validation
# =====================================================
# CELL 2 - METADATA LOADING & VALIDATION
# =====================================================
#
# Purpose:
#   Load and validate all Supplemental Question
#   framework metadata before execution begins.
#
# Responsibilities:
#   - Load active entities
#   - Validate registry metadata
#   - Validate mandatory columns
#   - Build entity configuration collection
#   - Print execution summary
#
# Notes:
#   No SQ processing occurs in this cell.
#   No source tables are read in this cell.
#   No target tables are updated in this cell.
#
# =====================================================

print("=" * 100)
print("STEP 1 - LOADING SQ ENTITY REGISTRY")
print("=" * 100)

# =====================================================
# LOAD ACTIVE ENTITIES
# =====================================================

entity_registry_df = spark.table(SQ_ENTITY_REGISTRY_TABLE).filter(
    (F.col("IsActive") == True) & (F.lower(F.col("ProcessingFramework")) == "sqframework")
)

entity_count = entity_registry_df.count()

print(f"Active SQ Entities Identified = {entity_count}")

if entity_count == 0:
    raise Exception(f"No Active Entities Found In {SQ_ENTITY_REGISTRY_TABLE}")

# =====================================================
# VALIDATE REQUIRED METADATA
# =====================================================

required_columns = [
    "EntityName",
    "BaseAnswerTableName",
    "SourceTableName",
    "BusinessKeyColumn",
    "BusinessKeyInternalColumn",
    "IncidentJoinColumn",
    "SourceJoinType",
    "IncidentTypeFilter",
    "IsActive",
    "ProcessingFramework",
]

missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in entity_registry_df.columns
]

if len(missing_columns) > 0:
    raise Exception("Missing Registry Columns : " + ",".join(missing_columns))

print("Registry Column Validation Completed")

# =====================================================
# VALIDATE NULL METADATA
# =====================================================

mandatory_columns = [
    "EntityName",
    "BaseAnswerTableName",
    "SourceTableName",
    "BusinessKeyColumn",
    "BusinessKeyInternalColumn",
    "IncidentJoinColumn",
    "SourceJoinType",
]

for column_name in mandatory_columns:
    null_count = entity_registry_df.filter(F.col(column_name).isNull()).count()

    if null_count > 0:
        raise Exception(f"Null Values Found In Registry Column = {column_name}")

print("Registry Null Validation Completed")

# =====================================================
# VALIDATE SOURCE JOIN TYPES
# =====================================================

valid_source_join_types = ["ENTITY_GLOBALIDENTIFIER", "INCIDENT_OBJECTID"]

invalid_join_types = entity_registry_df.filter(
    ~F.col("SourceJoinType").isin(valid_source_join_types)
)

invalid_count = invalid_join_types.count()

if invalid_count > 0:
    display(invalid_join_types)

    raise Exception("Invalid SourceJoinType Found")

print("Source Join Type Validation Completed")

# =====================================================
# VALIDATE ENTITY UNIQUENESS
# =====================================================

duplicate_entities = (
    entity_registry_df.groupBy("EntityName").count().filter(F.col("count") > 1)
)

duplicate_count = duplicate_entities.count()

if duplicate_count > 0:
    display(duplicate_entities)

    raise Exception("Duplicate Entity Definitions Found")

print("Entity Uniqueness Validation Completed")

# =====================================================
# BUILD ENTITY CONFIGURATION COLLECTION
# =====================================================

entity_configs = entity_registry_df.orderBy("EntityName").collect()

print(f"Entity Configurations Loaded = {len(entity_configs)}")

# =====================================================
# METADATA SUMMARY
# =====================================================

print("=" * 100)
print("ACTIVE ENTITY SUMMARY")
print("=" * 100)

display(entity_registry_df.orderBy("EntityName"))

print("=" * 100)
print("METADATA VALIDATION COMPLETED")
print("=" * 100)

# COMMAND ----------

# DBTITLE 1,reusable framework functions
# =====================================================
# LOGGING HELPERS
# =====================================================


def log_info(entity_name, message):
    print(f"[INFO][{entity_name}] {message}")


def log_warning(entity_name, message):
    print(f"[WARNING][{entity_name}] {message}")


def log_error(entity_name, message):
    print(f"[ERROR][{entity_name}] {message}")


def get_entity_config(entity_name):
    matches = [
        config for config in entity_configs if config["EntityName"] == entity_name
    ]

    if len(matches) == 0:
        raise Exception(f"Entity Not Found : {entity_name}")

    return matches[0]


def normalize_fieldvalue(dataframe):
    return dataframe.withColumn(
        "FieldValue",
        F.when(F.trim(F.col("FieldValue")) == "", None).otherwise(F.col("FieldValue")),
    )


def validate_duplicate_answers(entity_name, dataframe, business_key_column):
    duplicate_count = (
        dataframe.groupBy("tenant_id", business_key_column, "AnswerColumnName")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        log_warning(entity_name, f"Duplicate Groups Found = {duplicate_count}")


# =====================================================
# PIVOT DATAFRAME BUILDER
# =====================================================
#
# Purpose:
#   Convert row-based SQ answers into
#   QA1-QA300 pivot structure.
#
# Input:
#   tenant_id
#   business key
#   AnswerColumnName
#   FieldValue
#
# Output:
#   One row per
#       tenant_id
#       business key
#
# Notes:
#   Uses MAX(FieldValue) to align with
#   legacy SQL MAX(CASE WHEN...) behavior.
#
# =====================================================

# =====================================================
# PIVOT DATAFRAME BUILDER
# =====================================================
#
# Purpose:
#   Convert row-based SQ answers into
#   QA1-QA300 pivot structure.
#
# Input:
#   entity_name
#   dataframe
#   source_business_key_column
#   target_business_key_column
#   target_datatype
#
# Output:
#   One row per:
#       tenant_id
#       business key
#
# Notes:
#   Uses MAX(FieldValue) to align with
#   legacy SQL MAX(CASE WHEN...) behavior.
#
# =====================================================


def build_pivot_dataframe(
    entity_name,
    dataframe,
    source_business_key_column,
    target_business_key_column,
    target_datatype,
    answer_table_name,
    pipeline_run_id,
):
    log_info(entity_name, "Building Pivot DataFrame")
    if dataframe.limit(1).count() == 0:
        log_warning(entity_name, "No Records Available For Pivot")

        return None

    # =================================================
    # APPLY DATATYPE CONVERSION
    # =================================================
    dataframe = normalize_fieldvalue(dataframe)

    if target_datatype == "DATE":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue as date)")
        )

    elif target_datatype == "TIMESTAMP":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue as timestamp)")
        )

    elif target_datatype == "INT":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue as int)")
        )

    elif target_datatype == "DECIMAL(18,8)":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue as decimal(18,8))")
        )

    else:
        working_df = dataframe

    # =================================================
    # PIVOT
    # =================================================

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * QA_COLUMN_COUNT + 1
    end_column = start_column + QA_COLUMN_COUNT - 1

    qa_columns = [f"QA{i}" for i in range(start_column, end_column + 1)]

    pivot_df = (
        working_df.groupBy("tenant_id", source_business_key_column)
        .pivot("AnswerColumnName", qa_columns)
        .agg(F.max("FieldValue"))
    )

    # =================================================
    # RENAME BUSINESS KEY
    # =================================================

    pivot_df = pivot_df.withColumnRenamed(
        source_business_key_column, target_business_key_column
    )

    # =================================================
    # AUDIT COLUMNS
    # =================================================

    pivot_df = (
        pivot_df.withColumn("SystemID", F.lit(0))
        .withColumn("CreatedOn", F.current_timestamp())
        .withColumn("ModifiedOn", F.current_timestamp())
        .withColumn("IsDeleted", F.lit(False))
        .withColumn("batch_id", F.lit(int(pipeline_run_id)))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # pivot_count = pivot_df.count()

    # log_info(entity_name, f"Pivot Records = {pivot_count}")

    return pivot_df

# COMMAND ----------

# =====================================================
# LOAD PENDING BATCHES
# =====================================================
#
# Purpose:
#   Identify pending batches that need to be processed
#   by Supplemental Questions framework.
#
# Supports:
#   NORMAL Mode
#   REPLAY Mode
#
# Returns:
#   List[int]
#
# =====================================================


def load_pending_batches():
    log_info("FRAMEWORK", f"Loading Pending Batches - Mode={run_mode}")

    # ================================================
    # REPLAY MODE
    # ================================================

    if run_mode == "REPLAY":
        if not replay_batch_id:
            raise Exception("Replay Batch Id Required When Run Mode = REPLAY")

        pending_batches = [int(replay_batch_id)]

        log_info("FRAMEWORK", f"Replay Batch = {replay_batch_id}")

        return pending_batches

    # ================================================
    # NORMAL MODE
    # ================================================

    registry_df = spark.table(SILVER_TABLE_REGISTRY).filter(
        F.lower(F.col("silver_table")) == "supplemental_questions"
    )

    registry_rows = registry_df.collect()

    if len(registry_rows) == 0:
        raise Exception("Registry Entry Not Found For supplemental_questions")

    last_processed_batch_id = registry_rows[0]["batch_id"]

    if last_processed_batch_id is None:
        last_processed_batch_id = 0

    log_info("FRAMEWORK", f"Last Processed Batch = {last_processed_batch_id}")

    # ================================================
    # LOAD NEW BATCHES
    # ================================================

    pending_batches_df = (
        spark.table(BATCH_WATERMARK_TABLE)
        .filter(F.col("batch_id") > last_processed_batch_id)
        .select(F.col("batch_id").alias("batch_id"))
        .distinct()
        .orderBy("batch_id")
    )

    pending_batches = [row["batch_id"] for row in pending_batches_df.collect()]

    log_info("FRAMEWORK", f"Pending Batch Count = {len(pending_batches)}")

    if len(pending_batches) > 0:
        log_info("FRAMEWORK", f"Oldest Batch = {pending_batches[0]}")

        log_info("FRAMEWORK", f"Latest Batch = {pending_batches[-1]}")

    return pending_batches


pending_batches = load_pending_batches()

if len(pending_batches) == 0:
    log_info("FRAMEWORK", "No Pending Batches Found")

    dbutils.notebook.exit("No Pending Batches Found")

log_info("FRAMEWORK", f"Pending Batches = {pending_batches}")

# =====================================================
# UPDATE WATERMARK
# =====================================================


def update_batch_watermark(batch_id):
    log_info("FRAMEWORK", f"Updating Watermark = {batch_id}")

    update_sql = f"""
    UPDATE {SILVER_TABLE_REGISTRY}
    SET batch_id = {batch_id}
    WHERE lower(silver_table)
          = 'supplemental_questions'
    """

    spark.sql(update_sql)

    log_info("FRAMEWORK", f"Watermark Updated = {batch_id}")


def build_audit_row(
    table_name,
    batch_id,
    status,
    start_ts,
    end_ts,
    inserted_rows=0,
    updated_rows=0,
    deleted_rows=0,
    error_message=None,
):
    return Row(
        execution_id=EXECUTION_ID,
        table_name=table_name,
        batch_id=batch_id,
        start_ts=start_ts,
        end_ts=end_ts,
        duration_seconds=int((end_ts - start_ts).total_seconds()),
        status=status,
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        deleted_rows=deleted_rows,
        error_message=error_message,
        retry_count=0,
        created_ts=datetime.now(),
    )


# =====================================================
# SAVE EXECUTION DETAILS
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
        StructField("dependency_level", IntegerType(), True),
    ]
)


def save_execution_details():
    global audit_rows
    global audit_schema

    # =================================================
    # VALIDATION
    # =================================================

    if len(audit_rows) == 0:
        log_warning("FRAMEWORK", "No Audit Records To Persist")

        return

    log_info("FRAMEWORK", f"Writing {len(audit_rows)} Audit Detail Records")

    try:
        detail_df = spark.createDataFrame(
            audit_rows,
            schema=audit_schema,
        )

        (
            detail_df.write.format("delta")
            .mode("append")
            .saveAsTable("control.silver_pipeline_execution_detail")
        )

        successful_rows = len([row for row in audit_rows if row.status == "SUCCESS"])

        failed_rows = len([row for row in audit_rows if row.status == "FAILED"])

        total_inserted_rows = sum(row.inserted_rows or 0 for row in audit_rows)

        total_updated_rows = sum(row.updated_rows or 0 for row in audit_rows)

        total_deleted_rows = sum(row.deleted_rows or 0 for row in audit_rows)

        log_info("FRAMEWORK", f"Persisted {len(audit_rows)} Detail Records")

        log_info("FRAMEWORK", f"Successful Detail Records = {successful_rows}")

        log_info("FRAMEWORK", f"Failed Detail Records = {failed_rows}")

        log_info("FRAMEWORK", f"Inserted Rows = {total_inserted_rows}")

        log_info("FRAMEWORK", f"Updated Rows = {total_updated_rows}")

        log_info("FRAMEWORK", f"Deleted Rows = {total_deleted_rows}")

    except Exception as ex:
        log_error("FRAMEWORK", f"Failed Persisting Audit Details : {str(ex)}")

        raise


# =====================================================
# SAVE EXECUTION SUMMARY
# =====================================================


def save_execution_summary(
    execution_start_ts,
    execution_end_ts,
    successful_batches,
    failed_batches,
):
    global audit_rows
    global summary_schema

    # =================================================
    # TABLE COUNTS
    # =================================================

    total_tables = len(audit_rows)

    successful_tables = len([row for row in audit_rows if row.status == "SUCCESS"])

    failed_tables = len([row for row in audit_rows if row.status == "FAILED"])

    # =================================================
    # MERGE METRICS
    # =================================================

    total_inserted_rows = sum(row.inserted_rows or 0 for row in audit_rows)

    total_updated_rows = sum(row.updated_rows or 0 for row in audit_rows)

    total_deleted_rows = sum(row.deleted_rows or 0 for row in audit_rows)

    # =================================================
    # DURATION
    # =================================================

    duration_seconds = int((execution_end_ts - execution_start_ts).total_seconds())

    duration_minutes = round(
        duration_seconds / 60,
        2,
    )

    # =================================================
    # STATUS
    # =================================================

    summary_status = "FAILED" if failed_tables > 0 else "SUCCESS"

    # =================================================
    # SUMMARY LOGGING
    # =================================================

    log_info("FRAMEWORK", f"Total Tables = {total_tables}")

    log_info("FRAMEWORK", f"Successful Tables = {successful_tables}")

    log_info("FRAMEWORK", f"Failed Tables = {failed_tables}")

    log_info("FRAMEWORK", f"Inserted Rows = {total_inserted_rows}")

    log_info("FRAMEWORK", f"Updated Rows = {total_updated_rows}")

    log_info("FRAMEWORK", f"Deleted Rows = {total_deleted_rows}")

    # =================================================
    # SUMMARY RECORD
    # =================================================

    summary_row = [
        (
            EXECUTION_ID,
            execution_start_ts,
            execution_end_ts,
            duration_seconds,
            duration_minutes,
            total_tables,
            successful_tables,
            failed_tables,
            0,  # blocked_tables
            0,  # skipped_tables
            len(successful_batches),
            total_inserted_rows,
            total_updated_rows,
            total_deleted_rows,
            summary_status,
            0,  # dependency_level
        )
    ]

    try:
        summary_df = spark.createDataFrame(
            summary_row,
            schema=summary_schema,
        )

        (
            summary_df.write.format("delta")
            .mode("append")
            .saveAsTable("control.silver_pipeline_execution")
        )

        log_info("FRAMEWORK", "Execution Summary Persisted")

    except Exception as ex:
        log_error("FRAMEWORK", f"Failed Persisting Summary : {str(ex)}")

        raise

# COMMAND ----------

rw_mapping_df = (
    spark.table(SQ_RW_MAPPING_TABLE)
    .persist()
)

# COMMAND ----------

# =====================================================
# MERGE ANSWER TABLE
# =====================================================
#
# Purpose:
#   Merge pivoted SQ data into a single
#   Supplemental Question Answer Table.
#
# Input:
#   entity_name
#   answer_table_name
#   business_key_column
#   pivot_df
#   pipeline_batch_id
#
# Output:
#   MERGE INTO target table
#
# Notes:
#   - Handles inserts
#   - Handles updates
#   - Updates audit columns
#   - Entity agnostic
#
# =====================================================


def merge_answer_table(
    entity_name,
    answer_table_name,
    business_key_column,
    pivot_df,
    pipeline_batch_id,
):
    log_info(entity_name, f"Starting Merge : {answer_table_name}")

    if pivot_df is None:
        log_warning(entity_name, f"No Data Found : {answer_table_name}")

        return None

    # record_count = pivot_df.count()

    if not pivot_df.take(1):
        log_warning(entity_name, f"Zero Records : {answer_table_name}")

        return None

    # log_info(entity_name, f"Records To Merge = {record_count}")

    temp_view_name = f"CurrentPivot_{entity_name}"

    pivot_df.createOrReplaceTempView(temp_view_name)

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * QA_COLUMN_COUNT + 1
    end_column = start_column + QA_COLUMN_COUNT - 1

    qa_columns = [f"QA{i}" for i in range(start_column, end_column + 1)]

    update_columns = [
        f"target.{column_name}=source.{column_name}" for column_name in qa_columns
    ]

    update_columns.extend(
        [
            "target.ModifiedOn=current_timestamp()",
            f"target.batch_id={pipeline_batch_id}",
            "target.ingest_ts=current_timestamp()",
        ]
    )

    update_set = ",\n".join(update_columns)

    insert_columns = [
        "tenant_id",
        business_key_column,
    ]

    insert_columns.extend(qa_columns)

    insert_columns.extend(
        [
            "SystemID",
            "CreatedOn",
            "ModifiedOn",
            "IsDeleted",
            "batch_id",
            "ingest_ts",
        ]
    )

    insert_values = [
        "source.tenant_id",
        f"source.{business_key_column}",
    ]

    insert_values.extend([f"source.{column_name}" for column_name in qa_columns])

    insert_values.extend(
        [
            "source.SystemID",
            "source.CreatedOn",
            "source.ModifiedOn",
            "source.IsDeleted",
            "source.batch_id",
            "source.ingest_ts",
        ]
    )

    merge_sql = f"""
    MERGE INTO {answer_table_name} target

    USING {temp_view_name} source

    ON target.tenant_id = source.tenant_id
    AND target.{business_key_column}
        = source.{business_key_column}

    WHEN MATCHED THEN
    UPDATE SET
    {update_set}

    WHEN NOT MATCHED THEN
    INSERT
    (
        {",".join(insert_columns)}
    )
    VALUES
    (
        {",".join(insert_values)}
    )
    """

    table_start_ts = datetime.now()

    try:
        log_info(entity_name, f"Executing Merge : {answer_table_name}")

        merge_result = spark.sql(merge_sql)

        inserted_rows = 0
        updated_rows = 0
        deleted_rows = 0

        try:
            metric_row = merge_result.first()

            log_info(entity_name, f"Merge Metrics Raw = {metric_row}")

            if metric_row:
                inserted_rows = getattr(metric_row, "num_inserted_rows", 0) or 0

                updated_rows = getattr(metric_row, "num_updated_rows", 0) or 0

                deleted_rows = getattr(metric_row, "num_deleted_rows", 0) or 0

        except Exception as metric_ex:
            log_warning(entity_name, f"Unable To Read Merge Metrics : {str(metric_ex)}")

        table_end_ts = datetime.now()

        log_info(entity_name, f"Merge Completed : {answer_table_name}")

        log_info(
            entity_name,
            f"Inserted={inserted_rows}, Updated={updated_rows}, Deleted={deleted_rows}",
        )

        return build_audit_row(
            table_name=answer_table_name,
            batch_id=pipeline_batch_id,
            status="SUCCESS",
            start_ts=table_start_ts,
            end_ts=table_end_ts,
            inserted_rows=inserted_rows,
            updated_rows=updated_rows,
            deleted_rows=deleted_rows,
        )

    except Exception as ex:
        table_end_ts = datetime.now()

        log_error(entity_name, f"Merge Failed : {answer_table_name}")

        return build_audit_row(
            table_name=answer_table_name,
            batch_id=pipeline_batch_id,
            status="FAILED",
            start_ts=table_start_ts,
            end_ts=table_end_ts,
            inserted_rows=0,
            updated_rows=0,
            deleted_rows=0,
            error_message=str(ex)[:2000],
        )


# =====================================================
# PROCESS ENTITY
# =====================================================
#
# Purpose:
#   Execute Supplemental Question processing
#   for a single configured entity.
#
# Flow:
#
#   1. Build Changed Entity Dataset
#   2. Build SQ Source Datasets
#   3. Build Final SQ Dataset
#   4. Discover Answer Tables
#   5. Create Missing Tables
#   6. Pivot Answer Tables
#   7. Merge Into Targets
#
# =====================================================
# =====================================================
# PROCESS ENTITY
# =====================================================
#
# Purpose:
#   Execute Supplemental Questions processing
#   for a single entity and single batch.
#
# Flow:
#   Step 0  - Changed Entities
#   Step 1  - Build Final SQ Dataset
#   Step 2  - Validation
#   Step 3  - Discover Answer Tables
#   Step 4  - Create Missing Tables
#   Step 5  - Pivot + Merge
#
# =====================================================
def process_entity(entity_config, pipeline_run_id):
    entity_name = entity_config["EntityName"]

    log_info(entity_name, f"Entity Processing Started (Batch={pipeline_run_id})")

    start_time = time.time()

    entity_audit_rows = []

    try:
        # ==========================================
        # ENTITY CONFIGURATION
        # ==========================================

        base_answer_table_name = entity_config["BaseAnswerTableName"]

        business_key_column = entity_config["BusinessKeyColumn"]

        business_key_internal_column = entity_config["BusinessKeyInternalColumn"]

        source_table_name = entity_config["SourceTableName"]

        incident_join_column = entity_config["IncidentJoinColumn"]

        source_join_type = entity_config["SourceJoinType"]

        incident_type_filter = entity_config["IncidentTypeFilter"]

        # ==========================================
        # STEP 0
        # CHANGED ENTITIES
        # ==========================================

        changed_entities_df = build_changed_entities(
            entity_config,
            pipeline_run_id,
        )

        changed_entities_df = changed_entities_df.persist()

        changed_count = changed_entities_df.count()

        log_info(
            entity_name,
            f"Batch={pipeline_run_id} Changed Entities = {changed_count}",
        )

        if changed_count == 0:
            log_info(
                entity_name,
                f"Batch={pipeline_run_id} No Changes Detected",
            )

            return {
                "EntityName": entity_name,
                "Status": "SUCCESS",
                "AuditRows": entity_audit_rows,
                "ErrorMessage": None,
            }

        # ==========================================
        # STEP 1-6
        # BUILD FINAL SQ DATASET
        # ==========================================

        final_sq_df = build_final_sq_dataset(
            entity_config,
            changed_entities_df,
        )

        final_sq_df = final_sq_df.persist()

        final_count = final_sq_df.count()

        log_info(
            entity_name,
            f"Batch={pipeline_run_id} Final SQ Rows = {final_count}",
        )

        if final_count == 0:
            log_info(
                entity_name,
                f"Batch={pipeline_run_id} No SQ Data Found",
            )

            return {
                "EntityName": entity_name,
                "Status": "SUCCESS",
                "AuditRows": entity_audit_rows,
                "ErrorMessage": None,
            }

        # ==========================================
        # VALIDATION
        # ==========================================

        validate_duplicate_answers(
            entity_name,
            final_sq_df,
            business_key_column,
        )

        # ==========================================
        # DISCOVER ANSWER TABLES
        # ==========================================

        (
            answer_tables_df,
            missing_tables_df,
        ) = load_answer_table_metadata(
            entity_name,
            base_answer_table_name,
        )

        answer_table_count = answer_tables_df.count()

        log_info(
            entity_name,
            f"Batch={pipeline_run_id} Answer Tables Found = {answer_table_count}",
        )

        # ==========================================
        # CREATE MISSING TABLES
        # ==========================================

        create_missing_answer_tables(
            missing_tables_df,
            business_key_internal_column,
        )

        # ==========================================
        # AVAILABLE ANSWER TABLES
        # ==========================================

        available_answer_tables = {
            row["AnswerTableName"].lower()
            for row in (final_sq_df.select("AnswerTableName").distinct().collect())
        }

        log_info(
            entity_name,
            f"Batch={pipeline_run_id} Answer Tables With Data = "
            f"{len(available_answer_tables)}",
        )

        # ==========================================
        # PROCESS EACH ANSWER TABLE
        # ==========================================

        answer_tables = answer_tables_df.collect()

        for row in answer_tables:
            answer_table_name = row["AnswerTableName"]

            log_info(
                entity_name,
                f"Batch={pipeline_run_id} "
                f"Processing Answer Table = "
                f"{answer_table_name}",
            )

            if answer_table_name.lower() not in available_answer_tables:
                log_info(
                    entity_name,
                    f"Batch={pipeline_run_id} No Rows Found For {answer_table_name}",
                )

                continue

            current_df = final_sq_df.filter(
                F.lower(F.col("AnswerTableName")) == answer_table_name.lower()
            )

            pivot_df = build_pivot_dataframe(
                entity_name,
                current_df,
                business_key_column,
                business_key_internal_column,
                row["TargetDataType"],
                answer_table_name,
                pipeline_run_id,
            )

            audit_row = merge_answer_table(
                entity_name,
                answer_table_name,
                business_key_internal_column,
                pivot_df,
                pipeline_run_id,
            )

            if audit_row is not None:
                entity_audit_rows.append(audit_row)

                if audit_row.status == "FAILED":
                    return {
                        "EntityName": entity_name,
                        "Status": "FAILED",
                        "AuditRows": entity_audit_rows,
                        "ErrorMessage": f"Merge Failed : {answer_table_name}",
                    }

        elapsed_seconds = round(
            time.time() - start_time,
            2,
        )

        log_info(
            entity_name,
            f"Batch={pipeline_run_id} "
            f"Completed Successfully "
            f"in {elapsed_seconds} seconds",
        )

        return {
            "EntityName": entity_name,
            "Status": "SUCCESS",
            "AuditRows": entity_audit_rows,
            "ErrorMessage": None,
        }

    except Exception as ex:
        log_error(
            entity_name,
            f"Batch={pipeline_run_id} {str(ex)}",
        )

        log_error(
            entity_name,
            traceback.format_exc(),
        )

        return {
            "EntityName": entity_name,
            "Status": "FAILED",
            "AuditRows": entity_audit_rows,
            "ErrorMessage": str(ex),
        }


# =====================================================
# LOAD ANSWER TABLE METADATA
# =====================================================
#
# Purpose:
#   Discover all answer tables configured for
#   an entity from RW Mapping and identify
#   missing physical tables.
#
# Input:
#   entity_name
#   base_answer_table_name
#
# Output:
#   answer_tables_df
#   missing_tables_df
#
# =====================================================


def load_answer_table_metadata(entity_name, base_answer_table_name):
    log_info(entity_name, "Loading Answer Table Metadata")

    answer_tables_df = (
        rw_mapping_df.filter(
            F.lower(F.col("AnswerTableName")).like(f"{base_answer_table_name.lower()}%")
        )
        .filter(F.upper(F.col("DataType")).isin(SUPPORTED_DATA_TYPES))
        .select(
            F.lower(F.col("AnswerTableName")).alias("AnswerTableName"),
            F.when(F.upper(F.col("DataType")) == "STRING", "STRING")
            .when(F.upper(F.col("DataType")) == "DATE", "DATE")
            .when(F.upper(F.col("DataType")) == "TIMESTAMP", "TIMESTAMP")
            .when(F.upper(F.col("DataType")) == "INT", "INT")
            .when(F.upper(F.col("DataType")) == "DECIMAL", "DECIMAL(18,8)")
            .alias("TargetDataType"),
        )
        .distinct()
    )

    existing_tables_df = spark.sql(
        """
            SELECT
                lower(
                    concat(
                        table_schema,
                        '.',
                        table_name
                    )
                ) AS TableName
            FROM information_schema.tables
            WHERE lower(table_schema)='silver'
            """
    )

    missing_tables_df = (
        answer_tables_df.alias("a")
        .join(
            existing_tables_df.alias("e"),
            F.lower(F.col("a.AnswerTableName")) == F.lower(F.col("e.TableName")),
            "left",
        )
        .filter(F.col("e.TableName").isNull())
        .select("a.AnswerTableName", "a.TargetDataType")
    )

    missing_count = missing_tables_df.count()

    log_info(entity_name, f"Missing Tables Found = {missing_count}")

    return (answer_tables_df, missing_tables_df)

# COMMAND ----------

# =====================================================
# BUILD FINAL SQ DATASET
# =====================================================
#
# Purpose:
#   Build final SQ dataset ready for
#   AnswerTable processing.
#
# Output:
#
#   BusinessKey
#   tenant_id
#   AnswerTableName
#   AnswerColumnName
#   FieldValue
#   DataType
#
# =====================================================


def build_final_sq_dataset(entity_config, changed_entities_df):
    entity_name = entity_config["EntityName"]

    source_table_name = entity_config["SourceTableName"]

    business_key_column = entity_config["BusinessKeyColumn"]

    base_answer_table_name = entity_config["BaseAnswerTableName"]

    log_info(entity_name, "Building Final SQ Dataset")

    # ==========================================
    # MULTISELECT
    # ==========================================

    fieldvalue_multiselect_df = build_multiselect_dataset(
        entity_config, changed_entities_df
    )

    # ==========================================
    # SINGLE SELECT
    # ==========================================

    fieldvalue_singleselect_df = build_singleselect_dataset(
        entity_config, changed_entities_df
    )

    # ==========================================
    # GENERIC
    # ==========================================

    fieldvalue_generic_df = build_generic_dataset(entity_config, changed_entities_df)

    # ==========================================
    # CONTROL MAPPING
    # ==========================================

    field_control_mapping_df = (
        spark.table("bronze.supplementalquestions_fielddefinition")
        .alias("fd")
        .join(
            spark.table("bronze.supplementalquestions_sqcontroltype").alias("ct"),
            [
                F.col("fd.ControlTypeID") == F.col("ct.SQControlTypeID"),
                F.col("fd.tenant_id") == F.col("ct.tenant_id"),
            ],
        )
        .filter(F.col("fd.DeletedStatus") == False)
        .select(
            F.col("fd.FieldDefinitionID"),
            F.col("fd.tenant_id"),
            F.col("ct.Description").alias("ControlDescription"),
            F.when(F.col("ct.Description") == "SingleSelect", "SingleSelect")
            .when(F.col("ct.Description") == "MultiSelect", "MultiSelect")
            .otherwise("Generic")
            .alias("ValueType"),
        )
    )

    # ==========================================
    # MULTISELECT
    # ==========================================

    multiselect_df = (
        field_control_mapping_df.alias("fcm")
        .filter(F.col("ValueType") == "MultiSelect")
        .join(
            fieldvalue_multiselect_df.alias("m"),
            [
                F.col("fcm.FieldDefinitionID") == F.col("m.FieldDefinitionID"),
                F.col("fcm.tenant_id") == F.col("m.tenant_id"),
            ],
        )
        .select(
            F.col(f"m.{business_key_column}").alias(business_key_column),
            F.col("m.FieldDefinitionID"),
            F.col("m.FieldValue"),
            F.col("m.tenant_id"),
        )
    )

    # ==========================================
    # SINGLE SELECT
    # ==========================================

    singleselect_df = (
        field_control_mapping_df.alias("fcm")
        .filter(F.col("ValueType") == "SingleSelect")
        .join(
            fieldvalue_singleselect_df.alias("s"),
            [
                F.col("fcm.FieldDefinitionID") == F.col("s.FieldDefinitionID"),
                F.col("fcm.tenant_id") == F.col("s.tenant_id"),
            ],
        )
        .select(
            F.col(f"s.{business_key_column}").alias(business_key_column),
            F.col("s.FieldDefinitionID"),
            F.col("s.FieldValue"),
            F.col("s.tenant_id"),
        )
    )

    # ==========================================
    # GENERIC
    # ==========================================

    generic_df = (
        field_control_mapping_df.alias("fcm")
        .filter(F.col("ValueType") == "Generic")
        .join(
            fieldvalue_generic_df.alias("g"),
            [
                F.col("fcm.FieldDefinitionID") == F.col("g.FieldDefinitionID"),
                F.col("fcm.tenant_id") == F.col("g.tenant_id"),
            ],
        )
        .select(
            F.col(f"g.{business_key_column}").alias(business_key_column),
            F.col("g.FieldDefinitionID"),
            F.col("g.FieldValue"),
            F.col("g.tenant_id"),
        )
    )

    sq_base_df = multiselect_df.unionByName(singleselect_df).unionByName(generic_df)

    final_sq_df = (
        sq_base_df.alias("v")
        .join(
            rw_mapping_df.alias("m"),
            [
                F.col("v.FieldDefinitionID") == F.col("m.FieldDefinitionIDInternal"),
                F.col("v.tenant_id") == F.col("m.tenant_id"),
            ],
        )
        .filter(
            F.lower(F.col("m.AnswerTableName")).like(
                f"{base_answer_table_name.lower()}%"
            )
        )
        .select(
            F.col(f"v.{business_key_column}"),
            F.col("m.tenant_id"),
            F.lower(F.col("m.AnswerTableName")).alias("AnswerTableName"),
            F.col("m.AnswerColumnName"),
            F.col("v.FieldValue"),
            F.upper(F.col("m.DataType")).alias("DataType"),
        )
    )

    return final_sq_df


# =====================================================
# CREATE MISSING ANSWER TABLES
# =====================================================
#
# Purpose:
#   Create missing SQ Answer Tables discovered
#   from RW Mapping metadata.
#
# Input:
#   missing_tables_df
#   business_key_internal_column
#
# =====================================================


def create_missing_answer_tables(missing_tables_df, business_key_internal_column):
    missing_tables = missing_tables_df.collect()

    if len(missing_tables) == 0:
        log_info("FRAMEWORK", "No Missing Tables Found")

        return

    for row in missing_tables:
        table_name = row["AnswerTableName"]

        datatype = row["TargetDataType"]

        log_info("FRAMEWORK", f"Creating Table : {table_name}")

        import re

        table_only = table_name.split(".")[-1]

        m = re.search(r"(\d+)$", table_only)

        suffix = int(m.group(1)) if m else 0

        start_column = suffix * QA_COLUMN_COUNT + 1
        end_column = start_column + QA_COLUMN_COUNT - 1

        qa_columns = ",\n".join(
            [f"QA{i} {datatype}" for i in range(start_column, end_column + 1)]
        )

        ddl = f"""
        CREATE TABLE IF NOT EXISTS
        {table_name}
        (
            tenant_id INT,

            {business_key_internal_column} INT,

            {qa_columns},

            SystemID INT,

            CreatedOn TIMESTAMP,

            ModifiedOn TIMESTAMP,

            IsDeleted BOOLEAN,

            batch_id BIGINT,

            ingest_ts TIMESTAMP
        )
        USING DELTA
CLUSTER BY (tenant_id, {business_key_internal_column})
 
TBLPROPERTIES (
  'delta.enableDeletionVectors'           = 'true',
  'delta.enableChangeDataFeed'            = 'true',
  'delta.targetFileSize'                  = '268435456',
  'delta.tuneFileSizesForRewrites'        = 'true',
 
  'delta.autoOptimize.optimizeWrite'      = 'true',
  'delta.autoOptimize.autoCompact'        = 'false',
 
  'delta.logRetentionDuration'            = 'interval 7 days',
  'delta.deletedFileRetentionDuration'    = 'interval 3 days'
);
        """

        spark.sql(ddl)

        log_info("FRAMEWORK", f"Created Table : {table_name}")


# =====================================================
# BUILD CHANGED ENTITIES
# =====================================================
#
# Purpose:
#   Identify impacted business keys for the
#   current pipeline batch.
#
# Output:
#
#   <BusinessKeyColumn>
#   tenant_id
#
# =====================================================


def build_changed_entities(entity_config, pipeline_run_id):
    entity_name = entity_config["EntityName"]

    source_table_name = entity_config["SourceTableName"]

    business_key_column = entity_config["BusinessKeyColumn"]

    incident_join_column = entity_config["IncidentJoinColumn"]

    source_join_type = entity_config["SourceJoinType"]

    incident_type_filter = entity_config["IncidentTypeFilter"]

    log_info(entity_name, "Building Changed Entities")

    incident_types = [int(x.strip()) for x in incident_type_filter.split(",")]

    # =================================================
    # INCIDENT FLOW
    # =================================================

    if source_join_type == "INCIDENT_OBJECTID":
        incident_df = (
            spark.table("bronze.EmsEvent_Incident")
            .alias("i")
            .filter(F.col("IncidentTypeID").isin(incident_types))
            .select(F.col("IncidentID"), F.col("tenant_id").alias("incident_tenant_id"))
        )

        fieldvalue_df = (
            spark.table("bronze.supplementalquestions_fieldvalue")
            .alias("fv")
            .filter(F.col("batch_id") == pipeline_run_id)
            .join(
                incident_df,
                [
                    F.col("fv.ObjectID") == F.col("IncidentID"),
                    F.col("fv.tenant_id") == F.col("incident_tenant_id"),
                ],
            )
            .select(
                F.col("fv.ObjectID").alias(business_key_column), F.col("fv.tenant_id")
            )
        )

        singleselect_df = (
            spark.table("bronze.supplementalquestions_fieldvaluesingleselect")
            .alias("fv")
            .filter(F.col("batch_id") == pipeline_run_id)
            .join(
                incident_df,
                [
                    F.col("fv.ObjectID") == F.col("IncidentID"),
                    F.col("fv.tenant_id") == F.col("incident_tenant_id"),
                ],
            )
            .select(
                F.col("fv.ObjectID").alias(business_key_column), F.col("fv.tenant_id")
            )
        )

        multiselect_df = (
            spark.table("bronze.supplementalquestions_fieldvaluemultiselect")
            .alias("fv")
            .filter(F.col("batch_id") == pipeline_run_id)
            .join(
                incident_df,
                [
                    F.col("fv.ObjectID") == F.col("IncidentID"),
                    F.col("fv.tenant_id") == F.col("incident_tenant_id"),
                ],
            )
            .select(
                F.col("fv.ObjectID").alias(business_key_column), F.col("fv.tenant_id")
            )
        )

        changed_df = (
            fieldvalue_df.unionByName(singleselect_df)
            .unionByName(multiselect_df)
            .distinct()
        )

        log_info(entity_name, f"Changed Entities = {changed_df.count()}")

        return changed_df

    # =================================================
    # STANDARD ENTITY FLOW
    # =================================================

    entity_df = spark.table(f"bronze.{source_table_name}")

    incident_df = spark.table("bronze.EmsEvent_Incident").filter(
        F.col("IncidentTypeID").isin(incident_types)
    )

    changed_df = None

    source_tables = [
        "bronze.supplementalquestions_fieldvalue",
        "bronze.supplementalquestions_fieldvaluesingleselect",
        "bronze.supplementalquestions_fieldvaluemultiselect",
    ]

    for table_name in source_tables:
        current_df = (
            spark.table(table_name)
            .filter(F.col("batch_id") == pipeline_run_id)
            .alias("fv")
            .join(
                entity_df.alias("e"),
                [
                    F.col("fv.GlobalIdentifier") == F.col("e.GlobalIdentifier"),
                    F.col("fv.tenant_id") == F.col("e.tenant_id"),
                ],
            )
            .join(
                incident_df.alias("i"),
                [
                    F.col(f"e.{incident_join_column}") == F.col("i.IncidentID"),
                    F.col("e.tenant_id") == F.col("i.tenant_id"),
                ],
            )
            .select(F.col(f"e.{business_key_column}"), F.col("e.tenant_id"))
            .distinct()
        )

        if changed_df is None:
            changed_df = current_df

        else:
            changed_df = changed_df.unionByName(current_df)

    changed_df = changed_df.distinct()

    log_info(entity_name, f"Changed Entities = {changed_df.count()}")

    return changed_df

# COMMAND ----------

def build_multiselect_dataset(entity_config, changed_entities_df):
    entity_name = entity_config["EntityName"]

    source_table_name = entity_config["SourceTableName"]

    business_key_column = entity_config["BusinessKeyColumn"]

    source_join_type = entity_config["SourceJoinType"]

    log_info(entity_name, "Building MultiSelect Dataset")

    # ==========================================
    # INCIDENT
    # ==========================================

    if source_join_type == "INCIDENT_OBJECTID":
        return (
            spark.table("bronze.supplementalquestions_fieldvaluemultiselect")
            .alias("fv")
            .join(
                changed_entities_df.alias("c"),
                [
                    F.col("fv.ObjectID") == F.col(f"c.{business_key_column}"),
                    F.col("fv.tenant_id") == F.col("c.tenant_id"),
                ],
            )
            .filter(F.col("fv.DeletedStatus") == False)
            .filter(F.col("fv.FieldValue") != "")
            .groupBy(
                F.col("fv.ObjectID").alias(business_key_column),
                F.col("fv.FieldDefinitionID"),
                F.col("fv.tenant_id"),
            )
            .agg(
                F.concat_ws(
                    ", ",
                    F.transform(
                        F.sort_array(
                            F.collect_list(F.struct("FieldValueID", "FieldValue"))
                        ),
                        lambda x: x["FieldValue"],
                    ),
                ).alias("FieldValue")
            )
        )

    # ==========================================
    # STANDARD ENTITIES
    # ==========================================

    entity_df = spark.table(f"bronze.{source_table_name}")

    return (
        spark.table("bronze.supplementalquestions_fieldvaluemultiselect")
        .alias("fv")
        .join(
            entity_df.alias("e"),
            [
                F.col("fv.GlobalIdentifier") == F.col("e.GlobalIdentifier"),
                F.col("fv.tenant_id") == F.col("e.tenant_id"),
            ],
        )
        .join(
            changed_entities_df.alias("c"),
            [
                F.col(f"e.{business_key_column}") == F.col(f"c.{business_key_column}"),
                F.col("e.tenant_id") == F.col("c.tenant_id"),
            ],
        )
        .filter(F.col("fv.DeletedStatus") == False)
        .filter(F.col("fv.FieldValue") != "")
        .groupBy(
            F.col(f"e.{business_key_column}"),
            F.col("fv.FieldDefinitionID"),
            F.col("fv.tenant_id"),
        )
        .agg(
            F.concat_ws(
                ", ",
                F.transform(
                    F.sort_array(
                        F.collect_list(F.struct("FieldValueID", "FieldValue"))
                    ),
                    lambda x: x["FieldValue"],
                ),
            ).alias("FieldValue")
        )
    )


def build_singleselect_dataset(entity_config, changed_entities_df):
    entity_name = entity_config["EntityName"]

    source_table_name = entity_config["SourceTableName"]

    business_key_column = entity_config["BusinessKeyColumn"]

    source_join_type = entity_config["SourceJoinType"]

    log_info(entity_name, "Building SingleSelect Dataset")

    # ==========================================
    # INCIDENT
    # ==========================================

    if source_join_type == "INCIDENT_OBJECTID":
        return (
            spark.table("bronze.supplementalquestions_fieldvaluesingleselect")
            .alias("fv")
            .join(
                changed_entities_df.alias("c"),
                [
                    F.col("fv.ObjectID") == F.col(f"c.{business_key_column}"),
                    F.col("fv.tenant_id") == F.col("c.tenant_id"),
                ],
            )
            .filter(F.col("fv.DeletedStatus") == False)
            .select(
                F.col("fv.ObjectID").alias(business_key_column),
                F.col("fv.FieldDefinitionID"),
                F.col("fv.FieldValue"),
                F.col("fv.tenant_id"),
            )
        )

    # ==========================================
    # STANDARD ENTITIES
    # ==========================================

    entity_df = spark.table(f"bronze.{source_table_name}")

    return (
        spark.table("bronze.supplementalquestions_fieldvaluesingleselect")
        .alias("fv")
        .join(
            entity_df.alias("e"),
            [
                F.col("fv.GlobalIdentifier") == F.col("e.GlobalIdentifier"),
                F.col("fv.tenant_id") == F.col("e.tenant_id"),
            ],
        )
        .join(
            changed_entities_df.alias("c"),
            [
                F.col(f"e.{business_key_column}") == F.col(f"c.{business_key_column}"),
                F.col("e.tenant_id") == F.col("c.tenant_id"),
            ],
        )
        .filter(F.col("fv.DeletedStatus") == False)
        .select(
            F.col(f"e.{business_key_column}"),
            F.col("fv.FieldDefinitionID"),
            F.col("fv.FieldValue"),
            F.col("fv.tenant_id"),
        )
    )


def build_generic_dataset(entity_config, changed_entities_df):
    entity_name = entity_config["EntityName"]

    source_table_name = entity_config["SourceTableName"]

    business_key_column = entity_config["BusinessKeyColumn"]

    source_join_type = entity_config["SourceJoinType"]

    log_info(entity_name, "Building Generic Dataset")

    # ==========================================
    # INCIDENT
    # ==========================================

    if source_join_type == "INCIDENT_OBJECTID":
        return (
            spark.table("bronze.supplementalquestions_fieldvalue")
            .alias("fv")
            .join(
                changed_entities_df.alias("c"),
                [
                    F.col("fv.ObjectID") == F.col(f"c.{business_key_column}"),
                    F.col("fv.tenant_id") == F.col("c.tenant_id"),
                ],
            )
            .filter(F.col("fv.DeletedStatus") == False)
            .select(
                F.col("fv.ObjectID").alias(business_key_column),
                F.col("fv.FieldDefinitionID"),
                F.col("fv.FieldValue"),
                F.col("fv.tenant_id"),
            )
        )

    # ==========================================
    # STANDARD ENTITIES
    # ==========================================

    entity_df = spark.table(f"bronze.{source_table_name}")

    return (
        spark.table("bronze.supplementalquestions_fieldvalue")
        .alias("fv")
        .join(
            entity_df.alias("e"),
            [
                F.col("fv.GlobalIdentifier") == F.col("e.GlobalIdentifier"),
                F.col("fv.tenant_id") == F.col("e.tenant_id"),
            ],
        )
        .join(
            changed_entities_df.alias("c"),
            [
                F.col(f"e.{business_key_column}") == F.col(f"c.{business_key_column}"),
                F.col("e.tenant_id") == F.col("c.tenant_id"),
            ],
        )
        .filter(F.col("fv.DeletedStatus") == False)
        .select(
            F.col(f"e.{business_key_column}"),
            F.col("fv.FieldDefinitionID"),
            F.col("fv.FieldValue"),
            F.col("fv.tenant_id"),
        )
    )

# COMMAND ----------

# =====================================================
# CELL-4
# ENTITY EXECUTION PLANNER
# =====================================================

log_info("FRAMEWORK", "Building Entity Execution Plan")

active_entities = [entity for entity in entity_configs if entity["IsActive"] == True]
# =====================================================
# WORKER CALCULATION
# =====================================================

ACTIVE_ENTITY_COUNT = len(active_entities)

CURRENT_WORKERS = min(MAX_WORKERS, max(MIN_WORKERS, ACTIVE_ENTITY_COUNT))

log_info("FRAMEWORK", f"Active Entities = {ACTIVE_ENTITY_COUNT}")

log_info("FRAMEWORK", f"Worker Count = {CURRENT_WORKERS}")
entity_count = len(active_entities)


if entity_count == 0:
    raise Exception("No Active SQ Entities Found")

for entity in active_entities:
    log_info("FRAMEWORK", f"Queued Entity = {entity['EntityName']}")

display(spark.createDataFrame(active_entities))

# COMMAND ----------

# =====================================================
# CELL-5
# EXECUTE PENDING BATCHES
# =====================================================

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

execution_start_ts = datetime.now()

framework_start_time = time.time()

successful_entities = []

failed_entities = []

successful_batches = []

failed_batches = []

# Framework-level audit collection
audit_rows = []

# =====================================================
# PROCESS BATCHES
# =====================================================

for pipeline_run_id in pending_batches:
    log_info("FRAMEWORK", f"Starting Batch = {pipeline_run_id}")

    batch_start_time = time.time()

    try:
        # ==========================================
        # PROCESS ACTIVE ENTITIES
        # ==========================================

        futures = []

        with ThreadPoolExecutor(max_workers=CURRENT_WORKERS) as executor:
            for entity_config in active_entities:
                futures.append(
                    executor.submit(
                        process_entity,
                        entity_config,
                        pipeline_run_id,
                    )
                )

            batch_failed = False

            for future in futures:
                result = future.result()

                entity_name = result["EntityName"]

                audit_rows.extend(result["AuditRows"])

                if result["Status"] == "SUCCESS":
                    successful_entities.append(
                        (
                            pipeline_run_id,
                            entity_name,
                        )
                    )

                else:
                    batch_failed = True

                    failed_entities.append(
                        (
                            pipeline_run_id,
                            entity_name,
                        )
                    )

                    log_error(
                        entity_name,
                        result["ErrorMessage"],
                    )

            if batch_failed:
                raise Exception(
                    f"One Or More Entities Failed For Batch {pipeline_run_id}"
                )

        # ==========================================
        # UPDATE WATERMARK
        # ==========================================

        update_batch_watermark(pipeline_run_id)

        successful_batches.append(pipeline_run_id)

        batch_elapsed = round(time.time() - batch_start_time, 2)

        log_info(
            "FRAMEWORK",
            f"Completed Batch = {pipeline_run_id} in {batch_elapsed} Seconds",
        )

    except Exception as ex:
        failed_batches.append(pipeline_run_id)

        log_error("FRAMEWORK", f"Batch Failed = {pipeline_run_id}")

        log_error("FRAMEWORK", str(ex))

        # ==========================================
        # CONTINUE WITH NEXT BATCH
        # ==========================================

        continue

# =====================================================
# FRAMEWORK SUMMARY
# =====================================================

execution_end_ts = datetime.now()

elapsed_seconds = round(time.time() - framework_start_time, 2)

log_info("FRAMEWORK", f"Completed In {elapsed_seconds} Seconds")

log_info("FRAMEWORK", f"Successful Batches = {len(successful_batches)}")

log_info("FRAMEWORK", f"Failed Batches = {len(failed_batches)}")

log_info("FRAMEWORK", f"Successful Entity Runs = {len(successful_entities)}")

log_info("FRAMEWORK", f"Failed Entity Runs = {len(failed_entities)}")

# =====================================================
# AUDIT PERSISTENCE
# =====================================================

save_execution_details()

save_execution_summary(
    execution_start_ts=execution_start_ts,
    execution_end_ts=execution_end_ts,
    successful_batches=successful_batches,
    failed_batches=failed_batches,
)