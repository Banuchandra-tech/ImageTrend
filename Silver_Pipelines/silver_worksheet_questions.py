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

PIPELINE_NAME = "WorksheetQuestionsFramework"

PIPELINE_VERSION = "1.0"
SILVER_TABLE_REGISTRY = "control.silver_table_registry"

BATCH_WATERMARK_TABLE = "control.bronze_batch_watermark"

PIPELINE_TYPE = "WORKSHEET_QUESTIONS"

# =====================================================
# METADATA CONFIGURATION
# =====================================================

WS_RW_MAPPING_TABLE = "silver.dim_worksheetquestions_rw_mapping"

# =====================================================
# TARGET TABLE CONFIGURATION
# =====================================================

WS_COLUMN_COUNT = 300

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
print("Worksheet QUESTIONS FRAMEWORK PIPELINE STARTED")
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
# CELL 2 - WORKSHEET METADATA LOADING & VALIDATION
# =====================================================
#
# Purpose:
#   Load and validate Worksheet Question
#   RW Mapping metadata.
#
# Responsibilities:
#   - Load RW Mapping metadata
#   - Validate required columns
#   - Validate datatype values
#   - Validate answer table names
#   - Validate WS column assignments
#   - Print metadata summary
#
# Notes:
#   No worksheet processing occurs here.
#   No answer tables are updated here.
#
# =====================================================

print("=" * 100)
print("STEP 1 - LOADING WORKSHEET RW MAPPING")
print("=" * 100)

# =====================================================
# LOAD RW MAPPING
# =====================================================

worksheet_mapping_df = spark.table(WS_RW_MAPPING_TABLE)

mapping_count = worksheet_mapping_df.count()

print(f"Worksheet Mapping Records = {mapping_count}")

if mapping_count == 0:
    raise Exception(f"No Records Found In {WS_RW_MAPPING_TABLE}")

# =====================================================
# REQUIRED COLUMNS
# =====================================================

required_columns = [
    "tenant_id",
    "FieldDefinitionIDInternal",
    "WorksheetIdInternal",
    "SectionIDInternal",
    "AnswerTableName",
    "AnswerColumnName",
    "AnswerColumnNumber",
    "DataType",
]

missing_columns = [c for c in required_columns if c not in worksheet_mapping_df.columns]

if missing_columns:
    raise Exception("Missing Columns : " + ",".join(missing_columns))

print("Column Validation Completed")

# =====================================================
# NULL VALIDATION
# =====================================================

mandatory_columns = [
    "FieldDefinitionIDInternal",
    "WorksheetIdInternal",
    "SectionIDInternal",
    "AnswerTableName",
    "AnswerColumnName",
    "AnswerColumnNumber",
    "DataType",
]

for column_name in mandatory_columns:
    null_count = worksheet_mapping_df.filter(F.col(column_name).isNull()).count()

    if null_count > 0:
        raise Exception(f"Null Values Found In {column_name}")

print("Null Validation Completed")

# =====================================================
# DATATYPE VALIDATION
# =====================================================

invalid_datatypes = worksheet_mapping_df.filter(
    ~F.col("DataType").isin(SUPPORTED_DATA_TYPES)
)

if invalid_datatypes.count() > 0:
    display(invalid_datatypes.select("FieldDefinitionIDInternal", "DataType"))

    raise Exception("Invalid DataType Found")

print("DataType Validation Completed")

# =====================================================
# ANSWER COLUMN VALIDATION
# =====================================================

invalid_columns = worksheet_mapping_df.filter(
    F.regexp_extract(F.col("AnswerColumnName"), r"([0-9]+)$", 1).cast("int")
    != F.col("AnswerColumnNumber")
)

if invalid_columns.count() > 0:
    display(invalid_columns)

    raise Exception("Invalid WS Column Assignment Found")

print("WS Column Validation Completed")

# =====================================================
# DUPLICATE MAPPING CHECK
# =====================================================

duplicate_mappings = (
    worksheet_mapping_df.groupBy(
        "tenant_id",
        "FieldDefinitionIDInternal",
        "WorksheetIdInternal",
        "SectionIDInternal",
    )
    .count()
    .filter(F.col("count") > 1)
)

if duplicate_mappings.count() > 0:
    display(duplicate_mappings)

    raise Exception("Duplicate RW Mapping Found")

print("Duplicate Validation Completed")

# =====================================================
# ACTIVE MAPPINGS
# =====================================================

active_mapping_df = worksheet_mapping_df

mapping_count = active_mapping_df.count()

print(f"Worksheet Mapping Records = {mapping_count}")

# =====================================================
# METADATA SUMMARY
# =====================================================

print("=" * 100)
print("WORKSHEET RW MAPPING SUMMARY")
print("=" * 100)

display(active_mapping_df.orderBy("AnswerTableName", "AnswerColumnNumber"))

print("=" * 100)
print("METADATA VALIDATION COMPLETED")
print("=" * 100)

# COMMAND ----------

# DBTITLE 1,reusable framework functions
# =====================================================
# LOGGING HELPERS
# =====================================================


def log_info(component, message):
    print(f"[INFO][{component}] {message}")


def log_warning(component, message):
    print(f"[WARNING][{component}] {message}")


def log_error(component, message):
    print(f"[ERROR][{component}] {message}")


# =====================================================
# NORMALIZE FIELD VALUE
# =====================================================


def normalize_fieldvalue(dataframe):
    return dataframe.withColumn(
        "FieldValue",
        F.when(F.trim(F.col("FieldValue")) == "", None).otherwise(F.col("FieldValue")),
    )


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
# WORKSHEET PIVOT BUILDER
# =====================================================
def build_ws_pivot_dataframe(
    dataframe, target_datatype, answer_table_name, pipeline_run_id
):
    if not dataframe.take(1):
        log_warning("PIVOT", "No Records Available")
        return None

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

    elif target_datatype == "DECIMAL":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue as decimal(18,8))")
        )

    else:
        working_df = dataframe

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * WS_COLUMN_COUNT + 1

    end_column = start_column + WS_COLUMN_COUNT - 1

    ws_columns = [
        f"WS{i}"
        for i in range(start_column, end_column + 1)
    ]

    pivot_df = (
        working_df.groupBy(
            "tenant_id",
            "IncidentIDInternal",
            "WorksheetInstanceID",
            "WorksheetDateTime",
            "PerformerID",
            "WorksheetName",
            "WorksheetIdInternal",
            "WorksheetInstanceCrewMember",
        )
        .pivot("AnswerColumnName", ws_columns)
        .agg(F.max("FieldValue"))
    )

    pivot_df = (
        pivot_df.withColumn("SystemID", F.lit(0))
        .withColumnRenamed("PerformerID", "WorksheetInstancePerformerIDInternal")
        .withColumn("CreatedOn", F.current_timestamp())
        .withColumn("ModifiedOn", F.current_timestamp())
        .withColumn("batch_id", F.lit(int(pipeline_run_id)))
        .withColumn("ingest_ts", F.current_timestamp())
    )

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
        F.lower(F.col("silver_table")) == "worksheet_questions"
    )

    registry_rows = registry_df.collect()

    if len(registry_rows) == 0:
        raise Exception("Registry Entry Not Found For worksheet_questions")

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
          = 'worksheet_questions'
    """

    spark.sql(update_sql)

    log_info("FRAMEWORK", f"Watermark Updated = {batch_id}")


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

    summary_status = "FAILED" if len(failed_batches) > 0 else "SUCCESS"

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

# =====================================================
# WORKSHEET FRAMEWORK FIXES
# =====================================================
#
# Purpose:
#   Consolidated fixes identified during
#   worksheet framework review.
#
# Includes:
#   1. build_worksheet_views()
#   2. build_ws_multiselect_dataset()
#   3. build_ws_generic_dataset()
#   4. build_final_ws_dataset()
#
# Notes:
#   This cell REPLACES existing versions
#   of these functions.
#
# =====================================================

# =====================================================
# BUILD WORKSHEET MULTISELECT DATASET
# =====================================================


def build_ws_multiselect_dataset():
    return (
        spark.table("bronze.worksheet_fieldvaluemultiselect")
        .alias("fv")
        .join(
            spark.table("bronze.worksheet_worksheetinstance").alias("wi"),
            [
                F.col("fv.WorksheetInstanceID") == F.col("wi.WorksheetInstanceID"),
                F.col("fv.tenant_id") == F.col("wi.tenant_id"),
            ],
        )
        .join(
            spark.table("ChangedWorksheetInstances").alias("c"),
            [
                F.col("wi.WorksheetInstanceID") == F.col("c.WorksheetInstanceID"),
                F.col("wi.tenant_id") == F.col("c.tenant_id"),
            ],
        )
        .filter(F.col("fv.DeletedStatus") == False)
        .filter(F.col("fv.FieldValue") != "")
        .groupBy(
            F.col("wi.WorksheetInstanceID"),
            F.col("wi.ObjectID").alias("IncidentIDInternal"),
            F.col("fv.FieldDefinitionID"),
            F.col("fv.tenant_id"),
        )
        .agg(
            F.concat_ws(", ", F.sort_array(F.collect_set("FieldValue"))).alias(
                "FieldValue"
            )
        )
    )


# =====================================================
# BUILD WORKSHEET GENERIC DATASET
# =====================================================


def build_ws_generic_dataset():
    return (
        spark.table("bronze.worksheet_fieldvalue")
        .alias("fv")
        .join(
            spark.table("bronze.worksheet_worksheetinstance").alias("wi"),
            [
                F.col("fv.WorksheetInstanceID") == F.col("wi.WorksheetInstanceID"),
                F.col("fv.tenant_id") == F.col("wi.tenant_id"),
            ],
        )
        .join(
            spark.table("ChangedWorksheetInstances").alias("c"),
            [
                F.col("wi.WorksheetInstanceID") == F.col("c.WorksheetInstanceID"),
                F.col("wi.tenant_id") == F.col("c.tenant_id"),
            ],
        )
        .filter(F.col("fv.DeletedStatus") == False)
        .select(
            F.col("wi.WorksheetInstanceID"),
            F.col("wi.ObjectID").alias("IncidentIDInternal"),
            F.col("fv.FieldDefinitionID"),
            F.col("fv.FieldValue"),
            F.col("fv.tenant_id"),
        )
    )


rw_mapping_df = spark.table(WS_RW_MAPPING_TABLE).persist()
# =====================================================
# BUILD WORKSHEET VIEWS
# =====================================================


def build_worksheet_views(pipeline_run_id):
    # =================================================
    # STEP 0
    # ChangedWorksheetInstances
    # =================================================

    changed_multiselect = (
        spark.table("bronze.worksheet_fieldvaluemultiselect")
        .alias("fv")
        .join(
            spark.table("bronze.worksheet_worksheetinstance").alias("wi"),
            [
                F.col("fv.WorksheetInstanceID") == F.col("wi.WorksheetInstanceID"),
                F.col("fv.tenant_id") == F.col("wi.tenant_id"),
            ],
        )
        .filter(F.col("fv.batch_id") == pipeline_run_id)
        .select(
            F.col("wi.WorksheetInstanceID"),
            F.col("wi.ObjectID").alias("IncidentIDInternal"),
            F.col("wi.tenant_id"),
        )
        .distinct()
    )

    changed_generic = (
        spark.table("bronze.worksheet_fieldvalue")
        .alias("fv")
        .join(
            spark.table("bronze.worksheet_worksheetinstance").alias("wi"),
            [
                F.col("fv.WorksheetInstanceID") == F.col("wi.WorksheetInstanceID"),
                F.col("fv.tenant_id") == F.col("wi.tenant_id"),
            ],
        )
        .filter(F.col("fv.batch_id") == pipeline_run_id)
        .select(
            F.col("wi.WorksheetInstanceID"),
            F.col("wi.ObjectID").alias("IncidentIDInternal"),
            F.col("wi.tenant_id"),
        )
        .distinct()
    )

    (
        changed_multiselect.unionByName(changed_generic)
        .distinct()
        .createOrReplaceTempView("ChangedWorksheetInstances")
    )

    # =================================================
    # STEP 1
    # WorksheetFieldValueMultiSelectAgg
    # =================================================

    (
        build_ws_multiselect_dataset().createOrReplaceTempView(
            "WorksheetFieldValueMultiSelectAgg"
        )
    )

    # =================================================
    # STEP 2
    # WorksheetFieldValueGeneric
    # =================================================

    (build_ws_generic_dataset().createOrReplaceTempView("WorksheetFieldValueGeneric"))

    # =================================================
    # STEP 3
    # WorksheetControlMapping
    # =================================================

    (
        spark.table("bronze.worksheet_fielddefinition")
        .filter(F.col("DeletedStatus") == False)
        .select(
            "FieldDefinitionID",
            "SectionID",
            F.when(F.col("WorksheetControlTypeID") == 11, F.lit("MultiSelect"))
            .otherwise(F.lit("Generic"))
            .alias("ValueType"),
            "tenant_id",
        )
        .createOrReplaceTempView("WorksheetControlMapping")
    )

    # =================================================
    # STEP 4
    # WorksheetFieldValues
    # =================================================

    multiselect_df = (
        spark.table("WorksheetControlMapping")
        .alias("c")
        .join(
            spark.table("WorksheetFieldValueMultiSelectAgg").alias("m"),
            [
                F.col("c.FieldDefinitionID") == F.col("m.FieldDefinitionID"),
                F.col("c.tenant_id") == F.col("m.tenant_id"),
            ],
        )
        .filter(F.col("c.ValueType") == "MultiSelect")
        .select(
            "m.WorksheetInstanceID",
            "m.IncidentIDInternal",
            "m.FieldDefinitionID",
            "c.SectionID",
            "m.FieldValue",
            "m.tenant_id",
        )
    )

    generic_df = (
        spark.table("WorksheetControlMapping")
        .alias("c")
        .join(
            spark.table("WorksheetFieldValueGeneric").alias("g"),
            [
                F.col("c.FieldDefinitionID") == F.col("g.FieldDefinitionID"),
                F.col("c.tenant_id") == F.col("g.tenant_id"),
            ],
        )
        .filter(F.col("c.ValueType") == "Generic")
        .select(
            "g.WorksheetInstanceID",
            "g.IncidentIDInternal",
            "g.FieldDefinitionID",
            "c.SectionID",
            "g.FieldValue",
            "g.tenant_id",
        )
    )

    (
        multiselect_df.unionByName(generic_df).createOrReplaceTempView(
            "WorksheetFieldValues"
        )
    )

    # =================================================
    # STEP 5
    # FinalWorksheetDataset
    # =================================================

    (
        spark.table("WorksheetFieldValues")
        .alias("fv")
        .join(
            spark.table("bronze.worksheet_worksheetinstance").alias("wi"),
            [
                F.col("fv.WorksheetInstanceID") == F.col("wi.WorksheetInstanceID"),
                F.col("fv.tenant_id") == F.col("wi.tenant_id"),
            ],
        )
        .join(
            rw_mapping_df.alias("rw"),
            [
                F.col("fv.FieldDefinitionID") == F.col("rw.FieldDefinitionIDInternal"),
                F.col("fv.SectionID") == F.col("rw.SectionIDInternal"),
                F.col("fv.tenant_id") == F.col("rw.tenant_id"),
            ],
        )
        .join(
            spark.table("silver.dim_personnel").alias("p"),
            [
                F.col("wi.PerformerID") == F.col("p.PersonnelPerformerIDInternal"),
                F.col("wi.tenant_id") == F.col("p.tenant_id"),
            ],
            "left",
        )
        .select(
            F.col("fv.tenant_id"),
            F.col("fv.IncidentIDInternal"),
            F.col("fv.WorksheetInstanceID"),
            F.col("wi.WorksheetDateTime"),
            F.col("wi.PerformerID"),
            F.col("rw.WorksheetIdInternal"),
            F.col("rw.WorksheetName"),
            F.col("rw.AnswerTableName"),
            F.col("rw.AnswerColumnName"),
            F.col("rw.AnswerColumnNumber"),
            F.col("rw.DataType"),
            F.col("fv.FieldValue"),
            F.col("p.PersonnelFullName").alias("WorksheetInstanceCrewMember"),
        )
        .createOrReplaceTempView("FinalWorksheetDataset")
    )


# =====================================================
# BUILD FINAL WORKSHEET DATASET
# =====================================================


def build_final_ws_dataset(pipeline_run_id):
    build_worksheet_views(pipeline_run_id)
    final_df = spark.table("FinalWorksheetDataset")

    final_df.cache()

    final_df.count()

    return final_df

# COMMAND ----------

# =====================================================
# PROCESS ANSWER TABLE
# =====================================================


def process_answer_table(answer_table_name, pipeline_run_id):
    try:
        log_info(answer_table_name, "Started")

        answer_df = spark.table("FinalWorksheetDataset").filter(
            F.lower(F.col("AnswerTableName")) == answer_table_name.lower()
        )

        if not answer_df.take(1):
            log_warning(answer_table_name, "No Records Found")

            return {
                "AnswerTableName": answer_table_name,
                "Status": "SUCCESS",
                "RowsProcessed": 0,
                "ErrorMessage": None,
            }

        datatype = answer_df.select("DataType").distinct().first()[0]

        pivot_df = build_ws_pivot_dataframe(
            answer_df, datatype, answer_table_name, pipeline_run_id
        )

        if pivot_df is None:
            return {
                "AnswerTableName": answer_table_name,
                "Status": "SUCCESS",
                "RowsProcessed": 0,
                "ErrorMessage": None,
            }
        source_count = answer_df.count()
        pivot_count = pivot_df.count()

        audit_record = merge_ws_answer_table(
            answer_table_name, pivot_df, pipeline_run_id
        )

        row_count = pivot_count

        log_info(answer_table_name, f"Completed Rows={row_count}")

        return {
            "AnswerTableName": answer_table_name,
            "Status": "SUCCESS",
            "RowsProcessed": row_count,
            "AuditRows": [audit_record],
            "ErrorMessage": None,
        }

    except Exception as ex:
        log_error(answer_table_name, str(ex))
        failure_audit = build_audit_row(
            table_name=answer_table_name,
            batch_id=pipeline_run_id,
            status="FAILED",
            start_ts=datetime.now(),
            end_ts=datetime.now(),
            error_message=str(ex)[:2000],
        )

        return {
            "AnswerTableName": answer_table_name,
            "Status": "FAILED",
            "RowsProcessed": 0,
            "AuditRows": [failure_audit],
            "ErrorMessage": str(ex),
        }

# COMMAND ----------

# =====================================================
# LOAD WORKSHEET ANSWER TABLES
# =====================================================


def load_ws_answer_table_metadata():
    log_info("FRAMEWORK", "Loading Worksheet Answer Tables")

    answer_tables_df = (
        spark.table(WS_RW_MAPPING_TABLE)
        .filter(F.upper(F.col("DataType")).isin(SUPPORTED_DATA_TYPES))
        .select(
            F.lower(F.col("AnswerTableName")).alias("AnswerTableName"),
            F.when(F.upper(F.col("DataType")) == "STRING", "STRING")
            .when(F.upper(F.col("DataType")) == "DATE", "DATE")
            .when(F.upper(F.col("DataType")) == "TIMESTAMP", "TIMESTAMP")
            .when(F.upper(F.col("DataType")) == "INT", "INT")
            .when(F.upper(F.col("DataType")) == "DECIMAL", "DECIMAL")
            .alias("TargetDataType"),
        )
        .distinct()
    )

    answer_table_count = answer_tables_df.count()

    log_info("FRAMEWORK", f"Worksheet Answer Tables Found = {answer_table_count}")

    return answer_tables_df


# =====================================================
# MERGE WORKSHEET ANSWER TABLE
# =====================================================


def merge_ws_answer_table(answer_table_name, pivot_df, pipeline_batch_id):
    if pivot_df is None:
        return None

    if not pivot_df.take(1):
        return None

    temp_view_name = "CurrentWorksheetPivot_" + answer_table_name.replace(".", "_")

    pivot_df.createOrReplaceTempView(temp_view_name)

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * WS_COLUMN_COUNT + 1

    end_column = start_column + WS_COLUMN_COUNT - 1

    ws_columns = [f"WS{i}" for i in range(start_column, end_column + 1)]

    update_columns = [f"target.{c}=source.{c}" for c in ws_columns]

    update_columns.extend(
        [
            "target.ModifiedOn=current_timestamp()",
            f"target.batch_id={pipeline_batch_id}",
            "target.ingest_ts=current_timestamp()",
            "target.WorksheetInstanceCrewMember = source.WorksheetInstanceCrewMember",
        ]
    )

    update_set = ",\n".join(update_columns)

    insert_columns = [
        "tenant_id",
        "IncidentIDInternal",
        "WorksheetInstanceID",
        "WorksheetDateTime",
    ]

    insert_columns.extend(ws_columns)

    insert_columns.extend(
        [
            "SystemID",
            "CreatedOn",
            "ModifiedOn",
            "WorksheetIdInternal",
            "WorksheetName",
            "WorksheetInstancePerformerIDInternal",
            "WorksheetInstanceCrewMember",
            "batch_id",
            "ingest_ts",
        ]
    )

    insert_values = [
        "source.tenant_id",
        "source.IncidentIDInternal",
        "source.WorksheetInstanceID",
        "source.WorksheetDateTime",
    ]

    insert_values.extend([f"source.{c}" for c in ws_columns])

    insert_values.extend(
        [
            "source.SystemID",
            "source.CreatedOn",
            "source.ModifiedOn",
            "source.WorksheetIdInternal",
            "source.WorksheetName",
            "source.WorksheetInstancePerformerIDInternal",
            "source.WorksheetInstanceCrewMember",
            "source.batch_id",
            "source.ingest_ts",
        ]
    )

    merge_sql = f"""
    MERGE INTO {answer_table_name} target

    USING {temp_view_name} source

    ON target.tenant_id = source.tenant_id
    AND target.WorksheetInstanceID =
        source.WorksheetInstanceID

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
        log_info(answer_table_name, "Executing Merge")

        merge_result = spark.sql(merge_sql)

        inserted_rows = 0
        updated_rows = 0
        deleted_rows = 0

        try:
            metric_row = merge_result.first()

            if metric_row:
                inserted_rows = getattr(metric_row, "num_inserted_rows", 0) or 0

                updated_rows = getattr(metric_row, "num_updated_rows", 0) or 0

                deleted_rows = getattr(metric_row, "num_deleted_rows", 0) or 0

        except Exception as metric_ex:
            log_warning(
                answer_table_name, f"Unable To Read Merge Metrics : {str(metric_ex)}"
            )

        table_end_ts = datetime.now()

        log_info(
            answer_table_name,
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

        log_error(answer_table_name, f"Merge Failed : {str(ex)}")

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

# COMMAND ----------

# =====================================================
# CREATE MISSING WORKSHEET ANSWER TABLES
# =====================================================


def create_missing_ws_answer_tables(missing_tables_df):
    missing_tables = missing_tables_df.collect()

    if len(missing_tables) == 0:
        log_info("FRAMEWORK", "No Missing Worksheet Tables Found")

        return

    for row in missing_tables:
        table_name = row["AnswerTableName"]

        datatype = row["TargetDataType"]

        import re

        table_only = table_name.split(".")[-1]

        m = re.search(r"(\d+)$", table_only)

        suffix = int(m.group(1)) if m else 0

        start_column = suffix * WS_COLUMN_COUNT + 1

        end_column = start_column + WS_COLUMN_COUNT - 1

        ws_columns = ",\n".join(
            [f"WS{i} {datatype}" for i in range(start_column, end_column + 1)]
        )

        ddl = f"""
        CREATE TABLE IF NOT EXISTS
        {table_name}
        (
            tenant_id INT,

            IncidentIDInternal INT,

            WorksheetInstanceID INT,

            WorksheetDateTime TIMESTAMP,

            {ws_columns},

            SystemID INT,

            CreatedOn TIMESTAMP,

            ModifiedOn TIMESTAMP,

            WorksheetIdInternal INT,

            WorksheetName STRING,

            WorksheetInstancePerformerIDInternal  STRING,
            WorksheetInstanceCrewMember STRING,

            batch_id BIGINT,

            ingest_ts TIMESTAMP
        )
        USING DELTA

        CLUSTER BY
        (
            tenant_id,
            IncidentIDInternal
        )
        """

        spark.sql(ddl)

        log_info("FRAMEWORK", f"Created Table = {table_name}")


# =====================================================
# FIND MISSING TABLES
# =====================================================


def find_missing_ws_tables(answer_tables_df):
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

    return (
        answer_tables_df.alias("a")
        .join(
            existing_tables_df.alias("e"),
            F.lower(F.col("a.AnswerTableName")) == F.lower(F.col("e.TableName")),
            "left",
        )
        .filter(F.col("e.TableName").isNull())
        .select("a.AnswerTableName", "a.TargetDataType")
    )

# COMMAND ----------

answer_tables_df = load_ws_answer_table_metadata()

missing_tables_df = find_missing_ws_tables(answer_tables_df)

create_missing_ws_answer_tables(missing_tables_df)

# COMMAND ----------

# =====================================================
# ANSWER TABLE EXECUTION PLANNER
# =====================================================

log_info("FRAMEWORK", "Building Worksheet Execution Plan")

# =====================================================
# LOAD ACTIVE ANSWER TABLES
# =====================================================

answer_table_df = (
    spark.table(WS_RW_MAPPING_TABLE)
    .select("AnswerTableName")
    .distinct()
    .orderBy("AnswerTableName")
)

active_answer_tables = [row["AnswerTableName"] for row in answer_table_df.collect()]

# =====================================================
# VALIDATION
# =====================================================

table_count = len(active_answer_tables)

if table_count == 0:
    raise Exception(f"No Answer Tables Found In {WS_RW_MAPPING_TABLE}")

# =====================================================
# WORKER CALCULATION
# =====================================================

CURRENT_WORKERS = min(MAX_WORKERS, max(MIN_WORKERS, table_count))

# =====================================================
# LOGGING
# =====================================================

log_info("FRAMEWORK", f"Active Answer Tables = {table_count}")

log_info("FRAMEWORK", f"Worker Count = {CURRENT_WORKERS}")

for table_name in active_answer_tables:
    log_info("FRAMEWORK", f"Queued Table = {table_name}")

# =====================================================
# DISPLAY EXECUTION PLAN
# =====================================================

display(answer_table_df)

print("=" * 100)
print("WORKSHEET EXECUTION PLAN READY")
print("=" * 100)

# COMMAND ----------

# =====================================================
# EXECUTE PENDING BATCHES
# =====================================================

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

execution_start_ts = datetime.now()

framework_start_time = time.time()

successful_tables = []

failed_tables = []

successful_batches = []

failed_batches = []

audit_rows = []

# =====================================================
# PROCESS BATCHES
# =====================================================

for pipeline_run_id in pending_batches:

    log_info(
        "FRAMEWORK",
        f"Starting Batch = {pipeline_run_id}"
    )

    batch_start_time = time.time()

    try:

        # ==========================================
        # BUILD DATASET ONCE PER BATCH
        # ==========================================

        log_info(
            "FRAMEWORK",
            f"Building FinalWorksheetDataset For Batch {pipeline_run_id}"
        )

        build_final_ws_dataset(
            pipeline_run_id
        )

        log_info(
            "FRAMEWORK",
            "FinalWorksheetDataset Ready"
        )

        # ==========================================
        # PROCESS ANSWER TABLES IN PARALLEL
        # ==========================================

        futures = []

        with ThreadPoolExecutor(
            max_workers=CURRENT_WORKERS
        ) as executor:

            for answer_table_name in active_answer_tables:

                futures.append(
                    executor.submit(
                        process_answer_table,
                        answer_table_name,
                        pipeline_run_id
                    )
                )

            batch_failed = False

            for future in futures:

                result = future.result()

                answer_table_name = result["AnswerTableName"]

                audit_rows.extend(
                    result.get("AuditRows", [])
                )

                if result["Status"] == "SUCCESS":

                    successful_tables.append(
                        (
                            pipeline_run_id,
                            answer_table_name
                        )
                    )

                else:

                    batch_failed = True

                    failed_tables.append(
                        (
                            pipeline_run_id,
                            answer_table_name
                        )
                    )

                    log_error(
                        answer_table_name,
                        result["ErrorMessage"]
                    )

            if batch_failed:

                raise Exception(
                    f"One Or More Answer Tables Failed For Batch {pipeline_run_id}"
                )

        # ==========================================
        # UPDATE WATERMARK
        # ==========================================

        update_batch_watermark(
            pipeline_run_id
        )

        successful_batches.append(
            pipeline_run_id
        )

        batch_elapsed = round(
            time.time() - batch_start_time,
            2
        )

        log_info(
            "FRAMEWORK",
            f"Completed Batch = {pipeline_run_id} "
            f"in {batch_elapsed} Seconds"
        )

    except Exception as ex:

        failed_batches.append(
            pipeline_run_id
        )

        log_error(
            "FRAMEWORK",
            f"Batch Failed = {pipeline_run_id}"
        )

        log_error(
            "FRAMEWORK",
            str(ex)
        )

        continue

# =====================================================
# FRAMEWORK SUMMARY
# =====================================================

execution_end_ts = datetime.now()

elapsed_seconds = round(
    time.time() - framework_start_time,
    2
)

log_info(
    "FRAMEWORK",
    f"Completed In {elapsed_seconds} Seconds"
)

log_info(
    "FRAMEWORK",
    f"Successful Batches = {len(successful_batches)}"
)

log_info(
    "FRAMEWORK",
    f"Failed Batches = {len(failed_batches)}"
)

log_info(
    "FRAMEWORK",
    f"Successful Table Runs = {len(successful_tables)}"
)

log_info(
    "FRAMEWORK",
    f"Failed Table Runs = {len(failed_tables)}"
)

# =====================================================
# AUDIT PERSISTENCE
# =====================================================

save_execution_details()

save_execution_summary(
    execution_start_ts=execution_start_ts,
    execution_end_ts=execution_end_ts,
    successful_batches=successful_batches,
    failed_batches=failed_batches
)