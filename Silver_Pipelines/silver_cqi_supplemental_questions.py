# Databricks notebook source
# Databricks notebook source
# DBTITLE 1,CONFIGURATION & PARAMETERS
# =====================================================
# CELL 1 - CONFIGURATION & PARAMETERS
# =====================================================
#
# Purpose:
#   Initializes runtime configuration for the
#   CQI Supplemental Questions Framework Pipeline.
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
#   No CQI processing occurs in this cell.
#
# =====================================================

"""
CQI Supplemental Questions Framework Pipeline

Purpose:
    Execute CQI Supplemental Question processing
    using a metadata-driven framework.

Features:

    - Metadata Driven Processing
    - Dynamic Answer Table Discovery
    - Dynamic Overflow Table Creation
    - Dynamic Pivot Generation
    - Dynamic Merge Processing
    - Parallel Answer Table Execution
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
        Batch Discovery
              |
              v
        Build CQI Dataset
              |
              v
        Pivot Answer Tables
              |
              v
        Merge Answer Tables
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

PIPELINE_NAME = "CQISupplementalQuestionsFramework"

PIPELINE_VERSION = "1.0"

PIPELINE_TYPE = "CQI_SUPPLEMENTAL_QUESTIONS"

SILVER_TABLE_REGISTRY = "control.silver_table_registry"

BATCH_WATERMARK_TABLE = "control.bronze_batch_watermark"

# =====================================================
# METADATA CONFIGURATION
# =====================================================

CQI_RW_MAPPING_TABLE = "silver.dim_cqi_supplemental_questions_rw_mapping"

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

CURRENT_WORKERS = BASE_WORKERS

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
print("CQI SUPPLEMENTAL QUESTIONS FRAMEWORK PIPELINE STARTED")
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
# CELL 2 - CQI RW MAPPING VALIDATION
# =====================================================
#
# Purpose:
#   Load and validate CQI RW Mapping metadata
#   before execution begins.
#
# Responsibilities:
#   - Load RW Mapping
#   - Validate mandatory columns
#   - Validate supported datatypes
#   - Validate duplicate mappings
#   - Print metadata summary
#
# Notes:
#   No CQI processing occurs in this cell.
#   No source tables are read.
#   No answer tables are updated.
#
# =====================================================

print("=" * 100)
print("STEP 1 - LOADING CQI RW MAPPING")
print("=" * 100)

# =====================================================
# LOAD RW MAPPING
# =====================================================

rw_mapping_df = spark.table(CQI_RW_MAPPING_TABLE).persist()

mapping_count = rw_mapping_df.count()

print(f"RW Mapping Records = {mapping_count}")

if mapping_count == 0:
    raise Exception(f"No Records Found In {CQI_RW_MAPPING_TABLE}")

# =====================================================
# VALIDATE REQUIRED COLUMNS
# =====================================================

required_columns = [
    "tenant_id",
    "CQICategoryIDInternal",
    "FieldDefinitionIDInternal",
    "AnswerTableName",
    "AnswerColumnName",
    "AnswerColumnNumber",
    "DataType",
]

missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in rw_mapping_df.columns
]

if missing_columns:
    raise Exception("Missing RW Mapping Columns : " + ",".join(missing_columns))

print("Required Column Validation Completed")

# =====================================================
# VALIDATE NULLS
# =====================================================

mandatory_columns = [
    "CQICategoryIDInternal",
    "FieldDefinitionIDInternal",
    "AnswerTableName",
    "AnswerColumnName",
    "DataType",
]

for column_name in mandatory_columns:
    null_count = rw_mapping_df.filter(F.col(column_name).isNull()).count()

    if null_count > 0:
        raise Exception(f"Null Values Found In {column_name}")

print("Mandatory Column Validation Completed")

# =====================================================
# VALIDATE DATATYPES
# =====================================================

invalid_datatypes = rw_mapping_df.filter(
    ~F.upper(F.col("DataType")).isin(SUPPORTED_DATA_TYPES)
)

invalid_count = invalid_datatypes.count()

if invalid_count > 0:
    display(invalid_datatypes)

    raise Exception("Unsupported DataTypes Found")

print("Datatype Validation Completed")

# =====================================================
# VALIDATE DUPLICATE MAPPINGS
# =====================================================

duplicate_mapping = (
    rw_mapping_df.groupBy(
        "tenant_id", "CQICategoryIDInternal", "FieldDefinitionIDInternal"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_mapping.count()

if duplicate_count > 0:
    display(duplicate_mapping)

    raise Exception("Duplicate RW Mapping Found")

print("Duplicate Mapping Validation Completed")

# =====================================================
# VALIDATE ANSWER COLUMN RANGE
# =====================================================

invalid_slots = rw_mapping_df.filter(
    F.regexp_extract(F.col("AnswerColumnName"), r"([0-9]+)$", 1).cast("int")
    != F.col("AnswerColumnNumber")
)

if invalid_slots.count() > 0:
    display(invalid_slots)
    raise Exception("AnswerColumnName and AnswerColumnNumber do not match")

print("Answer Column Validation Completed")

# =====================================================
# METADATA SUMMARY
# =====================================================

print("=" * 100)
print("CQI RW MAPPING SUMMARY")
print("=" * 100)

display(
    rw_mapping_df.groupBy("AnswerTableName", "DataType")
    .count()
    .orderBy("AnswerTableName", "DataType")
)

print("=" * 100)
print("RW MAPPING VALIDATION COMPLETED")
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


# =====================================================
# PIVOT DATAFRAME BUILDER
# =====================================================
#
# Purpose:
#   Convert row-based CQI answers into
#   QA1-QA300 pivot structure.
#
# One row per:
#
#   tenant_id
#   IncidentIDInternal
#   CQICategoryIDInternal
#   CQIReviewerIDInternal
#
# =====================================================


def build_pivot_dataframe(
    dataframe, target_datatype, answer_table_name, pipeline_run_id
):
    if dataframe.limit(1).count() == 0:
        log_warning("PIVOT", "No Records Available")

        return None

    dataframe = normalize_fieldvalue(dataframe)

    # ==========================================
    # APPLY DATATYPE CONVERSION
    # ==========================================

    if target_datatype == "DATE":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue AS DATE)")
        )

    elif target_datatype == "TIMESTAMP":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue AS TIMESTAMP)")
        )

    elif target_datatype == "INT":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue AS INT)")
        )

    elif target_datatype == "DECIMAL":
        working_df = dataframe.withColumn(
            "FieldValue", F.expr("try_cast(FieldValue AS DECIMAL(18,8))")
        )

    else:
        working_df = dataframe

    # ==========================================
    # BUILD QA1-QA300 PIVOT
    # ==========================================

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * QA_COLUMN_COUNT + 1

    end_column = start_column + QA_COLUMN_COUNT - 1

    qa_columns = [f"QA{i}" for i in range(start_column, end_column + 1)]

    pivot_df = (
        working_df.groupBy(
            "tenant_id",
            "IncidentIDInternal",
            "CQICategoryIDInternal",
            "CQIReviewerIDInternal",
        )
        .pivot("AnswerColumnName", qa_columns)
        .agg(F.max("FieldValue"))
    )

    # ==========================================
    # AUDIT COLUMNS
    # ==========================================

    pivot_df = (
        pivot_df.withColumn("SystemID", F.lit(0))
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
        F.lower(F.col("silver_table")) == "cqi_supplemental_questions"
    )

    registry_rows = registry_df.collect()

    if len(registry_rows) == 0:
        raise Exception("Registry Entry Not Found For CQI supplemental_questions")

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
          = 'cqi_supplemental_questions'
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

# =====================================================
# BUILD FINAL CQI DATASET
# =====================================================
#
# Purpose:
#   Build FinalCQIDataset for the current batch.
#
# Output:
#
#   tenant_id
#   IncidentIDInternal
#   CQICategoryIDInternal
#   CQIReviewerIDInternal
#   AnswerTableName
#   AnswerColumnName
#   DataType
#   FieldValue
#   ModifiedOn
#
# =====================================================


def build_final_cqi_dataset(pipeline_run_id):
    log_info("FRAMEWORK", "Building Final CQI Dataset")

    # =================================================
    # CQI RESPONSES
    # =================================================

    spark.sql(
        f"""

CREATE OR REPLACE TEMP VIEW CQIResponses AS

SELECT

    c.tenant_id,

    i.IncidentID                            AS IncidentIDInternal,

    c.CategoryID                            AS CQICategoryIDInternal,

    er.PerformerID                          AS CQIReviewerIDInternal,

    fd.FieldDefinitionID                    AS FieldDefinitionIDInternal,

    err.Response                            AS FieldValue,

    err.ModifiedOn 

FROM bronze.cqi_category c

INNER JOIN bronze.resource_agency a
    ON c.OwnerAgencyID = a.AgencyID
   AND c.tenant_id = a.tenant_id

INNER JOIN bronze.resource_incidenttype it
    ON c.IncidentTypeID = it.IncidentTypeID
   AND c.tenant_id = it.tenant_id

LEFT JOIN bronze.cqi_categoryquestion cq
    ON c.CategoryID = cq.CategoryID
   AND c.tenant_id = cq.tenant_id

LEFT JOIN bronze.supplementalquestions_fielddefinition fd
    ON cq.QuestionID = fd.FieldDefinitionID
   AND cq.tenant_id = fd.tenant_id

LEFT JOIN bronze.cqi_emsreview er
    ON c.CategoryID = er.CategoryID
   AND c.tenant_id = er.tenant_id

LEFT JOIN bronze.emsevent_incident i
    ON er.IncidentID = i.IncidentID
   AND er.tenant_id = i.tenant_id

LEFT JOIN bronze.cqi_emsreviewresponse err
    ON er.ReviewID = err.ReviewID
   AND cq.QuestionID = err.QuestionID
   AND er.tenant_id = err.tenant_id

WHERE

        it.IncidentTypeID = 1

    AND err.batch_id = {pipeline_run_id}

"""
    )

    # =================================================
    # MAP TO RW MAPPING
    # =================================================

    spark.sql(
        """

CREATE OR REPLACE TEMP VIEW CQIResponsesMapped AS

SELECT

    r.tenant_id,

    r.IncidentIDInternal,

    r.CQICategoryIDInternal,

    r.CQIReviewerIDInternal,

    m.AnswerTableName,

    m.AnswerColumnName,

    upper(m.DataType) AS DataType,

    r.FieldValue,

    r.ModifiedOn 

FROM CQIResponses r

INNER JOIN silver.dim_cqi_supplemental_questions_rw_mapping m

ON
       r.tenant_id = m.tenant_id
   AND r.CQICategoryIDInternal = m.CQICategoryIDInternal
   AND r.FieldDefinitionIDInternal = m.FieldDefinitionIDInternal

"""
    )

    # =================================================
    # AGGREGATE MULTI-RESPONSES
    # =================================================

    spark.sql(
        """

CREATE OR REPLACE TEMP VIEW CQIAggregatedResponses AS

SELECT

    tenant_id,

    IncidentIDInternal,

    CQICategoryIDInternal,

    CQIReviewerIDInternal,

    AnswerTableName,

    AnswerColumnName,

    DataType,

    LEFT(
    concat_ws(
        ', ',
        collect_set(FieldValue)
    ),
    2000
) AS FieldValue,

    MAX(ModifiedOn) AS ModifiedOn 

FROM CQIResponsesMapped

GROUP BY

    tenant_id,

    IncidentIDInternal,

    CQICategoryIDInternal,

    CQIReviewerIDInternal,

    AnswerTableName,

    AnswerColumnName,

    DataType

"""
    )

    # =================================================
    # FINAL DATASET
    # =================================================

    spark.sql(
        """

CREATE OR REPLACE TEMP VIEW FinalCQIDataset AS

SELECT

    tenant_id,

    IncidentIDInternal,

    CQICategoryIDInternal,

    CQIReviewerIDInternal,

    AnswerTableName,

    AnswerColumnName,

    DataType,

    FieldValue, 

    ModifiedOn

FROM CQIAggregatedResponses

"""
    )

    has_rows = len(spark.table("FinalCQIDataset").take(1)) > 0

    if has_rows:
        log_info("FRAMEWORK", "Final CQI Dataset Built")
    else:
        log_info("FRAMEWORK", "Final CQI Dataset Is Empty")


# =====================================================
# MERGE ANSWER TABLE
# =====================================================
#
# Purpose:
#   Merge pivoted CQI data into a single
#   CQI Supplemental Question Answer Table.
#
# Grain:
#
#   tenant_id
#   IncidentIDInternal
#   CQICategoryIDInternal
#   CQIReviewerIDInternal
#
# =====================================================


def merge_answer_table(
    answer_table_name,
    pivot_df,
    pipeline_batch_id,
):
    log_info(answer_table_name, "Starting Merge")

    if pivot_df is None:
        log_warning(answer_table_name, "No Data Found")

        return None

    if not pivot_df.take(1):
        log_warning(answer_table_name, "Zero Records")

        return None

    temp_view_name = (
        f"CurrentCQIPivot_{pipeline_batch_id}_{answer_table_name.split('.')[-1]}"
    )

    pivot_df.createOrReplaceTempView(temp_view_name)

    import re

    table_only = answer_table_name.split(".")[-1]

    m = re.search(r"(\d+)$", table_only)

    suffix = int(m.group(1)) if m else 0

    start_column = suffix * QA_COLUMN_COUNT + 1

    end_column = start_column + QA_COLUMN_COUNT - 1

    qa_columns = [f"QA{i}" for i in range(start_column, end_column + 1)]

    # ==========================================
    # UPDATE SET
    # ==========================================

    update_columns = [f"target.{column}=source.{column}" for column in qa_columns]

    update_columns.extend(
        [
            "target.ModifiedOn=current_timestamp()",
            f"target.batch_id={pipeline_batch_id}",
            "target.ingest_ts=current_timestamp()",
        ]
    )

    update_set = ",\n".join(update_columns)

    # ==========================================
    # INSERT COLUMNS
    # ==========================================

    insert_columns = [
        "tenant_id",
        "IncidentIDInternal",
        "CQICategoryIDInternal",
        "CQIReviewerIDInternal",
    ]

    insert_columns.extend(qa_columns)

    insert_columns.extend(
        [
            "SystemID",
            "CreatedOn",
            "ModifiedOn",
            "batch_id",
            "ingest_ts",
        ]
    )

    # ==========================================
    # INSERT VALUES
    # ==========================================

    insert_values = [
        "source.tenant_id",
        "source.IncidentIDInternal",
        "source.CQICategoryIDInternal",
        "source.CQIReviewerIDInternal",
    ]

    insert_values.extend([f"source.{column}" for column in qa_columns])

    insert_values.extend(
        [
            "source.SystemID",
            "source.CreatedOn",
            "source.ModifiedOn",
            "source.batch_id",
            "source.ingest_ts",
        ]
    )

    # ==========================================
    # MERGE SQL
    # ==========================================

    merge_sql = f"""

    MERGE INTO {answer_table_name} target

    USING {temp_view_name} source

    ON

           target.tenant_id
        = source.tenant_id

    AND target.IncidentIDInternal
        = source.IncidentIDInternal

    AND target.CQICategoryIDInternal
        = source.CQICategoryIDInternal

    AND target.CQIReviewerIDInternal
        = source.CQIReviewerIDInternal

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
# PROCESS ANSWER TABLE
# =====================================================


def process_answer_table(
    answer_table_name,
    pipeline_run_id,
):
    log_info(
        answer_table_name, f"Answer Table Processing Started (Batch={pipeline_run_id})"
    )

    start_time = time.time()

    table_audit_rows = []

    try:
        # ==========================================
        # LOAD ANSWER TABLE METADATA
        # ==========================================

        table_metadata = answer_tables_df.filter(
            F.lower(F.col("AnswerTableName")) == answer_table_name.lower()
        ).first()

        if table_metadata is None:
            raise Exception(f"Metadata Not Found For {answer_table_name}")

        target_datatype = table_metadata["TargetDataType"]

        # ==========================================
        # FILTER FINAL CQI DATASET
        # ==========================================

        current_df = (
            spark.table("FinalCQIDataset").filter(
                F.lower(F.col("AnswerTableName")) == answer_table_name.lower()
            )
        ).persist()

        current_count = current_df.count()

        log_info(answer_table_name, f"Rows To Process = {current_count}")

        if current_count == 0:
            current_df.unpersist()
            return {
                "AnswerTableName": answer_table_name,
                "Status": "SUCCESS",
                "AuditRows": table_audit_rows,
                "ErrorMessage": None,
            }

        # ==========================================
        # BUILD PIVOT
        # ==========================================

        pivot_df = build_pivot_dataframe(
            dataframe=current_df,
            target_datatype=target_datatype,
            answer_table_name=answer_table_name,
            pipeline_run_id=pipeline_run_id,
        )

        # ==========================================
        # MERGE
        # ==========================================

        audit_row = merge_answer_table(
            answer_table_name=answer_table_name,
            pivot_df=pivot_df,
            pipeline_batch_id=pipeline_run_id,
        )
        current_df.unpersist()

        if audit_row is not None:
            table_audit_rows.append(audit_row)

            if audit_row.status == "FAILED":
                return {
                    "AnswerTableName": answer_table_name,
                    "Status": "FAILED",
                    "AuditRows": table_audit_rows,
                    "ErrorMessage": f"Merge Failed : {answer_table_name}",
                }

        elapsed_seconds = round(time.time() - start_time, 2)

        log_info(
            answer_table_name, f"Completed Successfully In {elapsed_seconds} Seconds"
        )

        return {
            "AnswerTableName": answer_table_name,
            "Status": "SUCCESS",
            "AuditRows": table_audit_rows,
            "ErrorMessage": None,
        }

    except Exception as ex:
        try:
            current_df.unpersist()
        except:
            pass

        log_error(answer_table_name, str(ex))

        log_error(answer_table_name, traceback.format_exc())

        return {
            "AnswerTableName": answer_table_name,
            "Status": "FAILED",
            "AuditRows": table_audit_rows,
            "ErrorMessage": str(ex),
        }


# =====================================================
# LOAD ANSWER TABLE METADATA
# =====================================================


def load_answer_table_metadata():
    log_info("FRAMEWORK", "Loading Answer Table Metadata")

    answer_tables_df = (
        rw_mapping_df.filter(F.upper(F.col("DataType")).isin(SUPPORTED_DATA_TYPES))
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

    log_info("FRAMEWORK", f"Answer Tables Found = {answer_table_count}")

    return answer_tables_df

# COMMAND ----------

# =====================================================
# CREATE MISSING ANSWER TABLES
# =====================================================
#
# Purpose:
#   Create missing CQI Answer Tables discovered
#   from RW Mapping metadata.
#
# =====================================================


def create_missing_answer_tables():
    existing_tables_df = spark.sql(
        """
        SELECT
            lower(concat(table_schema,'.',table_name)) AS TableName
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

    missing_tables = missing_tables_df.collect()

    if len(missing_tables) == 0:
        log_info("FRAMEWORK", "No Missing Answer Tables Found")

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

            IncidentIDInternal INT,

            CQICategoryIDInternal INT,

            CQIReviewerIDInternal STRING,

            {qa_columns},

            SystemID INT,

            CreatedOn TIMESTAMP,

            ModifiedOn TIMESTAMP, 

            batch_id BIGINT,

            ingest_ts TIMESTAMP

        )

        USING DELTA

        CLUSTER BY
        (
            tenant_id,

            IncidentIDInternal,

            CQICategoryIDInternal,

            CQIReviewerIDInternal
        )

        TBLPROPERTIES (

          'delta.enableDeletionVectors'        = 'true',

          'delta.enableChangeDataFeed'         = 'true',

          'delta.targetFileSize'               = '268435456',

          'delta.tuneFileSizesForRewrites'     = 'true',

          'delta.autoOptimize.optimizeWrite'   = 'true',

          'delta.autoOptimize.autoCompact'     = 'false',

          'delta.logRetentionDuration'         = 'interval 7 days',

          'delta.deletedFileRetentionDuration' = 'interval 3 days'

        )

        """

        spark.sql(ddl)

        log_info("FRAMEWORK", f"Created Table : {table_name}")

# COMMAND ----------

# =====================================================
# ANSWER TABLE EXECUTION PLANNER
# =====================================================

log_info("FRAMEWORK", "Building Answer Table Execution Plan")

# =====================================================
# LOAD ANSWER TABLES
# =====================================================

answer_tables_df = load_answer_table_metadata()

active_answer_tables = [
    row["AnswerTableName"]
    for row in answer_tables_df.orderBy("AnswerTableName").collect()
]

# =====================================================
# WORKER CALCULATION
# =====================================================

ACTIVE_TABLE_COUNT = len(active_answer_tables)

CURRENT_WORKERS = min(MAX_WORKERS, max(MIN_WORKERS, ACTIVE_TABLE_COUNT))

log_info("FRAMEWORK", f"Active Answer Tables = {ACTIVE_TABLE_COUNT}")

log_info("FRAMEWORK", f"Worker Count = {CURRENT_WORKERS}")

if ACTIVE_TABLE_COUNT == 0:
    raise Exception("No CQI Answer Tables Found")

# =====================================================
# EXECUTION SUMMARY
# =====================================================

for answer_table in active_answer_tables:
    log_info("FRAMEWORK", f"Queued Answer Table = {answer_table}")

display(answer_tables_df.orderBy("AnswerTableName"))

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
    log_info("FRAMEWORK", f"Starting Batch = {pipeline_run_id}")

    batch_start_time = time.time()

    try:
        # ==========================================
        # BUILD FINAL CQI DATASET
        # ==========================================

        build_final_cqi_dataset(pipeline_run_id)
        # ==========================================
        # NOTHING TO PROCESS
        # ==========================================

        final_dataset_count = spark.table("FinalCQIDataset").limit(1).count()

        if final_dataset_count == 0:
            log_info(
                "FRAMEWORK", f"No CQI Responses Found For Batch = {pipeline_run_id}"
            )

            update_batch_watermark(pipeline_run_id)

            successful_batches.append(pipeline_run_id)

            continue

        # ==========================================
        # CREATE MISSING ANSWER TABLES
        # ==========================================

        create_missing_answer_tables()

        # ==========================================
        # PROCESS ANSWER TABLES
        # ==========================================

        futures = []

        with ThreadPoolExecutor(max_workers=CURRENT_WORKERS) as executor:
            for answer_table_name in active_answer_tables:
                futures.append(
                    executor.submit(
                        process_answer_table, answer_table_name, pipeline_run_id
                    )
                )

            batch_failed = False

            for future in futures:
                result = future.result()

                answer_table_name = result["AnswerTableName"]

                audit_rows.extend(result.get("AuditRows", []))

                if result["Status"] == "SUCCESS":
                    successful_tables.append((pipeline_run_id, answer_table_name))

                else:
                    batch_failed = True

                    failed_tables.append((pipeline_run_id, answer_table_name))

                    log_error(answer_table_name, result["ErrorMessage"])

            if batch_failed:
                raise Exception(
                    f"One Or More Answer Tables Failed For Batch {pipeline_run_id}"
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

        continue

# =====================================================
# FRAMEWORK SUMMARY
# =====================================================

execution_end_ts = datetime.now()

elapsed_seconds = round(time.time() - framework_start_time, 2)

log_info("FRAMEWORK", f"Completed In {elapsed_seconds} Seconds")

log_info("FRAMEWORK", f"Successful Batches = {len(successful_batches)}")

log_info("FRAMEWORK", f"Failed Batches = {len(failed_batches)}")

log_info("FRAMEWORK", f"Successful Answer Tables = {len(successful_tables)}")

log_info("FRAMEWORK", f"Failed Answer Tables = {len(failed_tables)}")

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