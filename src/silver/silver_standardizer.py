# =====================================================================
# Type        : Python file
# File        : src/silver/silver_standardizer.py
# Purpose     : Metadata-driven Silver schema enforcement and standardization
# =====================================================================

from typing import List, Dict, Tuple, Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.sql_utils import quote_identifier
#from src.utils.debug_utils import debug_print, debug_print_columns, debug_print_schema


TECHNICAL_COLUMNS = [
    "source_file_name",
    "source_file_path",
    "pipeline_run_id",
    "ingestion_timestamp",
    "ingestion_date",
]


# =====================================================================
# Return temporary raw Bronze column name used for cast-failure validation.
# =====================================================================

def _bronze_temp_col(source_col: str) -> str:
    return f"_bronze_{source_col}"

# =====================================================================
# Remove temporary raw Bronze columns before writing valid records to Silver.
# =====================================================================

def drop_bronze_temp_columns(df: DataFrame) -> DataFrame:
    temp_columns = [col_name for col_name in df.columns if col_name.startswith("_bronze_")]
    return df.drop(*temp_columns) if temp_columns else df

# =====================================================================
# Validate that configured source columns exist in the Bronze DataFrame.
# =====================================================================
def validate_source_columns(bronze_df: DataFrame, column_config: List[Dict]) -> None:

    bronze_columns_lower = {col_name.lower() for col_name in bronze_df.columns}

    missing_columns = [
        cfg["source_column_name"]
        for cfg in column_config
        if cfg["source_column_name"].lower() not in bronze_columns_lower
    ]

    if missing_columns:
        raise ValueError(f"Missing configured Bronze source columns: {missing_columns}")


# =====================================================================
# Create standardized Silver DataFrame from raw Bronze DataFrame
#    - Keep raw Bronze values temporarily as _bronze_<source_column>.
#    - Apply try_cast to create standardized target columns.
#    - Later, detect row-level cast failures using: raw source value is not null/blank AND casted target value is null.
# =====================================================================

def build_standardized_dataframe(
    bronze_df: DataFrame,
    column_config: List[Dict],
    pipeline_run_id: str,
    business_dt: str
) -> DataFrame:

    select_exprs = []

    for cfg in column_config:
        source_col = cfg["source_column_name"]
        target_col = cfg["target_column_name"]
        target_type = cfg["target_data_type"]

        select_exprs.append(
            F.col(source_col).alias(_bronze_temp_col(source_col))
        )

        select_exprs.append(
            F.expr(
                f"try_cast({quote_identifier(source_col)} as {target_type})"
            ).alias(target_col)
        )

    for tech_col in TECHNICAL_COLUMNS:
        if tech_col in bronze_df.columns:
            select_exprs.append(F.col(tech_col))

    select_exprs.extend([
        F.lit(pipeline_run_id).alias("standardization_run_id"),
        F.to_date(F.lit(business_dt)).alias("business_dt"),
        F.current_timestamp().alias("standardization_timestamp"),
        F.current_date().alias("standardization_date"),
    ])

    #  Creates the standardized DataFrame from bronze.df by selecting all dynamically built expressions, select_exprs.
    #  The * unpacks the list so select() receives each cast/rename/metadata column separately
    #  without * it will pass the select_exprs as a single list argument which fails the select().

    # return bronze_df.select(*select_exprs)

    standardized_df = bronze_df.select(*select_exprs)

    print("===== STANDARDIZED DF COLUMNS AFTER SELECT =====")
    print(standardized_df.columns)

    return standardized_df

# =====================================================================
# Build rejection condition for required fields
#    Reject a record when:
#    1. A configured required target column is NULL.
#    2.  Capture Type casting issue during try_cast() .A source value is present, but try_cast returns NULL, and reject_on_cast_failure is true.
# This function only builds the Spark Column condition. The condition is applied later using standardized_df.filter(condition).
# =====================================================================

def build_rejection_condition(column_config: List[Dict]) -> Optional[Column]:

    condition = None

    for cfg in column_config:
        source_col = cfg["source_column_name"]
        target_col = cfg["target_column_name"]
        is_required = cfg["is_required"]
        reject_on_cast_failure = cfg["reject_on_cast_failure"]

        current_condition = None

        # if the filed is a mandatory field, populating null is a failed condition
        if is_required:
            current_condition = F.col(target_col).isNull()

        # if there is a type casting issue using try_cast() , it will return NULL instead of failing, this we will handle it here
        if reject_on_cast_failure:
            raw_col = _bronze_temp_col(source_col)

            cast_failure_condition = (
                F.col(raw_col).isNotNull()
                & (F.trim(F.col(raw_col).cast("string")) != "")
                & F.col(target_col).isNull()
            )

            current_condition = (
                cast_failure_condition
                if current_condition is None
                else current_condition | cast_failure_condition
            )

        if current_condition is not None:
            condition = (
                current_condition
                if condition is None
                else condition | current_condition
            )

    print("===== Rejection Condition =====")
    print(condition)

    return condition

# =====================================================================
# Split standardized data into valid and rejected DataFrames.
#    Valid records:
#    - pass required-field checks
#    - pass configured cast-failure checks
#    - temporary _bronze_ columns are removed before writing to Silver
#
#    Rejected records:
#    - written to dq.rejected_records with full rejected_record_json for traceability
# =====================================================================
def split_valid_rejected(
    standardized_df: DataFrame,
    column_config: List[Dict],
    catalog_name: str,
    entity_name: str,
    pipeline_run_id: str,
    target_table_name: str
) -> Tuple[DataFrame, DataFrame]:

    rejection_condition = build_rejection_condition(column_config)

    empty_rejected_df = standardized_df.limit(0).select(
        F.lit(None).cast("string").alias("rejection_id"),
        F.lit(None).cast("string").alias("pipeline_run_id"),
        F.lit(None).cast("string").alias("source_table_name"),
        F.lit(None).cast("string").alias("target_table_name"),
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(None).cast("string").alias("rejection_reason"),
        F.lit(None).cast("string").alias("rejected_record_json"),
        F.lit(None).cast("string").alias("source_file_name"),
        F.lit(None).cast("timestamp").alias("created_timestamp"),
        F.lit(None).cast("date").alias("created_date"),
    )

    if rejection_condition is None:
        return drop_bronze_temp_columns(standardized_df), empty_rejected_df

    rejected_base_df = standardized_df.filter(rejection_condition)
    valid_base_df = standardized_df.filter(~rejection_condition)

    source_file_name_expr = (
        F.col("source_file_name")
        if "source_file_name" in rejected_base_df.columns
        else F.lit(None).cast("string")
    )

    rejected_df = rejected_base_df.select(
        F.expr("uuid()").alias("rejection_id"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        F.lit(f"{catalog_name}.bronze.{entity_name}").alias("source_table_name"),
        F.lit(target_table_name).alias("target_table_name"),
        F.lit("SILVER_SCHEMA_CAST_VALIDATION").alias("rule_id"),
        F.lit("Required field is NULL or source value failed datatype casting").alias("rejection_reason"),
        F.to_json(F.struct(*[F.col(col_name) for col_name in rejected_base_df.columns])).alias("rejected_record_json"),
        source_file_name_expr.alias("source_file_name"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_date().alias("created_date"),
    )

    valid_df = drop_bronze_temp_columns(valid_base_df)

    print("===== VALID DF COLUMNS =====")
    print(valid_df.columns)

    print("===== REJECTED DF COLUMNS =====")
    print(rejected_df.columns)

    return valid_df, rejected_df

# =====================================================================
# Remove duplicates using columns marked as is_dedup_key in config
#    Rule:
#    - Partition by configured dedup keys.
#    - Keep the latest record based on ingestion_timestamp where available.
# =====================================================================
def remove_duplicates(valid_df: DataFrame, column_config: List[Dict]) -> DataFrame:

    dedup_keys = [
        cfg["target_column_name"]
        for cfg in column_config
        if cfg["is_dedup_key"]
    ]

    if not dedup_keys:
        return valid_df

    order_col = "ingestion_timestamp" if "ingestion_timestamp" in valid_df.columns else None

    if order_col:
        window_spec = Window.partitionBy(*dedup_keys).orderBy(F.col(order_col).desc_nulls_last())
    else:
        window_spec = Window.partitionBy(*dedup_keys).orderBy(F.lit(1))

    return (
        valid_df
        .withColumn("_dedup_row_number", F.row_number().over(window_spec))
        .filter(F.col("_dedup_row_number") == 1)
        .drop("_dedup_row_number")
    )


# =====================================================================