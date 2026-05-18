# =====================================================================
# Type        : Python file
# File        : src/silver/silver_standardizer.py
# Purpose     : Metadata-driven Silver schema enforcement and standardization
# =====================================================================

from typing import List, Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.sql_utils import escape_sql, bool_to_sql, quote_identifier


TECHNICAL_COLUMNS = [
    "source_file_name",
    "source_file_path",
    "pipeline_run_id",
    "ingestion_timestamp",
    "ingestion_date",
]

# =====================================================================
# Validate that configured source columns exist in the Bronze DataFrame.
# =====================================================================
def validate_source_columns(bronze_df: DataFrame, column_config: List[Dict]) -> None:

    bronze_columns_lower = {col.lower() for col in bronze_df.columns}

    missing_columns = [
        cfg["source_column_name"]
        for cfg in column_config
        if cfg["source_column_name"].lower() not in bronze_columns_lower
    ]

    if missing_columns:
        raise ValueError(f"Missing configured Bronze source columns: {missing_columns}")


# =====================================================================
# Create standardized Silver DataFrame from raw Bronze DataFrame
# =====================================================================

def build_standardized_dataframe(
    bronze_df: DataFrame,
    column_config: List[Dict],
    pipeline_run_id: str
) -> DataFrame:

    select_exprs = []

    for cfg in column_config:
        source_col = cfg["source_column_name"]
        target_col = cfg["target_column_name"]
        target_type = cfg["target_data_type"]

        select_exprs.append(
            F.expr(f"try_cast({quote_identifier(source_col)} as {target_type})").alias(target_col)
        )

    for tech_col in TECHNICAL_COLUMNS:
        if tech_col in bronze_df.columns:
            select_exprs.append(F.col(tech_col))

    select_exprs.extend([
        F.lit(pipeline_run_id).alias("standardization_run_id"),
        F.current_timestamp().alias("standardization_timestamp"),
        F.current_date().alias("standardization_date"),
    ])

    return bronze_df.select(*select_exprs)

# =====================================================================
# Build rejection condition for required fields
# =====================================================================

def build_rejection_condition(
    standardized_df: DataFrame,
    column_config: List[Dict]
):

    condition = None

    for cfg in column_config:
        target_col = cfg["target_column_name"]
        is_required = bool(cfg.get("is_required"))

        if is_required:
            current_condition = F.col(target_col).isNull()
            condition = current_condition if condition is None else (condition | current_condition)

    return condition

# =====================================================================
# Split standardized data into valid and rejected DataFrames.
# =====================================================================
def split_valid_rejected(
    standardized_df: DataFrame,
    column_config: List[Dict],
    catalog_name: str,
    entity_name: str,
    pipeline_run_id: str,
    target_table_name: str
) -> Tuple[DataFrame, DataFrame]:

    rejection_condition = build_rejection_condition(standardized_df, column_config)

    if rejection_condition is None:
        valid_df = standardized_df
        rejected_df = standardized_df.limit(0).select(
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
        return valid_df, rejected_df

    valid_df = standardized_df.filter(~rejection_condition)
    rejected_base_df = standardized_df.filter(rejection_condition)

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
        F.lit("SILVER_REQUIRED_FIELD_CHECK").alias("rule_id"),
        F.lit("Required field is NULL after standardization/casting").alias("rejection_reason"),
        F.to_json(F.struct(*[F.col(c) for c in rejected_base_df.columns])).alias("rejected_record_json"),
        source_file_name_expr.alias("source_file_name"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_date().alias("created_date"),
    )

    return valid_df, rejected_df

# =====================================================================
# Remove duplicates using columns marked as is_dedup_key in config
# =====================================================================
def remove_duplicates(valid_df: DataFrame, column_config: List[Dict]) -> DataFrame:
    dedup_keys = [
        cfg["target_column_name"]
        for cfg in column_config
        if bool(cfg.get("is_dedup_key"))
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