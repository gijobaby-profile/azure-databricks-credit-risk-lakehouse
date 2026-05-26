from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.silver_conformance.hash_utils import build_record_hash

#=========================================================================
# Build source DataFrame using metadata source_query.
#=========================================================================
def build_source_dataframe(spark, entity_config: Dict) -> DataFrame:
    source_query = entity_config["source_query"]
    if not str(source_query or "").strip():
        raise ValueError(f"source_query is missing for entity={entity_config['entity_name']}")
    return spark.sql(source_query)


#=========================================================================
# Apply metadata-driven derived columns using Spark SQL expressions.
#=========================================================================
def apply_derived_columns(df: DataFrame, derived_config: List[Dict]) -> DataFrame:
    
    output_df = df

    for rule in derived_config:
        output_df = output_df.withColumn(
            rule["derived_column_name"],
            F.expr(rule["derived_sql_expression"]).cast(rule["target_data_type"])
        )

    return output_df

#=========================================================================
# Add common technical metadata and record_hash.
#=========================================================================
def add_conformance_metadata(df: DataFrame, entity_config: Dict, pipeline_run_id: str) -> DataFrame:

    output_df = df

    if "source_system" not in output_df.columns:
        output_df = output_df.withColumn("source_system", F.lit("home_credit"))

    output_df = (
        output_df
        .withColumn("business_dt", F.to_date(F.lit(business_dt)))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
        .withColumn("created_date", F.current_date())
    )

    hash_columns = entity_config.get("hash_columns") or []
    existing_hash_columns = [col_name for col_name in hash_columns if col_name in output_df.columns]

    if existing_hash_columns:
        output_df = output_df.withColumn("record_hash", build_record_hash(existing_hash_columns))

    return output_df

#=========================================================================
# Remove duplicates by business keys, keeping the latest record.
#=========================================================================
def deduplicate_by_business_keys(df: DataFrame, business_keys: List[str]) -> DataFrame:

    if not business_keys:
        return df

    from pyspark.sql.window import Window

    order_col = "standardization_timestamp" if "standardization_timestamp" in df.columns else "created_timestamp"

    window_spec = Window.partitionBy(*business_keys).orderBy(F.col(order_col).desc_nulls_last())

    return (
        df.repartition(*[F.col(c) for c in business_keys])
        .withColumn("_conformed_row_number", F.row_number().over(window_spec))
        .filter(F.col("_conformed_row_number") == 1)
        .drop("_conformed_row_number")
    )

#=========================================================================
# Build a conformed DataFrame using metadata.
#=========================================================================
def build_conformed_dataframe(
    spark,
    entity_config: Dict,
    derived_config: List[Dict],
    pipeline_run_id: str,
    business_dt: str
) -> DataFrame:

    source_df = build_source_dataframe(spark, entity_config)
    derived_df = apply_derived_columns(source_df, derived_config)
    metadata_df = add_conformance_metadata(derived_df, entity_config, pipeline_run_id)

    business_keys = entity_config.get("business_key_columns") or []
    return deduplicate_by_business_keys(metadata_df, business_keys)
