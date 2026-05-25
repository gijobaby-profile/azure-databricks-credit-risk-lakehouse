from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


FUTURE_TIMESTAMP = "9999-12-31 23:59:59"

#=========================================================================
# Prepare source DataFrame for SCD2 processing.
#=========================================================================
def prepare_scd2_source(df: DataFrame, entity_config: Dict, pipeline_run_id: str, business_dt: str) -> DataFrame:

    effective_col = entity_config.get("effective_timestamp_column")

    if effective_col and effective_col in df.columns:
        effective_from_expr = F.col(effective_col).cast("timestamp")
    else:
        effective_from_expr = F.current_timestamp()

    return (
        df
        .withColumn("business_dt", F.to_date(F.lit(business_dt)))
        .withColumn("effective_from", effective_from_expr)
        .withColumn("effective_to", F.to_timestamp(F.lit(FUTURE_TIMESTAMP)))
        .withColumn("is_current", F.lit(True))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
        .withColumn("created_date", F.current_date())
    )

#=========================================================================
# Return new and changed records compared to current target SCD2 records.
#=========================================================================
def identify_new_and_changed_records(
    spark,
    source_df: DataFrame,
    target_table_full_name: str,
    business_keys: List[str]
):
    target_current_df = (
        spark.table(target_table_full_name)
        .filter(F.col("is_current") == True)
        .select(*business_keys, F.col("record_hash").alias("target_record_hash"))
    )

    join_condition = [source_df[key] == target_current_df[key] for key in business_keys]

    compared_df = source_df.alias("s").join(
        target_current_df.alias("t"),
        join_condition,
        "left"
    )

    new_df = compared_df.filter(F.col("target_record_hash").isNull()).select("s.*")

    changed_df = (
        compared_df
        .filter(F.col("target_record_hash").isNotNull())
        .filter(F.col("s.record_hash") != F.col("target_record_hash"))
        .select("s.*")
    )

    return new_df, changed_df

#=========================================================================
# Expire current SCD2 records that have changed.
#=========================================================================
def expire_changed_records(
    spark,
    changed_df: DataFrame,
    target_table_full_name: str,
    business_keys: List[str]
) -> None:

    changed_df.createOrReplaceTempView("_scd2_changed_records")

    join_condition = " AND ".join([f"t.{key} = s.{key}" for key in business_keys])

    spark.sql(f"""
        MERGE INTO {target_table_full_name} AS t
        USING _scd2_changed_records AS s
        ON {join_condition}
           AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.effective_to = s.effective_from,
            t.is_current = false,
            t.updated_timestamp = current_timestamp()
    """)

#=========================================================================
# Apply SCD Type 2 load.
#=========================================================================
def apply_scd2_merge(
    spark,
    source_df: DataFrame,
    entity_config: Dict,
    target_table_full_name: str,
    pipeline_run_id: str,
    business_dt: str
) -> int:

    business_keys = entity_config.get("business_key_columns") or []
    if not business_keys:
        raise ValueError("business_key_columns are required for SCD2")

    prepared_df = prepare_scd2_source(source_df, entity_config, pipeline_run_id, business_dt).persist()

    new_df, changed_df = identify_new_and_changed_records(
        spark=spark,
        source_df=prepared_df,
        target_table_full_name=target_table_full_name,
        business_keys=business_keys
    )

    new_df = new_df.persist()
    changed_df = changed_df.persist()

    new_count = new_df.count()
    changed_count = changed_df.count()

    if changed_count > 0:
        expire_changed_records(
            spark=spark,
            changed_df=changed_df,
            target_table_full_name=target_table_full_name,
            business_keys=business_keys
        )

    records_to_insert_df = new_df.unionByName(changed_df)

    if new_count + changed_count > 0:
        (
            records_to_insert_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table_full_name)
        )

    new_df.unpersist()
    changed_df.unpersist()
    prepared_df.unpersist()

    return new_count + changed_count
