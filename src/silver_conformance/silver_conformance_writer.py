from typing import Dict
from delta.tables import DeltaTable

#=========================================================================
# Replace only one business_dt partition using Delta replaceWhere.
#=========================================================================
def write_replace_business_dt(df, target_table_full_name: str, business_dt: str) -> int:
    df = df.persist()
    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"business_dt = '{business_dt}'")
        .saveAsTable(target_table_full_name)
    )

    df.unpersist()
    return row_count

#=========================================================================
# Overwrite target table and return written row count.
#=========================================================================
def write_overwrite(df, target_table_full_name: str) -> int:

    df = df.persist()
    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .saveAsTable(target_table_full_name)
    )

    df.unpersist()
    return row_count

#=========================================================================
# Merge conformed DataFrame into Delta table using business keys.
#=========================================================================
def write_merge(df, spark, target_table_full_name: str, business_keys: list[str]) -> int:

    if not business_keys:
        raise ValueError("business_keys are required for MERGE load strategy")

    df = df.persist()
    row_count = df.count()

    merge_condition = " AND ".join([f"t.{key} <=> s.{key}" for key in business_keys])
    target = DeltaTable.forName(spark, target_table_full_name)

    (
        target.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    df.unpersist()
    return row_count

#=========================================================================
# Write conformed entity based on metadata load_strategy.
#=========================================================================
def write_conformed_entity(df, spark, entity_config: Dict, business_dt: str) -> int:

    target_table_full_name = (
        f"{entity_config['target_catalog_name']}."
        f"{entity_config['target_schema_name']}."
        f"{entity_config['target_table_name']}"
    )

    load_strategy = str(entity_config["load_strategy"]).upper()
    business_keys = entity_config.get("business_key_columns") or []

    if load_strategy == "REPLACE_BUSINESS_DT":
        return write_replace_business_dt(df, target_table_full_name, business_dt)

    if load_strategy == "OVERWRITE":
        return write_overwrite(df, target_table_full_name)

    if load_strategy == "MERGE":
        return write_merge(df, spark, target_table_full_name, business_keys)

    raise ValueError(f"Unsupported load_strategy for generic writer: {load_strategy}")
