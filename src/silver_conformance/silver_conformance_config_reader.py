from typing import Dict, List
from src.utils.sql_utils import escape_sql

#=========================================================================
# Read one active Silver Conformance entity configuration row
#=========================================================================
def get_silver_conformance_entity_config(spark, catalog_name: str, entity_name: str) -> Dict:
    rows = spark.sql(f"""
        SELECT
            entity_name,
            target_catalog_name,
            target_schema_name,
            target_table_name,
            source_query,
            load_strategy,
            business_key_columns,
            hash_columns,
            effective_timestamp_column,
            is_scd2,
            load_enabled,
            load_sequence
        FROM {catalog_name}.config.silver_conformance_entity_config
        WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
          AND load_enabled = true
    """).collect()

    if not rows:
        raise ValueError(f"No active Silver Conformance config found for entity_name={entity_name}")

    return rows[0].asDict()

#=========================================================================
# Read all enabled Silver Conformance entity configs ordered by load sequence.
#=========================================================================
def get_active_silver_conformance_entities(spark, catalog_name: str) -> List[Dict]:
    rows = spark.sql(f"""
        SELECT
            entity_name,
            target_catalog_name,
            target_schema_name,
            target_table_name,
            source_query,
            load_strategy,
            business_key_columns,
            hash_columns,
            effective_timestamp_column,
            is_scd2,
            load_enabled,
            load_sequence
        FROM {catalog_name}.config.silver_conformance_entity_config
        WHERE load_enabled = true
        ORDER BY load_sequence
    """).collect()

    return [row.asDict() for row in rows]

#=========================================================================
# Read active derived column config for one conformed entity.
#=========================================================================
def get_silver_conformance_derived_column_config(spark, catalog_name: str, entity_name: str) -> List[Dict]:
    rows = spark.sql(f"""
        SELECT
            entity_name,
            derived_column_name,
            derived_sql_expression,
            target_data_type,
            is_active,
            column_sequence
        FROM {catalog_name}.config.silver_conformance_derived_column_config
        WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
          AND is_active = true
        ORDER BY column_sequence
    """).collect()

    return [row.asDict() for row in rows]

#=========================================================================
# Return fully qualified target table name from entity config.
#=========================================================================
def get_target_table_full_name(entity_config: Dict) -> str:
    return (
        f"{entity_config['target_catalog_name']}."
        f"{entity_config['target_schema_name']}."
        f"{entity_config['target_table_name']}"
    )
