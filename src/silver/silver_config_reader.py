# =====================================================================
# Type        : Python file
# File        : src/silver/silver_config_reader.py
# Purpose     : Read Silver standardization metadata configuration
# =====================================================================

from typing import List, Dict

from src.utils.sql_utils import escape_sql, bool_to_sql

# =====================================================================
# Return active Silver column config rows for an entity.
# =====================================================================

def get_silver_column_config(spark, catalog_name: str, entity_name: str) -> List[Dict]:
    if not str(entity_name or "").strip():
        raise ValueError("entity_name is required")

    rows = spark.sql(f"""
        SELECT
            entity_name,
            source_column_name,
            source_column_position,
            target_column_name,
            target_data_type,
            is_required,
            is_dedup_key,
            is_active,
            column_sequence,
            reject_on_cast_failure
        FROM {catalog_name}.config.silver_column_config
        WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
          AND is_active = true
        ORDER BY column_sequence
    """).collect()

    if not rows:
        raise ValueError(f"No active Silver column config found for entity_name: {entity_name}")
    
    # For every Row object in rows, convert that Row into a dictionary.Return the final list of dictionaries.
    return [row.asDict() for row in rows]


# =====================================================================
# Return fully qualified Silver standardized table name.
# =====================================================================

def get_standardized_table_name(catalog_name: str, entity_name: str) -> str:

    if not str(entity_name or "").strip():
        raise ValueError("entity_name is required")

    return f"{catalog_name}.silver.standardized_{entity_name}"


# =====================================================================
# Return fully qualified Bronze table name.
# =====================================================================

def get_bronze_table_name(catalog_name: str, entity_name: str) -> str:
    
    if not str(entity_name or "").strip():
        raise ValueError("entity_name is required")

    return f"{catalog_name}.bronze.{entity_name}"