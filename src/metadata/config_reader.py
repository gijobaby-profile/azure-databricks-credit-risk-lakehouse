# =====================================================================
# Type        : Python file
# File        : src/metadata/config_reader.py
# Purpose     : Read Bronze ingestion configuration file
# =====================================================================

from src.utils.sql_utils import escape_sql, bool_to_sql
from src.utils.common_utils import resolve_source_path

# =====================================================================
# Return one enabled Bronze ingestion config row for an entity
# =====================================================================

def get_bronze_config(spark, catalog_name: str, entity_name: str, business_dt:str) -> dict:
    
    #check whether entity name is empty or None, use if not so will handle both
    if not entity_name:
        raise ValueError("entity_name is required")

    rows = spark.sql(f"""
        SELECT
            entity_name,
            source_system,
            source_path,
            target_table_full_name,
            file_format,
            delimiter,
            header_flag,
            infer_schema_flag,
            merge_schema_flag,
            load_enabled,
            load_sequence
        FROM {catalog_name}.config.bronze_ingestion_config
        WHERE lower(entity_name) = lower('{escape_sql(entity_name)}')
          AND load_enabled = true
    """).collect()

    if not rows:
        raise ValueError(f"No enabled Bronze ingestion config found for entity_name={entity_name}")

    if len(rows) > 1:
        raise ValueError(f"Multiple enabled Bronze ingestion configs found for entity_name={entity_name}")

    config = rows[0].asDict()

    # Keep original metadata path for traceability.
    config["source_path_template"] = config["source_path"]

    # Resolve source_path with entity_name and business_dt.
    if business_dt:
        config["source_path"] = resolve_source_path(
            source_path_template=config["source_path"],
            entity_name=config["entity_name"],
            business_dt=business_dt
        )

    return config
