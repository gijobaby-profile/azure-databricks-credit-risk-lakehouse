# =====================================================================
# Type        : Python file
# File        : src/metadata/config_reader.py
# Purpose     : Read Bronze ingestion configuration file
# =====================================================================

# =====================================================================
# to escape the ' used as appostope eg 'beauro's_file' and handle Null value
# =====================================================================
def _escape(value) -> str:
    return "" if value is None else str(value).replace("'", "''")


# =====================================================================
# Return one enabled Bronze ingestion config row for an entity
# =====================================================================

def get_bronze_config(spark, catalog_name: str, entity_name: str) -> dict:
    
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
        WHERE lower(entity_name) = lower('{_escape(entity_name)}')
          AND load_enabled = true
    """).collect()

    if not rows:
        raise ValueError(f"No enabled Bronze ingestion config found for entity_name={entity_name}")

    if len(rows) > 1:
        raise ValueError(f"Multiple enabled Bronze ingestion configs found for entity_name={entity_name}")

    return rows[0].asDict()
