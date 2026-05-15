# =====================================================================
# Type        : Python file
# File        : src/ingestion/copy_into_builder.py
# Purpose     : Build metadata-driven Bronze COPY INTO SQL
# =====================================================================


# =====================================================================
# to escape the ' used as appostope eg 'beauro's_file' and handle Null value
# =====================================================================
def _escape(value) -> str:
    return "" if value is None else str(value).replace("'", "''")

#=====================================================================
# To translate the Python Bool value(True / False ) to SQL Bool value (true /  false)
#=====================================================================

def _bool_text(value) -> str:
    return "true" if bool(value) else "false"

#=====================================================================
# Build COPY INTO SQL for raw Bronze ingestion
#=====================================================================

def build_copy_into_sql(config: dict, pipeline_run_id: str, force_reload: bool = False) -> str:

    querry = f"""
    COPY INTO {config["target_table_full_name"]}
    FROM (
        SELECT
            *,
            _metadata.file_name AS source_file_name,
            _metadata.file_path AS source_file_path,
            '{_escape(pipeline_run_id)}' AS pipeline_run_id,
            current_timestamp() AS ingestion_timestamp,
            current_date() AS ingestion_date
        FROM '{_escape(config["source_path"])}'
    )
    FILEFORMAT = {str(config["file_format"]).upper()}
    FORMAT_OPTIONS (
        'header' = '{_bool_text(config["header_flag"])}',
        'delimiter' = '{_escape(config["delimiter"])}',
        'inferSchema' = '{_bool_text(config["infer_schema_flag"])}',
        'multiLine' = 'true',
        'escape' = '"',
        'mode' = 'PERMISSIVE'
    )
    COPY_OPTIONS (
        'mergeSchema' = '{_bool_text(config["merge_schema_flag"])}',
        'force' = '{_bool_text(force_reload)}'
    ) """.strip()

    return querry
