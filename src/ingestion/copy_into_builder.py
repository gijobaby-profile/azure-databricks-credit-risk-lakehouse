# =====================================================================
# Type        : Python file
# File        : src/ingestion/copy_into_builder.py
# Purpose     : Build metadata-driven Bronze COPY INTO SQL
# =====================================================================

from src.utils.sql_utils import escape_sql, bool_to_sql

#=====================================================================
# Build COPY INTO SQL for raw Bronze ingestion
#=====================================================================

def build_copy_into_sql(config: dict, pipeline_run_id: str, business_dt: str, force_reload: bool = False) -> str:

    querry = f"""
    COPY INTO {config["target_table_full_name"]}
    FROM (
        SELECT
            *,
            _metadata.file_name AS source_file_name,
            _metadata.file_path AS source_file_path,
            '{escape_sql(pipeline_run_id)}' AS pipeline_run_id,
            to_date('{escape_sql(business_dt)}') AS business_dt,
            current_timestamp() AS ingestion_timestamp,
            current_date() AS ingestion_date
        FROM '{escape_sql(config["source_path"])}'
    )
    FILEFORMAT = {str(config["file_format"]).upper()}
    FORMAT_OPTIONS (
        'header' = '{bool_to_sql(config["header_flag"])}',
        'delimiter' = '{escape_sql(config["delimiter"])}',
        'inferSchema' = '{bool_to_sql(config["infer_schema_flag"])}',
        'multiLine' = 'true',
        'escape' = '"',
        'mode' = 'PERMISSIVE'
    )
    COPY_OPTIONS (
        'mergeSchema' = '{bool_to_sql(config["merge_schema_flag"])}',
        'force' = '{bool_to_sql(force_reload)}'
    ) """.strip()

    return querry
