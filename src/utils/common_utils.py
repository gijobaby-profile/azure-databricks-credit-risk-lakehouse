# =====================================================================
# Type        : Python file
# File        : src/metadata/config_reader.py
# Purpose     : Read Bronze ingestion configuration file
# =====================================================================

from src.utils.sql_utils import escape_sql, bool_to_sql

# =====================================================================
# Resolve dynamic source path using entity_name and business_dt.
# =====================================================================

def resolve_source_path(source_path_template: str, entity_name: str, business_dt: str) -> str:

    if not source_path_template:
        raise ValueError("source_path_template is required")

    if not entity_name:
        raise ValueError("entity_name is required")

    if not business_dt:
        raise ValueError("business_dt is required")

    business_dt_token = business_dt.replace("-", "")

    return (
        source_path_template
        .replace("{entity_name}", entity_name)
        .replace("{business_dt}", business_dt_token)
    )


# =====================================================================
