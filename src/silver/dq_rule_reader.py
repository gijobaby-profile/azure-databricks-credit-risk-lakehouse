# =====================================================================
# Type        : Python file
# File        : src/silver/dq_rule_reader.py
# Purpose     : Read active business DQ rules for Silver validation
# =====================================================================

from typing import List, Dict

from src.utils.sql_utils import escape_sql

# =====================================================================
# Return active business DQ rules for a target Silver table.
# =====================================================================
def get_active_dq_rules(
    spark,
    catalog_name: str,
    target_schema_name: str,
    target_table_name: str
) -> List[Dict]:

    rows = spark.sql(f"""
        SELECT
            rule_id,
            rule_name,
            rule_description,
            rule_type,
            target_catalog_name,
            target_schema_name,
            target_table_name,
            target_column_name,
            rule_sql_expression,
            severity,
            is_active,
            created_timestamp,
            updated_timestamp
        FROM {catalog_name}.dq.data_quality_rules
        WHERE lower(target_catalog_name) = lower('{escape_sql(catalog_name)}')
          AND lower(target_schema_name) = lower('{escape_sql(target_schema_name)}')
          AND lower(target_table_name) = lower('{escape_sql(target_table_name)}')
          AND is_active = true
        ORDER BY rule_id
    """).collect()

    return [row.asDict() for row in rows]
