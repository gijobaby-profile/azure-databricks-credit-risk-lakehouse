# =====================================================================
# Type        : Python file
# File        : src/silver/dq_writer.py
# Purpose     : Write Silver DQ/DLQ result records
# =====================================================================

from typing import List, Dict

from pyspark.sql import DataFrame

from src.utils.sql_utils import escape_sql

# =====================================================================
# Write rejected records to dq.rejected_records and return rejected count
# =====================================================================

def write_rejected_records(
    rejected_df: DataFrame,
    catalog_name: str,
    mode: str = "append"
) -> int:

    rejected_count = rejected_df.count()

    if rejected_count > 0:
        (
            rejected_df.write
            .format("delta")
            .mode(mode)
            .saveAsTable(f"{catalog_name}.dq.rejected_records")
        )

    return rejected_count

# =====================================================================
# Write one DQ result summary row into dq.data_quality_results.
# =====================================================================

def write_dq_result(
    spark,
    catalog_name: str,
    pipeline_run_id: str,
    target_table_name: str,
    rule_id: str,
    rule_name: str,
    rule_type: str,
    total_record_count: int,
    failed_record_count: int,
    severity: str = "HIGH"
) -> None:
 
    passed_record_count = max(total_record_count - failed_record_count, 0)
    failed_percentage = 0 if total_record_count == 0 else round((failed_record_count / total_record_count) * 100, 4)
    status = "PASSED" if failed_record_count == 0 else "FAILED"

    spark.sql(f"""
        INSERT INTO {catalog_name}.dq.data_quality_results
        (
            dq_result_id,
            pipeline_run_id,
            rule_id,
            table_name,
            rule_name,
            rule_type,
            failed_record_count,
            total_record_count,
            passed_record_count,
            failed_percentage,
            status,
            severity,
            checked_timestamp,
            checked_date
        )
        SELECT
            uuid(),
            '{escape_sql(pipeline_run_id)}',
            '{escape_sql(rule_id)}',
            '{escape_sql(target_table_name)}',
            '{escape_sql(rule_name)}',
            '{escape_sql(rule_type)}',
            CAST({failed_record_count} AS BIGINT),
            CAST({total_record_count} AS BIGINT),
            CAST({passed_record_count} AS BIGINT),
            CAST({failed_percentage} AS DECIMAL(10,4)),
            '{escape_sql(status)}',
            '{escape_sql(severity)}',
            current_timestamp(),
            current_date()
    """)


# =====================================================================
# Write business DQ rule results into dq.data_quality_results.
# =====================================================================

def write_dq_rule_results(
    spark,
    catalog_name: str,
    pipeline_run_id: str,
    target_table_name: str,
    rule_results: List[Dict]
) -> None:

    for result in rule_results:
        write_dq_result(
            spark=spark,
            catalog_name=catalog_name,
            pipeline_run_id=pipeline_run_id,
            target_table_name=target_table_name,
            rule_id=result["rule_id"],
            rule_name=result["rule_name"],
            rule_type=result["rule_type"],
            total_record_count=result["total_record_count"],
            failed_record_count=result["failed_record_count"],
            severity=result.get("severity", "MEDIUM"),
        )

