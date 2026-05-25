from typing import Dict, List, Tuple
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.sql_utils import escape_sql

#=========================================================================
# Read active DQ rules from existing dq.data_quality_rules.
#=========================================================================
def read_active_conformance_dq_rules(
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
            is_active
        FROM {catalog_name}.dq.data_quality_rules
        WHERE lower(target_catalog_name) = lower('{escape_sql(catalog_name)}')
          AND lower(target_schema_name) = lower('{escape_sql(target_schema_name)}')
          AND lower(target_table_name) = lower('{escape_sql(target_table_name)}')
          AND is_active = true
        ORDER BY rule_id
    """).collect()

    return [row.asDict() for row in rows]

#=========================================================================
# Return empty DataFrame matching dq.rejected_records structure.
#=========================================================================
def _empty_rejected_df(df: DataFrame) -> DataFrame:
    return df.limit(0).select(
        F.lit(None).cast("string").alias("rejection_id"),
        F.lit(None).cast("string").alias("pipeline_run_id"),
        F.lit(None).cast("string").alias("source_table_name"),
        F.lit(None).cast("string").alias("target_table_name"),
        F.lit(None).cast("string").alias("rule_id"),
        F.lit(None).cast("string").alias("rejection_reason"),
        F.lit(None).cast("string").alias("rejected_record_json"),
        F.lit(None).cast("string").alias("source_file_name"),
        F.lit(None).cast("string").alias("source_file_path"),
        F.lit(None).cast("string").alias("quarantine_path"),
        F.lit(None).cast("timestamp").alias("created_timestamp"),
        F.lit(None).cast("date").alias("created_date"),
    )

#=========================================================================
# Apply metadata-driven business DQ rules.
# rule_sql_expression must return TRUE for failed records.
#=========================================================================
def apply_conformance_dq_rules(
    df: DataFrame,
    dq_rules: List[Dict],
    catalog_name: str,
    target_schema_name: str,
    target_table_name: str,
    pipeline_run_id: str
) -> Tuple[DataFrame, DataFrame, List[Dict]]:


    if not dq_rules:
        return df, _empty_rejected_df(df), []

    total_count = df.count()
    failed_conditions = []
    rejected_dfs = []
    result_rows = []

    for rule in dq_rules:
        failed_condition = F.expr(rule["rule_sql_expression"])
        failed_conditions.append(failed_condition)

        failed_df = df.filter(failed_condition)
        failed_count = failed_df.count()

        result_rows.append({
            "rule_id": rule["rule_id"],
            "rule_name": rule["rule_name"],
            "rule_type": rule["rule_type"],
            "severity": rule["severity"],
            "failed_record_count": failed_count,
            "total_record_count": total_count,
        })

        if failed_count > 0:
            source_file_name_expr = F.col("source_file_name") if "source_file_name" in failed_df.columns else F.lit(None).cast("string")
            source_file_path_expr = F.col("source_file_path") if "source_file_path" in failed_df.columns else F.lit(None).cast("string")

            rejected_dfs.append(
                failed_df.select(
                    F.expr("uuid()").alias("rejection_id"),
                    F.lit(pipeline_run_id).alias("pipeline_run_id"),
                    F.lit(f"{catalog_name}.{target_schema_name}.{target_table_name}").alias("source_table_name"),
                    F.lit(f"{catalog_name}.{target_schema_name}.{target_table_name}").alias("target_table_name"),
                    F.lit(rule["rule_id"]).alias("rule_id"),
                    F.lit(f"Conformance DQ failed: {rule['rule_name']}").alias("rejection_reason"),
                    F.to_json(F.struct(*[F.col(c) for c in failed_df.columns])).alias("rejected_record_json"),
                    source_file_name_expr.alias("source_file_name"),
                    source_file_path_expr.alias("source_file_path"),
                    F.lit(None).cast("string").alias("quarantine_path"),
                    F.current_timestamp().alias("created_timestamp"),
                    F.current_date().alias("created_date"),
                )
            )

    combined_failure_condition = reduce(lambda left, right: left | right, failed_conditions)
    valid_df = df.filter(~combined_failure_condition)

    if rejected_dfs:
        rejected_df = reduce(lambda left, right: left.unionByName(right), rejected_dfs)
    else:
        rejected_df = _empty_rejected_df(df)

    return valid_df, rejected_df, result_rows

#=========================================================================
# Write rejected rows to existing dq.rejected_records table.
#=========================================================================
def write_conformance_rejected_records(rejected_df: DataFrame, catalog_name: str) -> int:
    rejected_df = rejected_df.persist()
    rejected_count = rejected_df.count()

    if rejected_count > 0:
        (
            rejected_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{catalog_name}.dq.rejected_records")
        )

    rejected_df.unpersist()
    return rejected_count

#=========================================================================
# Write DQ summary rows to existing dq.data_quality_results table.
#=========================================================================
def write_conformance_dq_results(
    spark,
    catalog_name: str,
    pipeline_run_id: str,
    table_name: str,
    rule_results: List[Dict]
) -> None:

    for result in rule_results:
        total_count = result["total_record_count"]
        failed_count = result["failed_record_count"]
        passed_count = max(total_count - failed_count, 0)
        failed_percentage = 0 if total_count == 0 else round((failed_count / total_count) * 100, 4)
        status = "PASSED" if failed_count == 0 else "FAILED"

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
                '{escape_sql(result["rule_id"])}',
                '{escape_sql(table_name)}',
                '{escape_sql(result["rule_name"])}',
                '{escape_sql(result["rule_type"])}',
                CAST({failed_count} AS BIGINT),
                CAST({total_count} AS BIGINT),
                CAST({passed_count} AS BIGINT),
                CAST({failed_percentage} AS DECIMAL(10,4)),
                '{escape_sql(status)}',
                '{escape_sql(result["severity"])}',
                current_timestamp(),
                current_date()
        """)
