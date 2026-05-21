# =====================================================================
# Type        : Python file
# File        : src/silver/dq_rule_validator.py
# Purpose     : Apply metadata-driven business DQ rules and create DLQ records
# =====================================================================

from typing import List, Dict, Tuple
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# =====================================================================
# Return empty rejected-record DataFrame with dq.rejected_records structure.
# =====================================================================

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
        F.lit(None).cast("timestamp").alias("created_timestamp"),
        F.lit(None).cast("date").alias("created_date"),
    )

# =====================================================================
# Apply business DQ rules from metadata.
#    Rule design:
#    - rule_expression is a Spark SQL condition that returns TRUE for failed records.
#    - If one record fails multiple rules, it creates multiple DLQ rows, one per rule.
# =====================================================================

def apply_business_dq_rules(
    valid_df: DataFrame,
    dq_rules: List[Dict],
    catalog_name: str,
    entity_name: str,
    pipeline_run_id: str,
    target_table_name: str
) -> Tuple[DataFrame, DataFrame, List[Dict]]:

    if not dq_rules:
        return valid_df, _empty_rejected_df(valid_df), []

    total_record_count = valid_df.count()
    failed_conditions = []
    rejected_dfs = []
    rule_results = []

    for rule in dq_rules:
        rule_id = rule["rule_id"]
        rule_name = rule["rule_name"]
        rule_type = rule["rule_type"]
        rule_expression = rule["rule_expression"]
        severity = rule["severity"]

        failed_condition = F.expr(rule_expression)
        failed_conditions.append(failed_condition)

        failed_df = valid_df.filter(failed_condition)
        failed_count = failed_df.count()

        rule_results.append({
            "rule_id": rule_id,
            "rule_name": rule_name,
            "rule_type": rule_type,
            "severity": severity,
            "failed_record_count": failed_count,
            "total_record_count": total_record_count,
        })

        if failed_count > 0:
            source_file_name_expr = (
                F.col("source_file_name")
                if "source_file_name" in failed_df.columns
                else F.lit(None).cast("string")
            )

            rejected_dfs.append(
                failed_df.select(
                    F.expr("uuid()").alias("rejection_id"),
                    F.lit(pipeline_run_id).alias("pipeline_run_id"),
                    F.lit(f"{catalog_name}.silver.standardized_{entity_name}").alias("source_table_name"),
                    F.lit(target_table_name).alias("target_table_name"),
                    F.lit(rule_id).alias("rule_id"),
                    F.lit(f"Business DQ rule failed: {rule_name}").alias("rejection_reason"),
                    F.to_json(F.struct(*[F.col(col_name) for col_name in failed_df.columns])).alias("rejected_record_json"),
                    source_file_name_expr.alias("source_file_name"),
                    F.current_timestamp().alias("created_timestamp"),
                    F.current_date().alias("created_date"),
                )
            )

    combined_failure_condition = reduce(lambda left, right: left | right, failed_conditions)
    dq_valid_df = valid_df.filter(~combined_failure_condition)

    if rejected_dfs:
        dq_rejected_df = reduce(lambda left, right: left.unionByName(right), rejected_dfs)
    else:
        dq_rejected_df = _empty_rejected_df(valid_df)

    return dq_valid_df, dq_rejected_df, rule_results
