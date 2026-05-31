import uuid
from typing import Optional
from pyspark.sql import SparkSession

from src.utils.sql_utils import escape_sql

# ============================================================================
# Use supplied pipeline_run_id or generate a new UUID.
# ============================================================================
def build_pipeline_run_id(pipeline_run_id: Optional[str]) -> str:
    return (
        str(pipeline_run_id).strip()
        if pipeline_run_id and str(pipeline_run_id).strip()
        else str(uuid.uuid4())
    )

# ============================================================================
# Resolve business_dt to a concrete yyyy-MM-dd value using Spark SQL.
# ============================================================================
def resolve_business_dt_sql(spark: SparkSession, business_dt: str) -> str:
    if business_dt and str(business_dt).strip():
        row = spark.sql(
            f"""
            SELECT date_format(to_date('{escape_sql(business_dt)}'), 'yyyy-MM-dd') AS business_dt
            """
        ).collect()[0]
    else:
        row = spark.sql(
            """
            SELECT date_format(current_date(), 'yyyy-MM-dd') AS business_dt
            """
        ).collect()[0]

    return row["business_dt"]
# ============================================================================
# Delete one business_dt partition from a Gold Delta table and return pre-delete count.
# ============================================================================
def delete_business_dt_partition(spark: SparkSession,target_table_full_name: str,business_dt: str) -> int:
    count_before_delete = (
        spark.table(target_table_full_name)
        .filter(f"business_dt = DATE('{escape_sql(business_dt)}')")
        .count()
    )

    spark.sql(
        f"""
        DELETE FROM {target_table_full_name}
        WHERE business_dt = DATE('{escape_sql(business_dt)}')
        """
    )

    return count_before_delete

# ============================================================================
# Count records for a target table and business_dt.
# ============================================================================
def count_business_dt(spark: SparkSession, target_table_full_name: str, business_dt: str) -> int:
    return (
        spark.table(target_table_full_name)
        .filter(f"business_dt = DATE('{escape_sql(business_dt)}')")
        .count()
    )
