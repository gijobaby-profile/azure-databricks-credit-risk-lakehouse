from typing import List
from pyspark.sql import functions as F
from pyspark.sql.column import Column

#=========================================================================
# Build SHA-256 hash from a list of columns.
#=========================================================================

def build_record_hash(columns: List[str]) -> Column:
    if not columns:
        raise ValueError("At least one column is required to build record_hash")

    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(col_name).cast("string"), F.lit("__NULL__")) for col_name in columns]
        ),
        256
    )
