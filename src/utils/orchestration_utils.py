# ============================================================
# Orchestration utility functions
# Used by ADF orchestration notebooks for SQL-safe formatting.
# ============================================================

from typing import Optional, Any

# ============================================================
# Convert Python value to a SQL string literal.
# ============================================================

def sql_string(value: Optional[Any]) -> str:

    if value is None or value == "":
        return "NULL"

    return "'" + str(value).replace("'", "''") + "'"

# ============================================================
# Convert date string to SQL DATE expression.
# ============================================================

def sql_date(value: Optional[Any]) -> str:

    if value is None or value == "":
        raise ValueError("Date value is required")

    return f"DATE({sql_string(value)})"

# ============================================================
# Convert Python/string boolean to SQL boolean literal.
# ============================================================

def sql_bool(value: Optional[Any]) -> str:

    return "true" if str(value).strip().lower() in ("true", "1", "yes", "y") else "false"

# ============================================================
# Convert value to SQL integer literal.
# ============================================================
def sql_int(value: Optional[Any], default: int = 0) -> str:

    try:
        return str(int(str(value).strip()))
    except Exception:
        return str(default)

# ============================================================
# Build fully qualified Unity Catalog table name.
# ============================================================

def full_table_name(catalog_name: str, schema_name: str, table_name: str) -> str:

    if not catalog_name or not schema_name or not table_name:
        raise ValueError("catalog_name, schema_name, and table_name are required")

    return f"{catalog_name}.{schema_name}.{table_name}"