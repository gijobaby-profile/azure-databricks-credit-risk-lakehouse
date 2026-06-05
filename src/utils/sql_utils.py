# =====================================================================
# Type        : Python file
# File        : src/utils/sql_utils.py
# Purpose     : Common SQL helper utilities
# =====================================================================


# =====================================================================
# Escape single quotes for safe use inside SQL string literals.
# =====================================================================
def escape_sql(value) -> str:
    return "" if value is None else str(value).replace("'", "''")

# =====================================================================
# Convert Python boolean value to SQL boolean text.
# =====================================================================
def bool_to_sql(value) -> str:
    if value is None:
        raise ValueError("Boolean value is missing")

    if not isinstance(value, bool):
        raise TypeError(f"Expected boolean value, got {type(value).__name__}: {value}")

    return "true" if value else "false"

# =====================================================================
# Backtick-quote a column/table/schema identifier for Spark SQL expressions ( protect the column names with space, sepcial char, keywords, date, select etc. )
# =====================================================================
def quote_identifier(identifier: str) -> str:
    if identifier is None or str(identifier).strip() == "":
        raise ValueError("SQL identifier is required")

    return f"`{str(identifier).replace('`', '``')}`"

# =====================================================================
# Safely convert a value to integer.
# Returns the default value if conversion fails.
# Useful before injecting numeric values into SQL strings.
# =====================================================================
def safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default
#=====================================================================