# =====================================================================
# Type        : Python file
# File        : src/utils/debug_utils.py
# Purpose     : Helper utility to enable and diable the debug mode
# =====================================================================

# =====================================================================
# Simple common debug helper
# How to use : 
#                debug_print(debug_mode, "===== VALID DF COLUMNS =====")
#                debug_print(debug_mode, str(valid_df.columns))
# =====================================================================
def debug_print(debug_mode: bool, message: str):
    if debug_mode:
        print(message)


# =====================================================================
# Better debug helper for dataframe columns
# How to use : ( Advantage - with in one line both the message and values are porvidng)
#                
#               debug_print_columns(debug_mode, valid_df, "VALID DF")
# =====================================================================
def debug_print_columns(debug_mode: bool, df: DataFrame, df_name: str):
    if debug_mode:
        print(f"===== {df_name} COLUMNS =====")
        for col_name in df.columns:
            print(col_name)


# =====================================================================
# Better debug helper for dataframe schema
# How to use : ( Advantage - with in one line both the message and values are porvidng)
#                
#               debug_print_schema(debug_mode, valid_df, "VALID DF")
# =====================================================================
def debug_print_schema(debug_mode: bool, df: DataFrame, df_name: str):
    if debug_mode:
        print(f"===== {df_name} SCHEMA =====")
        df.printSchema()