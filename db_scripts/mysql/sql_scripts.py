class queries:
    GET_TABLE_NAMES = "SELECT table_name FROM information_schema.tables WHERE table_schema = (%s)"
    GET_ROW_COUNT = "SELECT COUNT(*) FROM {}.{}"
    GET_PRIMARY_KEYS = """SELECT  t.table_name, k.column_name FROM information_schema.table_constraints t 
        JOIN information_schema.key_column_usage k 
        USING(constraint_name, table_schema, table_name) 
        WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema=%s;"""
