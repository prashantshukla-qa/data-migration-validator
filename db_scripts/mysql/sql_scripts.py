class queries:
    GET_TABLE_NAMES = "SELECT table_name FROM information_schema.tables WHERE table_schema = (%s)"
    GET_ROW_COUNT = "SELECT COUNT(*) FROM {}.{}"
