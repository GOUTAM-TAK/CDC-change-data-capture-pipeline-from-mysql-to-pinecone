
def get_primary_key_column(connection, table_name):
    query = """
    SELECT COLUMN_NAME
    FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = %s
      AND TABLE_NAME = %s
      AND CONSTRAINT_NAME = 'PRIMARY';
    """
    cursor = connection.cursor()
    cursor.execute(query, ('task1', table_name))  # Replace 'task1' with your database name
    primary_key_column = cursor.fetchall()
    cursor.close()
    print(f"successfully fetch primary column name : {primary_key_column[0][0]} of table: {table_name}")
    if primary_key_column:
        return primary_key_column[0][0]
    else:
        raise ValueError(f"No primary key found for table {table_name}")
