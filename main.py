import psycopg2


source_connection = psycopg2.connect(host="localhost", port=8082, database="postgres", user="postgres",
                                     password="password")
target_connection = psycopg2.connect(host="localhost", port=8083, database="postgres", user="postgres",
                                     password="password")


def add_data(target, table_name, schema, vals):
    cursor = target.cursor()
    query = "INSERT INTO {} VALUES (".format(table_name)
    for i in range(len(schema) - 1):
        query = query + "%s, "
    query = query + "%s)"
    for data in vals:
        cursor.execute(query, data)
    target.commit()


def transfer_data(source, target, table_name, target_table_name, schema, limit):
    offset = 0
    cursor = source.cursor()
    create_table(target, create_table_query(schema, target_table_name))
    check = True
    if not is_table_empty(target, target_table_name):
        while check:
            query = "SELECT * FROM {} OFFSET {} LIMIT {}".format(table_name, offset, limit)
            cursor.execute(query)
            data_list = cursor.fetchall()
            offset = offset + limit
            check = data_list
            add_data(target, target_table_name, schema, data_list)


def is_table_empty(target, table_Name):
    cursor = target.cursor()
    query = "SELECT * FROM {} LIMIT 1 ".format(table_Name)
    cursor.execute(query)
    is_empty = cursor.fetchone()
    return is_empty


def create_table_query(schema, table_name):
    query = 'CREATE TABLE IF NOT EXISTS {}('.format(table_name)

    columns = []
    for field_name, field_type, max_len in schema:
        if max_len is None:
            columns.append(" ".join([field_name, field_type]))
        else:
            columns.append(" ".join([field_name, field_type]) + "({})".format(max_len))
    _columns = ", ".join(columns)
    query = query + _columns + ')'
    return query


def create_table(target, query):
    cursor = target.cursor()
    cursor.execute(query)
    target.commit()


def get_schema(conn, table_name):
    cursor = conn.cursor()

    query = """SELECT column_name, udt_name, character_maximum_length FROM information_schema.columns WHERE table_schema = 'public' 
            AND table_name = '{}' """.format(table_name)
    cursor.execute(query)
    schema = cursor.fetchall()
    return schema


if __name__ == '__main__':
    schema = get_schema(source_connection, "blu_mu_tv")
    transfer_data(source_connection, target_connection, "blu_mu_tv", "blu_tv", schema, 30)
