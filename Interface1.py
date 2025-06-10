import os
import tempfile
import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='hoa0976271476', dbname=DATABASE_NAME):
    conn_string = f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'"
    return psycopg2.connect(conn_string)


def preprocess_file_to_temp(input_path, sep_char=','):
    temp_fd, temp_path = tempfile.mkstemp(prefix="ratings_processed_", suffix=".csv", text=True)
    try:
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as temp_file:
            with open(input_path, 'r', encoding='utf-8', errors='ignore') as source_file:
                line_data = source_file.readlines()
                for raw_line in line_data:
                    cleaned_line = raw_line.strip()
                    if not cleaned_line:
                        continue

                    columns = cleaned_line.split('::')
                    if len(columns) >= 3:
                        output_row = sep_char.join(columns[:3])
                        temp_file.write(output_row + '\n')
    except:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return temp_path


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    create_db(DATABASE_NAME)
    connection = openconnection
    cursor = connection.cursor()
    drop_query = f"DROP TABLE IF EXISTS {ratingstablename};"
    cursor.execute(drop_query)
    create_query = f"CREATE TABLE {ratingstablename} (userid INTEGER, movieid INTEGER, rating FLOAT);"
    cursor.execute(create_query)
    connection.commit()
    cursor.close()
    processed_file = preprocess_file_to_temp(ratingsfilepath)
    try:
        with connection.cursor() as copy_cursor:
            with open(processed_file, 'r', encoding='utf-8') as data_file:
                copy_command = f"COPY {ratingstablename} (userid, movieid, rating) FROM STDIN WITH CSV DELIMITER ','"
                copy_cursor.copy_expert(copy_command, data_file)
                connection.commit()
    finally:
        if os.path.isfile(processed_file):
            os.unlink(processed_file)


def _create_rrobin_sequence(openconnection):
    connection = openconnection
    cursor = connection.cursor()
    sequence_sql = "CREATE SEQUENCE IF NOT EXISTS rrobin_seq START 1;"
    try:
        cursor.execute(sequence_sql)
        connection.commit()
    except Exception:
        connection.rollback()
    finally:
        cursor.close()


def _get_next_rrobin_index(numberofpartitions, openconnection):
    connection = openconnection
    cursor = connection.cursor()

    next_val_query = "SELECT nextval('rrobin_seq');"
    cursor.execute(next_val_query)
    sequence_value = cursor.fetchone()[0]
    cursor.close()

    partition_index = (sequence_value - 1) % numberofpartitions
    return partition_index


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    connection = openconnection
    cursor = connection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    rating_step = 5.0 / numberofpartitions
    partition_index = 0
    while partition_index < numberofpartitions:
        lower_bound = partition_index * rating_step
        upper_bound = lower_bound + rating_step
        partition_table = RANGE_TABLE_PREFIX + str(partition_index)

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {partition_table} (userid INTEGER, movieid INTEGER, rating FLOAT);"
        cursor.execute(create_table_sql)

        if partition_index == 0:
            insert_sql = f"INSERT INTO {partition_table} SELECT * FROM {ratingstablename} WHERE rating >= {lower_bound} AND rating <= {upper_bound};"
        else:
            insert_sql = f"INSERT INTO {partition_table} SELECT * FROM {ratingstablename} WHERE rating > {lower_bound} AND rating <= {upper_bound};"

        cursor.execute(insert_sql)
        partition_index += 1
    cursor.close()
    connection.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    connection = openconnection
    cursor = connection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    _create_rrobin_sequence(openconnection)

    partition_num = 0
    while partition_num < numberofpartitions:
        partition_name = RROBIN_TABLE_PREFIX + str(partition_num)

        table_creation_sql = f"CREATE TABLE IF NOT EXISTS {partition_name} (userid INTEGER, movieid INTEGER, rating FLOAT);"
        cursor.execute(table_creation_sql)

        data_insertion_sql = f"""INSERT INTO {partition_name} 
                                SELECT userid, movieid, rating 
                                FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS row_seq 
                                      FROM {ratingstablename}) AS numbered_data 
                                WHERE MOD(numbered_data.row_seq-1, {numberofpartitions}) = {partition_num};"""
        cursor.execute(data_insertion_sql)

        partition_num += 1

    cursor.close()
    connection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    connection = openconnection
    cursor = connection.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    _create_rrobin_sequence(openconnection)

    main_insert_sql = f"INSERT INTO {ratingstablename}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});"
    cursor.execute(main_insert_sql)

    total_partitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    target_partition_index = _get_next_rrobin_index(total_partitions, openconnection)
    target_partition_name = RROBIN_TABLE_PREFIX + str(target_partition_index)

    partition_insert_sql = f"INSERT INTO {target_partition_name}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});"
    cursor.execute(partition_insert_sql)

    cursor.close()
    connection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    connection = openconnection
    cursor = connection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    total_partitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    if total_partitions == 0:
        raise Exception("Partitions have not been created. Call rangepartition first.")
    rating_step = 5.0 / total_partitions
    partition_idx = int(rating / rating_step)

    if partition_idx >= total_partitions:
        partition_idx = total_partitions - 1
    elif rating % rating_step == 0 and rating != 0:
        partition_idx = max(partition_idx - 1, 0)

    target_table = RANGE_TABLE_PREFIX + str(partition_idx)
    insert_query = f"INSERT INTO {target_table}(userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});"
    cursor.execute(insert_query)

    cursor.close()
    connection.commit()


def create_db(dbname):
    postgres_conn = getopenconnection(dbname='postgres')
    postgres_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    db_cursor = postgres_conn.cursor()

    check_db_query = "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='%s'" % (dbname,)
    db_cursor.execute(check_db_query)
    db_exists = db_cursor.fetchone()[0]

    if db_exists == 0:
        create_db_query = 'CREATE DATABASE %s' % (dbname,)
        db_cursor.execute(create_db_query)

    db_cursor.close()
    postgres_conn.close()


def count_partitions(prefix, openconnection):
    connection = openconnection
    cursor = connection.cursor()

    count_query = f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%';"
    cursor.execute(count_query)
    partition_count = cursor.fetchone()[0]
    cursor.close()

    return partition_count 