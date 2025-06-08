import os
import tempfile
import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='hoa0976271476', dbname=DATABASE_NAME):
    conn_string = f"dbname='{dbname}' user='{user}' host='localhost' password='{password}'"
    return psycopg2.connect(conn_string)


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute("create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    cur.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
    cur.close()
    con.commit()

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