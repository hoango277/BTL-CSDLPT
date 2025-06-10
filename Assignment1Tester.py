DATABASE_NAME = 'dds_assgn1'


RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file

import psycopg2
import traceback
import time
import testHelper
import Interface1 as MyAssignment

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            # Test loadratings với timing
            start_time = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            execution_time = end_time - start_time
            if result :
                print(f"loadratings function pass! Thời gian thực thi: {execution_time:.4f} giây")
            else:
                print(f"loadratings function fail! Thời gian thực thi: {execution_time:.4f} giây")

            # Test rangepartition với timing
            start_time = time.time()
            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            execution_time = end_time - start_time
            if result :
                print(f"rangepartition function pass! Thời gian thực thi: {execution_time:.4f} giây")
            else:
                print(f"rangepartition function fail! Thời gian thực thi: {execution_time:.4f} giây")

            # Test rangeinsert với timing
            start_time = time.time()
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
            end_time = time.time()
            execution_time = end_time - start_time
            if result:
                print(f"rangeinsert function pass! Thời gian thực thi: {execution_time:.4f} giây")
            else:
                print(f"rangeinsert function fail! Thời gian thực thi: {execution_time:.4f} giây")

            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            # Test roundrobinpartition với timing
            start_time = time.time()
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end_time = time.time()
            execution_time = end_time - start_time
            if result :
                print(f"roundrobinpartition function pass! Thời gian thực thi: {execution_time:.4f} giây")
            else:
                print(f"roundrobinpartition function fail! Thời gian thực thi: {execution_time:.4f} giây")

            # Test roundrobininsert với timing
            start_time = time.time()
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
            end_time = time.time()
            execution_time = end_time - start_time
            if result :
                print(f"roundrobininsert function pass! Thời gian thực thi: {execution_time:.4f} giây")
            else:
                print(f"roundrobininsert function fail! Thời gian thực thi: {execution_time:.4f} giây")

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()