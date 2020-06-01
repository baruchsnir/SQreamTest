# !/usr/bin/python
import psycopg2
from config import config
import os

def get_fileds_from_t1_that_not_in_t2_one_query():
    try:
        sql = '''
        SELECT t1.first_name
        FROM t1
        LEFT JOIN t2 ON t1.first_name = t2.first_name
        WHERE t2.first_name IS NULL
        '''
        # read database configuration
        params = config('database.ini')
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the select query
        cur.execute(sql)
        rows = cur.fetchall()
        print('------------------------------------------')
        print('Task 1 first query results')
        for row in rows:
            print(str(row))
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in get_fileds_from_t1_that_not_in_t2 - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    finally:
        if conn is not None:
            conn.close()


def get_fileds_from_t1_that_not_in_t2_second_query():
    try:
        sql = '''
        SELECT t1.first_name
        FROM t1
        WHERE t1.first_name not in (select distinct first_name from t2);
        '''
        # read database configuration
        params = config('database.ini')
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the select query
        cur.execute(sql)
        rows = cur.fetchall()
        print('------------------------------------------')
        print('Task 1 second query results')
        for row in rows:
            print(str(row))
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in get_fileds_from_t1_that_not_in_t2 - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    get_fileds_from_t1_that_not_in_t2_one_query()
    get_fileds_from_t1_that_not_in_t2_second_query()
