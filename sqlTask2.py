# !/usr/bin/python
import psycopg2
from config import config
import os


def update_new_tabel_from_tables_T_and_CSV():
    try:
        sql1 = 'SELECT * FROM newtable;'
        sql_create = '''
        DROP TABLE IF EXISTS newtable;

        CREATE TABLE public.newtable
        (
                   NewTable_id SERIAL PRIMARY KEY,
                   id INTEGER NOT NULL,
                   first_name VARCHAR(50) NOT NULL
        )'''
        sql = '''
        INSERT INTO newtable (id, first_name)
        SELECT t1.id,t1.value
        FROM t t1
        LEFT JOIN csv t2 ON t2.id = t1.id
        WHERE t2.id IS NULL
        union
        SELECT csv.id,csv.value from csv
        	 Inner JOIN t on csv.id = t.id
        order by id
        '''
        # read database configuration
        params = config('database.ini')
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        # create a new cursor
        cur = conn.cursor()
        # execute the select query
        cur.execute(sql_create)
        conn.commit()
        cur.execute(sql)
        cur.execute(sql1)
        rows_t1 = cur.fetchall()
        print('------------------------------------------')
        print('Task 2 query results')
        for row in rows_t1:
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
    update_new_tabel_from_tables_T_and_CSV()