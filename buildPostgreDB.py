# !/usr/bin/python
import psycopg2
from config import config
import os



def createDb():
    conn = None
    exist_one = False
    sql = '''CREATE DATABASE sqltasks 
     WITH ENCODING='UTF8'
     OWNER=postgres
     CONNECTION LIMIT=25;'''

    sql_find = 'SELECT datname FROM pg_database WHERE datname = \'sqltasks\''
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        # create a cursor
        cursor = conn.cursor()
        cursor.execute(sql_find)
        rows = cursor.fetchall()
        if len(rows) > 0:
            exist_one = True
            print("Find our DataBase \'sqlasks\'........")
        if not exist_one:
            # print('Create new database')
            cursor.execute(sql)
            print("Database created successfully........")
        # Closing the connection
        conn.close()
    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in createDb - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS T1 (
            t1_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS T2 (
           t2_id SERIAL PRIMARY KEY,
           id INTEGER NOT NULL,
           first_name VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS T (
           t_id SERIAL PRIMARY KEY,
           id INTEGER NOT NULL,
           value VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS CSV (
           t_id SERIAL PRIMARY KEY,
           id INTEGER NOT NULL,
           value VARCHAR(50) NOT NULL
        )
        """)
    conn = None
    try:
        # read connection parameters
        params = config()
        params['database'] = 'sqltasks'
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        # create a cursor
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in create_tables - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
def add_data_to_tablse():
    """ insert multiple values into the tables  """
    sqls = [ 'SELECT * from public.T1',
             'SELECT * from public.T2',
             'SELECT * from public.T',
             'SELECT * from public.CSV'
    ]
    sqls_insert = ['INSERT INTO public.T1(	first_name, last_name) VALUES(%s,%s);',
            'INSERT INTO public.T2(	id, first_name) VALUES(%s,%s);',
            'INSERT INTO public.T(	id, value) VALUES(%s,%s);',
            'INSERT INTO public.CSV(	id, value) VALUES(%s,%s);',
            ]
    lists = [
             [('dan', 'shamir'),
              ('moshe', 'cohen'),
              ('dudi', 'amsalem'),
              ('uri', 'nir'),
              ('dov', 'levi')],
             [(1,'dan'),
              (3,'kely'),
              (11,'dudi'),
              (10,'asaf'),
              (12,'dov')],
             [(1,'AAA'),
              (2,'BBB'),
              (3,'CCC'),
              (4,'DDD'),
              (5,'EEE'),
              (6,'FFF')],
              [(1,'AAA'),
               (2,'ZZZ'),
               (3,'KKK'),
               (9,'GGG'),
               (11,'YYY'),
             ]
    ]
    conn = None
    try:
        # read database configuration
        params = config('database.ini')
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        for i in range(len(sqls_insert)):
            sql = sqls[i]
            cur.execute(sql)
            rows = cur.fetchall()
            # We only insert for blank table
            if len(rows) == 0:
                sql = sqls_insert[i]
                list = lists[i]
                cur.executemany(sql,list)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as ex:
        template = "Problem in add_data_to_tablse - An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print('Exception - ', message)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    createDb()
    create_tables()
    add_data_to_tablse()