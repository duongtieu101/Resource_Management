import pyodbc
from .sql_queries import create_table_queries, drop_table_queries, insert_table_queries

def create_database(database_server, password):
    """
    Create database if not exists
    Input: 
        driver: Driver.
        database_server: database server.
    Output:
        conn: connection to database.
        cur: cursor.
        
    """
    
    # connect to default database
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}' + 
                        ';SERVER=' + database_server + 
                        ';UID=' + 'sa' + 
                        ';PORT=1434' +
                        ';PWD=' + password +
                        ';database=master',
                        autocommit=True
                      )

    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    # cur.execute('DROP DATABASE IF EXISTS InternalProj')
    cur.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'InternalProj') \
                    BEGIN \
                    CREATE DATABASE InternalProj; \
                    END;")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}' + 
                      ';SERVER=' + database_server + 
                      ';UID=' + 'sa' + 
                      ';PORT=1434' +
                      ';PWD=' + password +
                      ';database=InternalProj',
                      autocommit=True
                      )
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    Input:
        conn:connection to db
        cur: cursor that run the query
    Output: Drop all tables that in drop_table_queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    Input:
        conn:connection to db
        cur: cursor that run the query
    Output: Create all tables that in create_table_queries list
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def load(database_server, password, tables_list):
    """
    Load dataframe to table on database
    Input:
        tables_list: list of dataframe of tables need to load.
    Output:
        None
    """
    print("Starting load data to data warehouse")
    cur, conn = create_database(database_server, password)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    for index in range(len(tables_list)):
        for i, row in tables_list[index].iterrows():
            cur.execute(insert_table_queries[index], list(row))
    
    cur.execute("SELECT * FROM dim_source")
    for row in cur.fetchall():
        print(row)
    conn.commit()

