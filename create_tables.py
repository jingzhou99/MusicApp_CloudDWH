import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from log import logger


def drop_tables(cur, conn):
    """drop all table before creating new schema"""
    
    print('drop all tables if exists')
    for query in drop_table_queries:
        print('executing',query)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """create staging tables, and new schema"""
    
    print('start creating new schema')
    for query in create_table_queries:
        print('executing',query)
        cur.execute(query)
        conn.commit()

def main():
    """
    Parse .cfg file
    Connect to redshift cluster
    Create staging tables and new schema
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('All tables been created.')

if __name__ == "__main__":
    main()
