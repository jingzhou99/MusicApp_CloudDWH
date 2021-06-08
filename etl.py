import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from log import logger


def load_staging_tables(cur, conn):
    """Load data from S3 bucket to staging tables.
    
    Description: 
        Load data from S3 bucket to staging tables.
        Make sure has access to S3.        
    Args:
        cur: cursor
        conn: connection to redshift cluster
    Return:
        None
    """
    print('Start loading staging tables...')
    
    for query in copy_table_queries:
        print('executing',query,'this process might takes several minutes...be patient.')
        cur.execute(query)
        conn.commit()
        
    print('All staging tables loaded')
    
def insert_tables(cur, conn):
    """Insert data into fact and dimensional tables.
    
    Description: 
        Transform data in staging table, 
        Use temp tables for the upsert process, to remove duplicates.        
    Args:
        cur: cursor
        conn: connection to redshift cluster
    Return:
        None
    """
    print('Start inserting job...')
    
    for query in insert_table_queries:
        print('executing',query,'this process might takes several minutes...be patient.')
        cur.execute(query)
        conn.commit()

    print('All data inserted.')
    
def main():
    """
    Connect to the redshift cluster, and get the cursor
    Call function 'load_staging_tables' to load data from S3 bucket.
    Call function 'insert_tables' to transform and upsert into star schema tables.
    Close connection to redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')


    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()
    
    print('ETL job done.')

    
if __name__ == "__main__":
    main()