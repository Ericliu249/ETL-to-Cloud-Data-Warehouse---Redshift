import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Description: This function is used for loading 
    data from S3 to redshift staging tables.
    
    Args:
        cur: the cursor object
        conn: the database connection object
        
    Returns:
        None
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Description: This function is used for inserting data to 
    the fact and dimension tables from redshift staging tables.
    
    Args:
        cur: the cursor object
        conn: the database connection object
        
    Returns:
        None
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Description: This function is used to connect to the Redshift cluster, 
    do ETL job from S3 to redshift staging tables and finally to the analytics tables.
    
    Args:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()