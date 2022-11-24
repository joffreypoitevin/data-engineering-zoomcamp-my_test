import pyarrow.parquet as pq
from sqlalchemy import *
import psycopg2
import pandas as pd
import wget
import argparse
import logging
import os 
from sqlalchemy_utils import *

### SCRIPT TO INGEST DATA AND QUERY ###

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

def default_sql_query(table):
    return f"""SELECT COUNT(*) FROM {table}"""

def get_data(my_url=None):

    #if no url is mentioned
    if my_url is None:
        trips = pq.read_table("yellow_tripdata_2021-01.parquet")
        logging.info("no url specified. we get the yellow cab data 2021-01")
        df = trips.to_pandas()
        return df

    #download data
    try: 
        filename = os.path.dirname(os.path.abspath(__file__)) + '/' + os.path.basename(my_url)
        if os.path.exists(filename):
            logging.info("datafile already downloaded. Remove it and redownload it.")
            os.remove(filename)
        filename = wget.download(my_url)
    except:
        raise ValueError("couldn't get data, check url.")

    #read data accept csv or parquet
    try:
        if filename.endswith('.parquet'):
            trips = pq.read_table(filename)
            df = trips.to_pandas()
        elif filename.endswith('.csv'):
            df = pd.read_csv(filename)
    except:
        raise ValueError("accept only csv file or parquet")
    
    logging.info(f"filename {filename} downloaded and stored as pandas df")
    return df

def store_table_in_db(df,user,password,host,port,name_db, name_table, if_exists='replace'):

    #if if_exists not in ['replace', 'append', 'fail']:
    #    raise ValueError("argument if_exists should be fail, append or replace")

    #get a connection to postgresql (locall running via docker)
    #dialect and driver already determined
    db = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name_db}')
    # Create database if it does not exist.
    if not database_exists(db.url):
        logging.info(f"database does not exist. Database {name_db} is created.")
        create_database(db.url)
    else:
        logging.info(f"database {name_db} exists.")    
    ins = inspect(db)
    if not ins.dialect.has_table(db.connect(),name_table):
        logging.info(f"table {name_table} was not found inside the database. Creating one and store the data")
    else:
        logging.info(f"table {name_table} already existing. {if_exists} with new data.")
    #store data into db as table with table name, replace, append or fails if already exists
    with db.connect().execution_options(autocommit=True) as conn:
        try:
            df.to_sql(f'{name_table}', con=conn, if_exists=if_exists, index= False)
            logging.info(f"data stored.")
        except ValueError:
            logging.info("data was not stored.")
            pass 


def query_data_from_table(user,password,host,port,name_db, name_table, sql_query=None):

    #get a connection to postgresql (locall running via docker)
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name_db}')

    if sql_query is None:
        sql_query = default_sql_query(name_table)
        logging.info("default query: count") 
    #perfom query from db
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql_query)) 
    
    return pd.DataFrame(query.fetchall())
  

def main(params):
    """
    user = params.user default: root
    password = params.password default: root
    host = params.host default pg_admin (container name of postresql image)
    port = params.port default 5432 (port of pg_admin container)
    name_db = params.name_db (default: my_db)
    name_table = params.name_table (default ny_taxi) table that is stored by default.
    url=params.url default None, ny_taxi data (yellow_tripdata_2021-01.parquet) by default
    sql_query: count by default
    
    """

    #database params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    name_db = params.name_db
    name_table = params.name_table
    if params.if_exists:
        if_exists = params.if_exists
    else:
        if_exists = 'replace' 

    #data params
    if params.url:
        url = params.url
    else: 
        url = None

    if params.sql_query:
        sql_query = params.sql_query
    else:
        sql_query = default_sql_query(name_table)

    #collect data
    df = get_data(url)

    #store data
    store_table_in_db(df,user,password,host,port,name_db, name_table, if_exists)

    #simple query
    df_query = query_data_from_table(user,password,host,port,name_db, name_table, sql_query)

    return print(df_query)

if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser()
    
    # Add arguments database
    parser.add_argument('--user', default=str(os.environ.get('user', 'root')), type=str)
    parser.add_argument('--password', default=str(os.environ.get('password', 'root')), type=str)
    parser.add_argument('--host', default=str(os.environ.get('host', 'pg_container')), type=str)
    parser.add_argument('--port', default=str(os.environ.get('port', '5432')), type=str)
    parser.add_argument('--name_db', default=str(os.environ.get('name_db', 'my_db')), type=str)
    parser.add_argument('--name_table', default=str(os.environ.get('name_table', 'ny_taxi')), type=str)
    parser.add_argument('--if_exists', default=str(os.environ.get('if_exists', 'replace')), type=str)

    # Add argument data location
    parser.add_argument('--url', default=os.environ.get('url', None))
    # Add argument query
    parser.add_argument('--sql_query', default=os.environ.get('sql_query', None))
    
    #parse arguments
    params = parser.parse_args()

    main(params)








