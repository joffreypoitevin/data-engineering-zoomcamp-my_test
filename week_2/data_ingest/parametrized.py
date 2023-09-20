from datetime import timedelta
import pyarrow.parquet as pq
from sqlalchemy import *
import psycopg2
import pandas as pd
import wget
import argparse
import logging
import os 
from sqlalchemy_utils import *
import prefect
import pyarrow as pa
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials, GcsBucket
from pathlib import Path
from prefect_gcp.bigquery import bigquery_load_cloud_storage


### SCRIPT TO INGEST DATA AND QUERY ###

#set level of logging to be displayed
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

#default query used
def default_sql_query(table):
    return f"""SELECT * FROM {table} limit 100"""


@prefect.task(log_prints=True)
def get_data(my_url=None):

    """
    my_url: url that points to data (support csv or parquet file).
    
    return: an pandas df with the data 
    """
    #if no url is mentioned, we return the local data ny taxi df
    if my_url is None:
        trips = pq.read_table("yellow_tripdata_2021-01.parquet")
        logging.info("no url specified. we get the yellow cab data 2021-01")
        df = trips.to_pandas()
        return df
    
    #check first if the url point out to an existing local datafile
    if os.path.exists(my_url):
            logging.info(f"url used to point to a local file. local file {my_url} used.")
            filename = my_url
    #otherwise attempt to download the data locally from an external source
    else:
        #download data
        try: 
            filename = os.path.dirname(os.path.abspath(__file__)) + '/' + os.path.basename(my_url)
            #if the data are already existing locally, we override them (a local dataset might be outdated)
            if os.path.exists(filename):
                logging.info("datafile already downloaded. Remove it and redownload it.")
                os.remove(filename)
            filename = wget.download(my_url)
        except:
            raise ValueError("couldn't get data, check url.")

    #read data accept csv or parquet as pandas df
    try:
        if filename.endswith('.parquet'):
            trips = pq.read_table(filename)
            df = trips.to_pandas()
        elif filename.endswith('.csv'):
            df = pd.read_csv(filename)
    except:
        raise ValueError("accept only csv file or parquet")
    
    logging.info(f"filename {filename} downloaded and stored as pandas df")
    print(f"filename {filename} downloaded and stored as pandas df")
    print(df)
    return df

@prefect.task(log_prints=True)#, retries=3, cache_key_fn=task_input_hash ,cache_expiration=timedelta(days=1))
def store_table_in_db(df,user,password,host,port,name_db, name_table, if_exists='replace'):
    """
    function that stored the df into the db

    Args:
        df (Pandas DataFrame): data to be stored
        user (str): user name in db 
        password (str): password for the db
        host (str): host machine that is connected to db
        port (str): port that connect the machine the db
        name_db (str): name of the db in postgresql
        name_table (str): name given to df in the database
        if_exists (str, optional): how to handle data if the table is already existing in the db: support 'replace','append' or 'fail'. Defaults to 'replace'.
    """

    #get a connection to postgresql (locally running via docker)
    #dialect and driver postgresql and psycopg2 already determined
    db = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name_db}')
    # Create database if it does not exist.
    if not database_exists(db.url):
        logging.info(f"database does not exist. Database {name_db} is created.")
        create_database(db.url)
    #if the database already exists in the server
    else:
        logging.info(f"database {name_db} exists.")
    #inspect the db to check if the table exists already in the db and inform the user (@todo change design lolol)        
    ins = inspect(db)
    if not ins.dialect.has_table(db.connect(),name_table):
        logging.info(f"table {name_table} was not found inside the database. Creating one and store the data")
    else:
        print(f"table {name_table} already existing. {if_exists} with new data.")
        logging.info(f"table {name_table} already existing. {if_exists} with new data.")
    #store data into db as table with table name, replace, append or fails if already exists
    with db.connect().execution_options(autocommit=True) as conn:
        try:
            df.to_sql(f'{name_table}', con=conn, if_exists=if_exists, index= False)
            print("data stored.")
            logging.info(f"data stored.")
        except ValueError:
            print("data was not stored.")
            logging.info("data was not stored.")
            pass 

@prefect.flow(name='subflow_collect_store_data')
def subflow_collect_store_data(user,password,host,port,name_db, name_table, if_exists='replace',my_url=None ):
    """_summary_

    Args:
        user (_type_): _description_
        password (_type_): _description_
        host (_type_): _description_
        port (_type_): _description_
        name_db (_type_): _description_
        name_table (_type_): _description_
        if_exists (str, optional): _description_. Defaults to 'replace'.
        my_url (_type_, optional): _description_. Defaults to None.
    """
    data = get_data(my_url)
    store_table_in_db(data,user,password,host,port,name_db, name_table, if_exists='replace')




@prefect.task(log_prints=True)#, retries=3, cache_key_fn=task_input_hash ,cache_expiration=timedelta(days=1))
def query_data_from_table(user,password,host,port,name_db, name_table, sql_query=None):
    """ query to operate in the db

    Args:
        user (str): user name in db 
        password (str): password for the db
        host (str): host machine that is connected to db
        port (str): port that connect the machine the db
        name_db (str): name of the db in postgresql
        name_table (str): name given to df in the database
        sql_query (str, optional): sql query to do. Defaults to None. If None, will use the default query

    Returns:
        pd.DataFrame: the queried data
    """

    #get a connection to postgresql (locall running via docker)
    db = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name_db}')

    if sql_query is None:
        sql_query = default_sql_query(name_table)
        logging.info("default query: first 100 rows") 
    #perfom query from db
    with db.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql_query)) 
    
    df = pd.DataFrame(query.fetchall())
    print("QUERY")
    print(df)
    return df

@prefect.task(log_prints=True)#, retries=3, cache_key_fn=task_input_hash ,cache_expiration=timedelta(days=1))
def transform_data(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    df = df.iloc[:, :10]
    df['transformation'] = "this is an artificial transformation just to practice prefect"
    
    return df

@prefect.flow(name='subflow_query_transform_data')
def subflow_query_transform_data(user,password,host,port,name_db, name_table, sql_query=None):
    """_summary_

    Args:
        user (_type_): _description_
        password (_type_): _description_
        host (_type_): _description_
        port (_type_): _description_
        name_db (_type_): _description_
        name_table (_type_): _description_
        sql_query (_type_, optional): _description_. Defaults to None.
    """
    data = query_data_from_table(user,password,host,port,name_db, name_table, sql_query=None)
    transformed_data = transform_data(data)

    return transformed_data

@prefect.task(log_prints=True)#, retries=3, cache_key_fn=task_input_hash ,cache_expiration=timedelta(days=1))
def collect_db_credentials(params):
    """

    Args:
        params (_type_): _description_

    Returns:
        _type_: _description_
    """

        #database params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    name_db = params.name_db

    return user, password, host, port, name_db

@prefect.task(log_prints=True)#, retries=3, cache_key_fn=task_input_hash ,cache_expiration=timedelta(days=1))
def collect_needed_data(params):
    """

    Args:
        params (_type_): _description_

    Returns:
        _type_: _description_
    """
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

    return name_table, if_exists, url, sql_query 

@prefect.flow(name="handle_parameters")
def subflow_handle_parameters(params):
    """_summary_

    Args:
        params (_type_): _description_

    Returns:
        _type_: _description_
    """
    user, password, host, port, name_db = collect_db_credentials(params)

    name_table, if_exists, url, sql_query = collect_needed_data(params)

    return user, password, host, port, name_db, name_table, if_exists, url, sql_query


@prefect.task(log_prints=True, retries=3)
def write_data_locally_csv(data, url):
    """ write data locally 

    Args:
        pd dataframe: path to the data

    Returns:
        None
    """

    # Use the os.path.basename() function to get the file name
    file_name_with_extension = os.path.basename(url)
    # Use os.path.splitext() to split the file name and extension
    file_name, file_extension = os.path.splitext(file_name_with_extension)

    # Use os.path.splitext() to split the file name and extension
    file_name, file_extension = os.path.splitext(file_name_with_extension)
    
    file_name = file_name + "_100"

    # Create a csv from the Pandas DataFrame
    data.to_csv(file_name, index=False)

    print(f'csV CREATED.') 

    return file_name 

@prefect.task(log_prints=True, retries=3)
def write_on_gcs(csv_file_path):
    """_summary_

    Args:
        path (_type_): _description_

    Returns:
        _type_: _description_
    """

    gcs_bucket_block = GcsBucket.load("gcs-bucket")
    gcs_path = gcs_bucket_block.upload_from_path(csv_file_path)

    print(f' File {csv_file_path} written on {gcs_path}.')  

    
@prefect.flow(name="store_transformed_df_on_gcs")
def subflow_store_on_gcs(data, url):
    """_summary_

    Args:
        data (_type_): _description_

    Returns:
        _type_: _description_
    """

    file_name = write_data_locally_csv(data, url)

    write_on_gcs(file_name)

    return file_name



@prefect.flow(name="from_gcs_transform_to_bq")
def subflow_store_on_gbq(path_from_gcs, file_name):

    path_from_gcs = "gs://week_2_datalake_datacamp2/week_2_datalake_datacamp2/week_2/" + file_name
    
    print("PATH TO GCS")
    print(path_from_gcs)
    
    #get credential block
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")

    #df.to_gbq(destination_table= "datacamp2.football.football1819", project_id="datacamp2", credentials = gcp_credentials_block.get_credentials_from_service_account() , if_exists="replace", chunksize=500_000)
    result = bigquery_load_cloud_storage(
        dataset="football",
        table=file_name,
        uri=path_from_gcs,
        gcp_credentials=gcp_credentials_block,
        location="EU"
    )
    return result


@prefect.flow(name="collect_storeDB_query_storeGCS_storeGBQ")
def mainflow(user, password, host, port, name_db, name_table, if_exists, url, sql_query):
    """
    execute the whole pipeline:
    -get_data
    -store_data
    -query_data
    -transform_data


    user = params.user default: root
    password = params.password default: root
    host = params.host default pg_admin (container name of postresql image)
    port = params.port default 5432 (port of pg_admin container)
    name_db = params.name_db (default: my_db)
    name_table = params.name_table (default ny_taxi) table that is stored by default.
    url=params.url default None, ny_taxi data (yellow_tripdata_2021-01.parquet) by default
    sql_query: count by default

    return: query data
    
    """

    # Check if url is a list
    if isinstance(url, list):
        # If it's a list, iterate through the elements and apply mainflow
        for link in url:
        #collect data and store data
            subflow_collect_store_data(user,password,host,port,name_db, name_table, if_exists, link)

            #simple query
            data = subflow_query_transform_data(user,password,host,port,name_db, name_table, sql_query)

            #store data locally and on gcs
            file_name = subflow_store_on_gcs(data, link)

            path_from_gcs = "gs://week_2_datalake_datacamp2/week_2_datalake_datacamp2/week_2/football1819_100.csv"

            #store the data on gbq
            subflow_store_on_gbq(path_from_gcs, file_name)
    else:
            # If it's not a list, simply apply mainflow to the single URL

            #collect data and store data
            subflow_collect_store_data(user,password,host,port,name_db, name_table, if_exists, url)

            #simple query
            data = subflow_query_transform_data(user,password,host,port,name_db, name_table, sql_query)

            #store data locally and on gcs
            file_name = subflow_store_on_gcs(data, url)

            path_from_gcs = "gs://week_2_datalake_datacamp2/week_2_datalake_datacamp2/week_2/" 

            #store the data on gbq
            subflow_store_on_gbq(path_from_gcs, file_name)




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

    path_from_gcs = "week_2_datalake_datacamp2/week_2_datalake_datacamp2/week_2/football1819.parquet.gz"
   
    #collect db credentials and data information
    user, password, host, port, name_db, name_table, if_exists, url, sql_query = subflow_handle_parameters(params)

    #run main flow: collect, store in db, query, store in GCS, transfer to GBQ
    mainflow(user, password, host, port, name_db, name_table, if_exists, url, sql_query)








