from datetime import timedelta
import pyarrow.parquet as pq

import pandas as pd
import wget
import argparse
import logging
import os 
from sqlalchemy_utils import *
import prefect
from prefect.tasks import task_input_hash
import pyarrow as pa
from prefect_gcp import GcpCredentials, GcsBucket
from pathlib import Path

@prefect.task(log_prints=True)
def extract_from_gcs(file='transformed_data.parquet.gz'):
    """_summary_

    Args:
        path_file (str, optional): _description_. Defaults to "transformed_data.gz".
    """
    #connect to gcsbucket
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcsgcp")

    data_name = "data_to_bq.parquet.gz"

    #get directory where the data are   
    gcp_cloud_storage_bucket_block.download_object_to_path(file, data_name)
    
    return  Path(data_name).resolve()

@prefect.task(log_prints=True, retries=3)
def transform_data_for_bq(path_file):
    """_summary_

    Args:
        path_file (_type_): _description_
    """

    # Read the Parquet file using pyarrow
    table = pq.read_table(path_file)

    # Convert the pyarrow table to a pandas DataFrame
    df = table.to_pandas()

    #transform
    df['bq_transformed'] = "transform to be in bq"

    print("data transformed for bq")

    return df

@prefect.task(log_prints=True, retries=3)
def write_to_bq(df):
    """

    Args:
        df (_type_): _description_
    """
    #get credential block
    gcp_credentials_block = GcpCredentials.load("gcpcredential")

    df.to_gbq(destination_table= "week_2_data.gcs_to_bq_test", project_id="datacamp-392412", credentials = gcp_credentials_block.get_credentials_from_service_account() , if_exists="replace", chunksize=500_000)


@prefect.flow(name="from_gcs_transform_to_bq")
def flow_store_on_gbq(path_file):

    path = extract_from_gcs(path_file)

    data = transform_data_for_bq(path)

    write_to_bq(data)

if __name__ == "__main__":

    path_file_from_bucket = "transformed_data.parquet.gz"

    flow_store_on_gbq(path_file_from_bucket)