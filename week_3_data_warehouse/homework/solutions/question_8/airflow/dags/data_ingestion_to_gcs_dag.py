import os
import logging
import requests
import gzip
import shutil

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq

import pandas as pd


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "fhv_tripdata_2019-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}"
dataset_temp = "/tmp"
parquet_file = dataset_file.replace(".csv.gz", ".parquet")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "zoomcamp")


# Function to download gzip files
def download_gzip_files(url, dest_dir):
    filename = os.path.join(dest_dir, url.split("/")[-1])
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename


# Function to convert gzip files to parquet
def convert_to_parquet(filename):
    with gzip.open(filename, "rb") as f_in:
        with open(filename[:-3], "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    df = pd.read_csv(filename[:-3])
    df.to_parquet(filename[:-7] + ".parquet", index=False)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    # Define the download_gzip_files task
    download_gzip_files_task = PythonOperator(
        task_id="download_gzip_files_task",
        python_callable=download_gzip_files,
        op_args=[
            dataset_url,
            dataset_temp,
        ],
        dag=dag,
    )

    # Define the convert_to_parquet task
    convert_to_parquet_task = PythonOperator(
        task_id="convert_to_parquet_task",
        python_callable=convert_to_parquet,
        op_args=[f"{dataset_temp}/{dataset_file}"],
        dag=dag,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{dataset_temp}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {dataset_temp}/{dataset_file}",
    )

    (
        download_gzip_files_task
        >> convert_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
        >> rm_task
    )
