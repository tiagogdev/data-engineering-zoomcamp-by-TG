# Week 3 Homework

**IMPORTANT NOTE:** You can load the data however you would like, but keep the files in `.GZ` Format. If you are using orchestration such as
Airflow or Prefect do not load the data into Big Query using the orchestrator. Stop with loading the files into a
bucket.

**NOTE:** You can use the `CSV` option for the `GZ` files when creating an External Table.

**SETUP:** Create an external table using the `fhv 2019` data.  
Create a table in BigQuery using the `fhv 2019` data (do not partition or cluster this table).  
Data can be found here: <https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv>

## Question 1

What is the count for fhv vehicle records for year 2019?

- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

## Solution 1

For this homework I used Apache Airflow.

Create a python script to load our data from GitHub repo to Google Cloud Storage (GCS).

**File `data_ingestion_to_gcs_dag.py`**

``` python
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
```

Create a environment file:

``` 
COMPOSE_PROJECT_NAME=EXAMPLE
GOOGLE_APPLICATION_CREDENTIALS=EXAMPLE
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=EXAMPLE
AIRFLOW_UID=EXAMPLE
GCP_PROJECT_ID=EXAMPLE
GCP_GCS_BUCKET=EXAMPLE

# Postgres
POSTGRES_USER=EXAMPLE
POSTGRES_PASSWORD=EXAMPLE
POSTGRES_DB=EXAMPLE
```

Create a dockerfile.

``` dockerfile
# First-time build can take upto 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID
```

Create a docker compose.
 
``` yaml
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Basic Airflow cluster configuration for CeleryExecutor with Redis and PostgreSQL.
#
# WARNING: This configuration is for local development. Do not use it in a production deployment.
#
# This configuration supports basic configuration using environment variables or an .env file
# The following variables are supported:
#
# AIRFLOW_IMAGE_NAME           - Docker image name used to run Airflow.
#                                Default: apache/airflow:2.2.3
# AIRFLOW_UID                  - User ID in Airflow containers
#                                Default: 50000
# Those configurations are useful mostly in case of standalone testing/running Airflow in test/try-out mode
#
# _AIRFLOW_WWW_USER_USERNAME   - Username for the administrator account (if requested).
#                                Default: airflow
# _AIRFLOW_WWW_USER_PASSWORD   - Password for the administrator account (if requested).
#                                Default: airflow
# _PIP_ADDITIONAL_REQUIREMENTS - Additional PIP requirements to add when starting all containers.
#                                Default: ''
#
# Feel free to modify this file to suit your needs.
---
version: '3'
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  build:
    context: .
    dockerfile: ./Dockerfile
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKEND: 'airflow.api.auth.backend.basic_auth'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
    GOOGLE_APPLICATION_CREDENTIALS: /.google/credentials/google_credentials.json
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: 'google-cloud-platform://?extra__google_cloud_platform__key_path=/.google/credentials/google_credentials.json'
    GCP_PROJECT_ID: 'wise-ally-376418'
    GCP_GCS_BUCKET: "tg-prefect-de-zoomcamp"

  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ~/.google/credentials/:/.google/credentials:ro

  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # yamllint disable rule:line-length
    command:
      - -c
      - |
        function ver() {
          printf "%04d%04d%04d%04d" $${1//./ }
        }
        airflow_version=$$(gosu airflow airflow version)
        airflow_version_comparable=$$(ver $${airflow_version})
        min_airflow_version=2.2.0
        min_airflow_version_comparable=$$(ver $${min_airflow_version})
        if (( airflow_version_comparable < min_airflow_version_comparable )); then
          echo
          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"
          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"
          echo
          exit 1
        fi
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#setting-the-right-airflow-user"
          echo
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#before-you-begin"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    # yamllint enable rule:line-length
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
    user: "0:0"
    volumes:
      - .:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    # Workaround for entrypoint issue. See: https://github.com/apache/airflow/issues/16252
    command:
      - bash
      - -c
      - airflow

volumes:
  postgres-db-volume:
```

Buckets on Google Cloud.
![q1_i01](../../images/hw3/q1_BigQuery.png)

Now, create external table, without partition or cluster, by running this query.

```sql
CREATE OR REPLACE EXTERNAL TABLE `wise-ally-376418.zoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://tg-prefect-de-zoomcamp/data/nyc-tl-data/trip data/fhv_tripdata_2019-*.csv']
);
```

Run this query.

```sql
SELECT COUNT(*) FROM `wise-ally-376418.zoomcamp.fhv_tripdata`; 
```
![q1_i02](../../images/hw3/q2_nr_rows.png)


## Question 2

Write a query to count the distinct number of `affiliated_base_number` for the entire dataset on both the tables. What
is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table

## Solution 2

Run these queries.

``` sql
CREATE OR REPLACE TABLE `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned` AS
SELECT * FROM wise-ally-376418.zoomcamp.fhv_tripdata;
```

``` sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `wise-ally-376418.zoomcamp.external_fhv_tripdata`; 
```

![q2_i01](../../images/hw3/q3_query_count_external.png)

``` sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `wise-ally-376418.zoomcamp.fhv_tripdata`; 
```
![q2_i02](../../images/hw3/q3_query_count.png)

## Question 3

How many records have both a blank (null) `PUlocationID` and `DOlocationID` in the entire dataset?

- 717,748
- 1,215,687
- 5
- 20,332

## Solution 3

``` sql
SELECT COUNT(dispatching_base_num) FROM `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

![q3_i01](../../images/hw3/q4_query_count.png)


## Question 4

What is the best strategy to optimize the table if query always filter by `pickup_datetime` and order by
`affiliated_base_number`?

- Cluster on `pickup_datetime` Cluster on `affiliated_base_number`
- Partition by `pickup_datetime` Cluster on `affiliated_base_number`
- Partition by `pickup_datetime` Partition by `affiliated_base_number`
- Partition by `affiliated_base_number` Cluster on `pickup_datetime`

## Solution 4

Partitioning by pickup_datetime and clustering on affiliated_base_number would likely be the best strategy for optimizing the table in this case. Partitioning the table on pickup_datetime would allow for efficient filtering based on pickup datetime, and clustering the table on affiliated_base_number would further improve query performance when sorting by this column

**A: Partition by pickup_datetime Cluster on affiliated_base_number**

## Question 5

Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct
`affiliated_base_number` between `pickup_datetime` 03/01/2019 and 03/31/2019 (inclusive).

Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from
clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these
values? Choose the answer which most closely matches.

- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

## Solution 5

``` sql
CREATE OR REPLACE TABLE `wise-ally-376418.zoomcamp.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `wise-ally-376418.zoomcamp.fhv_tripdata`;
```

``` sql
SELECT DISTINCT(affiliated_base_number) 
FROM `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

![q5_i01](../../images/hw3/q5_query_distinct_cost_non_partitoned.png)

``` sql
SELECT DISTINCT(affiliated_base_number) 
FROM `wise-ally-376418.zoomcamp.fhv_tripdata_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

![q5_i02](../../images/hw3/q5_query_distinct_cost_partitoned_clustered.png)

## Question 6

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

## Solution 6

External tables are similar to standard Big Query tables, in that these tables store their metadata and schema in Big Query storage. However, their data resides in an external source. External tables are contained inside a dataset, and you manage them in the same way that you manage a standard Big Query table. External tables in Big Query are used to access data stored in external storage systems without having to load the data into Big Query.

**A: GCP Bucket**

## Question 7

It is best practice in Big Query to always cluster your data:

- True
- False 

## Solution 7

It is not always a best practice in Big Query to cluster your data. Clustering can improve query performance for certain types of queries that filter or group by the clustered column(s), but it may also add additional complexity and cost to your data storage and management.

Whether or not to cluster data in Big Query depends on the specific use case and the types of queries that will be run against the data. If the queries that will be run against the data often filter or group by a specific set of columns, then clustering the data on those columns may improve performance. However, if the queries do not benefit from clustering or if the overhead of managing clustered data outweighs the performance benefits, it may not be necessary.

Ultimately, it is important to consider the trade-offs between the potential performance benefits of clustering and the additional complexity and cost it may add before deciding whether or not to cluster data in Big Query.

**A: False**

See [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables).