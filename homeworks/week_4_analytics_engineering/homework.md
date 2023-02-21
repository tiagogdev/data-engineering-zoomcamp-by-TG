## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

## Prerequisites: 

### Upload FHV 2019 Taxi Rides NY dataset into BigQuery

Create a python script to load fhv data from Github repo to Google Cloud Storage (GCS).

File `web_to_gcs.py`

``` python
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression="gzip")
    return df


@task(log_prints=True)
def clean(color: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    fhv 2019
    dispatching_base_num,pickup_datetime,dropOff_datetime,PUlocationID,DOlocationID,SR_Flag,Affiliated_base_number
    """
    if color == "fhv":
        """Rename columns"""
        df.rename(
            {"dropoff_datetime": "dropOff_datetime"}, axis="columns", inplace=True
        )
        df.rename({"PULocationID": "PUlocationID"}, axis="columns", inplace=True)
        df.rename({"DOLocationID": "DOlocationID"}, axis="columns", inplace=True)

        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

        # See https://pandas.pydata.org/docs/user_guide/integer_na.html
        df["PUlocationID"] = df["PUlocationID"].astype("Int64")
        df["DOlocationID"] = df["DOlocationID"].astype("Int64")

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(color: str, df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=9000)
    return


@flow()
def web_to_gcs() -> None:

    color = "fhv"
    year = 2019

    for month in range(1, 13):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        df_clean = clean(color, df)
        path = write_local(color, df_clean, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    web_to_gcs()
```

Buckets on Google Cloud

![buckets](/images/hw4/buckets_gcp.png)

Then, in BigQuery, I created the 3 tables.

![buckets](/images/hw4/create_table_gcp.png)

## Question 1

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442
- 51648442
- 61648442
- 71648442

## Solution 1

Run dbt via the command:

``` bash
dbt run --var 'is_test_run: false'
```

After this we'll have the fact_trips table in Google BigQuery and doing a count gives us the following number: **62 404 047**

![q1_i01](/images/hw4/q1_i01.png)

There is no number equal to this result. In this case we should select the closest, which is:  **61 648 442**

**A: 61648442**

## Question 2 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

## Solution 2

Report creation in Google Looker Studio, formerly Google Data Studio.

![q2_i01](/images/hw4/q2_i01.png)


**A: 89.9/10.1 (The closest)**


## Question 3 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696
- 53244696
- 63244696

## Solution 3

Create the staging in dbt.

File `models/staging/stg_fhv_tripdata.sql`

``` jinja
{{ config(materialized='view') }}
 

select
   -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as float64) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from {{ source('staging','fhv_tripdata') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```

Run dbt via the command:

``` bash
dbt run --var 'is_test_run: false'
```

After this we'll have the stg_fhv_tripdata table in Google BigQuery and doing a count gives us the following number: **43 244 696**

![q3_i01](/images/hw4/q3_i01.png)

**A: 43244696**

## Question 4

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722
- 32998722
- 42998722

## Solution 4

Create the fact in dbt.

File `models/core/fact_fhv_trips.sql`

``` jinja
{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.tripid, 
    'FHV' as service_type,
    fhv_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number
from {{ ref('stg_fhv_tripdata') }} as fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
```

Run dbt via the command:

``` bash
dbt run --var 'is_test_run: false'
```

After this we'll have the fact_fhv_trips table in Google BigQuery and doing a count gives us the following number: **22 998 722**

![q4_i01](/images/hw4/q4_i01.png)

**A: 22998722**

## Question 5

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December

## Solution 5

Report creation in Google Looker Studio, formerly Google Data Studio.

![q5_i01](/images/hw4/q5_i01.png)


**A: January**
