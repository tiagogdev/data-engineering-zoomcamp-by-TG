## Homework

### Question 1: 
**What is count for fhv vehicles data for year 2019**  

![alt text](https://i.imgur.com/3m5mei5.png)

```sql
CREATE OR REPLACE EXTERNAL TABLE `wise-ally-376418.zoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://tg-prefect-de-zoomcamp/data/nyc-tl-data/trip data/fhv_tripdata_2019-*.csv']
);
```

```sql
SELECT COUNT(*) FROM `wise-ally-376418.zoomcamp.fhv_tripdata`; 
```
![alt text](https://i.imgur.com/pf5nNxD.png)

**A: 43,244,696**


### Question 2: 
**Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?**  

```sql
CREATE OR REPLACE TABLE `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned` AS
SELECT * FROM wise-ally-376418.zoomcamp.fhv_tripdata;
```

```sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `wise-ally-376418.zoomcamp.fhv_tripdata`; 
```
![alt text](https://i.imgur.com/01hElWV.png)

```sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `wise-ally-376418.zoomcamp.fhv_tripdata`; 
```
![alt text](https://i.imgur.com/W0L4Ztq.png)

**A: 0 MB for the External Table and 317.94MB for the BQ Table**


### Question 3: 
**How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?**  

```sql
SELECT COUNT(dispatching_base_num) FROM `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

![alt text](https://i.imgur.com/pGQTMY3.png)

**A: 717,748**


### Question 4: 
**What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?**  

Partitioning by pickup_datetime and clustering on affiliated_base_number would likely be the best strategy for optimizing the table in this case. Partitioning the table on pickup_datetime would allow for efficient filtering based on pickup datetime, and clustering the table on affiliated_base_number would further improve query performance when sorting by this column

**A: Partition by pickup_datetime Cluster on affiliated_base_number**


### Question 5: 
**Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive). Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.**  

```sql
CREATE OR REPLACE TABLE `wise-ally-376418.zoomcamp.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `wise-ally-376418.zoomcamp.fhv_tripdata`;
```
```sql
SELECT DISTINCT(affiliated_base_number) 
FROM `wise-ally-376418.zoomcamp.fhv_tripdata_non_partitoned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';;
```
![alt text](https://i.imgur.com/Gh1audK.png)

```sql
SELECT DISTINCT(affiliated_base_number) 
FROM `wise-ally-376418.zoomcamp.fhv_tripdata_partitoned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

![alt text](https://i.imgur.com/FnLGK6F.png)

**A: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table**

### Question 6: 
**Where is the data stored in the External Table you created?**  
External tables are similar to standard Big Query tables, in that these tables store their metadata and schema in Big Query storage. However, their data resides in an external source. External tables are contained inside a dataset, and you manage them in the same way that you manage a standard Big Query table. External tables in Big Query are used to access data stored in external storage systems without having to load the data into Big Query.

**A: GCP Bucket**

### Question 7: 
**It is best practice in Big Query to always cluster your data:**  
It is not always a best practice in Big Query to cluster your data. Clustering can improve query performance for certain types of queries that filter or group by the clustered column(s), but it may also add additional complexity and cost to your data storage and management.

Whether or not to cluster data in Big Query depends on the specific use case and the types of queries that will be run against the data. If the queries that will be run against the data often filter or group by a specific set of columns, then clustering the data on those columns may improve performance. However, if the queries do not benefit from clustering or if the overhead of managing clustered data outweighs the performance benefits, it may not be necessary.

Ultimately, it is important to consider the trade-offs between the potential performance benefits of clustering and the additional complexity and cost it may add before deciding whether or not to cluster data in Big Query.

**A: False**

### (Not required) Question 8: 
**A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table.** 

**Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur.**  