--Counting zero fare trips.How many records have a fare_amount of 0?

select count(*) from de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_taxi_external
where fare_amount=0;

CREATE OR REPLACE TABLE de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned AS
SELECT * FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_taxi_external;

CREATE OR REPLACE TABLE de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_taxi_external;

SELECT DISTINCT(VendorID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31';

--Data read estimation : Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT DISTINCT(PULocationID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned;

SELECT DISTINCT(PULocationID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_taxi_external;

SELECT PULocationID,DOLocationID
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned;

SELECT PULocationID
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned;

SELECT DISTINCT(VendorID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31';

CREATE OR REPLACE TABLE de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_taxi_external;

-- Partition benefits : Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

/*Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.*/

SELECT DISTINCT(VendorID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(*) from de-zoomcamp-ramani-2026.nyc_taxi_bq.yellow_tripdata_non_partitioned;
