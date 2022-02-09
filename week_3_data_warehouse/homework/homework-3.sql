-- Data Engineering Zoomcamp - Homework #3


---------------------------------------------------------------------------
-- Question 1: What is count for fhv vehicles data for year 2019
-- Answer 1: 42084899

CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_for_hire_vehicle_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_rational-aria-338717/raw/fhv_tripdata_2019-*.parquet']
);

SELECT COUNT(*)
FROM `trips_data_all.external_for_hire_vehicle_data`;


---------------------------------------------------------------------------
-- Question 2: How many distinct dispatching_base_num we have in fhv for 2019
-- Answer 2: 792

SELECT COUNT(DISTINCT(dispatching_base_num))
FROM `trips_data_all.external_for_hire_vehicle_data`;


---------------------------------------------------------------------------
-- Question 3: Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
-- Answer 3: Partition by dropoff_datetime and cluster by dispatching_base_num

-- Paritioned by dropoff_datetime
CREATE OR REPLACE TABLE `trips_data_all.external_for_hire_vehicle_data_part`
PARTITION BY DATE(dropoff_datetime) AS
SELECT * FROM `trips_data_all.external_for_hire_vehicle_data`;

-- Ordered by dispatching_base_num and filtered by dropoff_datetime
-- 0.8 secs elapsed
SELECT * FROM `trips_data_all.external_for_hire_vehicle_data_part`
WHERE DATE(dropoff_datetime) = '2019-03-10'
ORDER BY dispatching_base_num;

-- Paritioned by dropoff_datetime and clustered by dispatching_base_num
CREATE OR REPLACE TABLE `trips_data_all.external_for_hire_vehicle_data_part_clust`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dropoff_datetime AS
SELECT * FROM `trips_data_all.external_for_hire_vehicle_data`;

-- Ordered by dispatching_base_num and filtered by dropoff_datetime
-- less than 0.0 secs elapsed
SELECT * FROM `trips_data_all.external_for_hire_vehicle_data_part_clust`
WHERE DATE(dropoff_datetime) = '2019-03-10'
ORDER BY dispatching_base_num;


---------------------------------------------------------------------------
-- Question 4 What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
-- Answer 4: Count: 26558, Estimated data processed: 155 MB, Actual data processed: 400 MB

SELECT COUNT(*) FROM `trips_data_all.external_for_hire_vehicle_data`
WHERE (DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-30') AND (dispatching_base_num IN ('B00987', 'B02060', 'B02279'));


---------------------------------------------------------------------------
-- Question 5: What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
-- Answer 5: Cluster by dispatching_base_num and SR_Flag


---------------------------------------------------------------------------
-- Question 6: What improvements can be seen by partitioning and clustering for data size less than 1 GB *
-- Answer 6: No improvments


---------------------------------------------------------------------------
-- Question 7: In which format does BigQuery save data
-- Answer 7: Columnar
