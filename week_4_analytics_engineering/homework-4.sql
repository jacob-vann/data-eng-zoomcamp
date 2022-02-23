-- Data Engineering Zoomcamp - Homework #4


---------------------------------------------------------------------------
-- Question 1: What is the count of records in the model fact_trips after running all models
-- with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)
-- Answer 1: 61635151

SELECT COUNT(*) FROM `rational-aria-338717.dbt_jvann.fact_trips`;

-- Question 2: What is the distribution between service type filtering by years 2019 and 2020 data
-- as done in the videos . (Yellow/Green)
-- Answer 2: 89.9/10.2 => 89.9/10.1
SELECT ROUND(COUNT(*)/
(SELECT COUNT(*) FROM `rational-aria-338717.dbt_jvann.fact_trips`)*100, 1)
FROM `rational-aria-338717.dbt_jvann.fact_trips`
GROUP BY service_type;

-- Question 3: What is the count of records in the model stg_fhv_tripdata after running all models with
-- the test run variable disabled
-- Answer 3: 42084899

SELECT COUNT(*) FROM `rational-aria-338717.dbt_jvann.stg_fhv_tripdata`;

-- Question 4: What is the count of records in the model fact_fhv_trips after running all dependencies
-- with the test run variable disabled
-- Answer 4: 22676253
SELECT COUNT(*) FROM `rational-aria-338717.dbt_jvann.fact_fhv_trips`;

-- Question 5: What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table
-- Answer 5: January 
