-- Question 3
SELECT COUNT(*)
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime::TIMESTAMP::DATE = '2021-01-15';

-- Question 4
SELECT tpep_pickup_datetime::TIMESTAMP::DATE
FROM yellow_taxi_trips
WHERE to_char(tpep_pickup_datetime, 'MM') = '01'
ORDER BY tip_amount DESC
LIMIT 1;

-- Question 5
SELECT
CASE WHEN dof."Zone" IS null
THEN 'Unknow'
ELSE dof."Zone" END AS "drop_off"
FROM yellow_taxi_trips
INNER JOIN zones pup ON  "PULocationID" = pup."LocationID"
INNER JOIN zones dof ON  "DOLocationID" = dof."LocationID"
WHERE pup."Zone" = 'Central Park'
GROUP BY "drop_off"
ORDER BY COUNT(*) DESC
LIMIT 1;

-- Question 6
SELECT
CASE WHEN dof."Zone" IS null
THEN CONCAT(pup."Zone", '/Unknown')
ELSE CONCAT(pup."Zone", '/', dof."Zone") END AS "zone_pair"
FROM yellow_taxi_trips
INNER JOIN zones pup ON  "PULocationID" = pup."LocationID"
INNER JOIN zones dof ON  "DOLocationID" = dof."LocationID"
GROUP BY "zone_pair", dof."LocationID", dof."Zone"
ORDER BY AVG(fare_amount) DESC
LIMIT 1;
