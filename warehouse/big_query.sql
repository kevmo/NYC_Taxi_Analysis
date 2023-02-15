SELECT COUNT(*) 
FROM `taxi-rides-ny-375722.FHV_2019.fhv_`;

SELECT COUNT(DISTINCT Affiliated_base_number) as abn_ct 
FROM `taxi-rides-ny-375722.FHV_2019.fhv_`;

SELECT COUNT(DISTINCT Affiliated_base_number) as abn_ct 
FROM `taxi-rides-ny-375722.fhv_native_dataset.fhv_native`;




-----

--  partition by pickup_datetime, cluster by Affiliated_base_number:

CREATE OR REPLACE TABLE `taxi-rides-ny-375722.fhv_native_dataset.fhv_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY `Affiliated_base_number`
AS
SELECT * FROM `taxi-rides-ny-375722.fhv_native_dataset.fhv_native`;

-----

-- BQ table 

SELECT COUNT(DISTINCT Affiliated_base_number) as abn_ct 
FROM `taxi-rides-ny-375722.fhv_native_dataset.fhv_native`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- partitioned 

SELECT COUNT(DISTINCT Affiliated_base_number) as abn_ct 
FROM `taxi-rides-ny-375722.fhv_native_dataset.fhv_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
