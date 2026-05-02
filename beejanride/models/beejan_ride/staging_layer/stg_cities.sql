{{ config(materialized='view') }}

WITH cte as (
    SELECT 
        cast(city_id AS int) AS city_id, 
        cast(country AS STRING) AS country, 
        cast(city_name AS STRING) AS city_name, 
        cast(launch_date AS date) AS city_launch_date, 
        ROW_NUMBER() OVER(PARTITION BY city_id ORDER BY launch_date DESC) AS rn
    FROM {{ source ('beejan_ride_beejan_ride_', 'cities_raw') }}
    WHERE city_id IS NOT NULL)
SELECT city_id, country, city_name, city_launch_date
FROM cte 
WHERE rn = 1

