{{ config(materialized='view') }}

WITH cte as (
    SELECT CAST(driver_id AS INT) AS driver_id,	
        CAST(onboarding_date AS DATE) AS driver_onborded_date,	
        CAST(driver_status AS STRING) AS driver_status,
        CAST(city_id AS INT) AS city_id,	
        CAST(vehicle_id	AS STRING) AS vehicle_id,
        CAST(rating AS NUMERIC) AS drivers_ratings,
        CAST(created_at	AS TIMESTAMP) AS drivers_created_date,
        CAST(updated_at AS TIMESTAMP) AS drivers_updated_date, 
        ROW_NUMBER() OVER(PARTITION BY driver_id, vehicle_id, city_id ORDER BY created_at DESC) AS rn
    FROM {{ source ('beejan_ride_beejan_ride', 'drivers_raw') }}
    WHERE driver_id IS NOT NULL
    AND vehicle_id IS NOT NULL
    AND city_id IS NOT NULL)
SELECT driver_id, vehicle_id, city_id, driver_onborded_date, driver_status, drivers_created_date, drivers_updated_date, drivers_ratings
FROM cte 
WHERE rn = 1