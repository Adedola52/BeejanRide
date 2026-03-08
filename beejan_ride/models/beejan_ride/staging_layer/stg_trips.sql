{{ config(materialized='view') }}

WITH cte as (
    SELECT CAST(trip_id AS INT) AS trip_id,
        CAST(rider_id AS INT) AS rider_id,	
        CAST(driver_id AS INT) AS driver_id,	
        CAST(vehicle_id AS STRING) AS vehicle_id,	
        CAST(city_id AS INT) AS city_id,
        CAST(requested_at AS TIMESTAMP) AS trip_request_date,	
        CAST(pickup_at AS TIMESTAMP) AS pickup_date,	
        CAST(dropoff_at AS TIMESTAMP) AS dropoff_date,	
        CAST(status AS STRING) AS trip_status,
        CAST(estimated_fare AS NUMERIC) AS estimated_fare,	
        CAST(actual_fare AS NUMERIC) AS actual_fare,
        CAST(surge_multiplier AS NUMERIC) AS surge_multiplier,	
        CAST(payment_method AS STRING) AS payment_method,
        CAST(is_corporate AS BOOLEAN) AS is_corporate,	
        CAST(created_at AS TIMESTAMP) AS trip_created_date,
        CAST(updated_at AS TIMESTAMP) AS trip_updated_date, 
        ROW_NUMBER() OVER (PARTITION BY city_id, trip_id, rider_id, driver_id ORDER BY created_at DESC) AS rn
    FROM {{ source ('beejan_ride_beejan_ride', 'trips_raw') }}
    WHERE city_id IS NOT NULL
    AND trip_id IS NOT NULL
    AND rider_id IS NOT NULL
    AND driver_id IS NOT NULL)
SELECT trip_id, rider_id, driver_id, vehicle_id, city_id, trip_request_date, pickup_date, dropoff_date,	
trip_status, estimated_fare, actual_fare, surge_multiplier, payment_method, is_corporate, trip_created_date, trip_updated_date
FROM cte 
WHERE rn = 1
