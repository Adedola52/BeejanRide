{{ config(materialized='incremental') }}

SELECT DISTINCT driver_id, vehicle_id, city_id, driver_onborded_date, driver_status, drivers_created_date, 
drivers_updated_date, drivers_ratings
FROM {{ ref('stg_drivers')}}