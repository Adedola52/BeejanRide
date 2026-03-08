{{ config(materialized='view') }}

WITH cte as (
    SELECT CAST(event_id AS INT) AS event_id,
    CAST(driver_id AS INT) AS driver_id,
    CAST(status AS STRING) AS event_status, 
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp, 
    ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn
FROM {{ source ('beejan_ride_beejan_ride', 'driver_status_events_raw') }}
WHERE event_id IS NOT NULL)
SELECT event_id, driver_id, event_status, event_timestamp
FROM cte
WHERE rn = 1