{{ config(materialized='ephemeral') }}

SELECT driver_id AS int_driver_driver_id, 
count(*) AS driver_lifetime_trips 
FROM {{ ref('int_trip') }}
GROUP BY 1