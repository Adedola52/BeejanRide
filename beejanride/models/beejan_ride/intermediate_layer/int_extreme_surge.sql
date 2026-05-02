{{ config(materialized='ephemeral') }}

SELECT trip_id AS sur_tripid, payment_id AS sur_payment_id,
CASE WHEN surge_multiplier > 10 
    THEN 'Extreme surge multiplier'
ELSE 'Fair surge multiplier' 
END AS Surge_multiplier_indicator
FROM {{ ref('int_trip') }}