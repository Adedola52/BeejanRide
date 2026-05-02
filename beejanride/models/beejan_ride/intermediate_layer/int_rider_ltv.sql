{{ config(materialized='ephemeral') }}

SELECT rider_id AS int_rid_rider_id, payment_id AS int_rid_payment_id,
sum(actual_fare * surge_multiplier) AS rider_lifetime_value 
FROM {{ ref('int_trip') }}
GROUP BY 1, 2