{{ config(materialized='ephemeral') }}

WITH cte AS (
    SELECT city_id, 
        trip_id AS int_dup_tripid, 
        rider_id, driver_id, vehicle_id, payment_id,
        count(*) AS payment_count 
FROM {{ ref('int_trip') }}
GROUP BY 1,2,3,4,5,6)
SELECT int_dup_tripid, payment_id as int_dup_paymentid,
CASE WHEN payment_count > 1 
    THEN 'Duplicate Payment'
ELSE 'Non-duplicate Payment'
END AS duplicate_payment_flag
from cte
