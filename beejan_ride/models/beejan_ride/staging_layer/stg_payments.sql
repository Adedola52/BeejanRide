{{ config(materialized='view') }}

WITH cte as (
    SELECT CAST(payment_id AS INT) AS payment_id,
        CAST(trip_id AS INT) AS trip_id,
        CAST(payment_status AS STRING) AS payment_status,
        CAST(payment_provider AS STRING) AS payment_provider,
        CAST(amount AS NUMERIC) AS amount,
        CAST(fee AS NUMERIC) AS service_charge,
        CAST(currency AS STRING) AS currency,
        CAST(created_at AS TIMESTAMP) AS payment_created_date,
        ROW_NUMBER() OVER(PARTITION BY payment_id, trip_id ORDER BY created_at DESC) AS rn
    FROM {{ source ('beejan_ride_beejan_ride', 'payments_raw') }}
    WHERE payment_id IS NOT NULL
    AND trip_id IS NOT NULL)
SELECT payment_id, trip_id, payment_status, payment_provider, amount, service_charge, currency, payment_created_date
FROM cte 
WHERE rn = 1
