{{ config(materialized='view') }}

WITH cte as (
    SELECT CAST(rider_id AS INT) AS rider_id, 
    CAST(country AS STRING) AS country, 
    CAST(signup_date AS DATE) AS rider_signup_date, 
    CAST(created_at AS TIMESTAMP) AS rider_created_date,
    CAST(referral_code as STRING) AS referral_code,
    ROW_NUMBER() OVER(PARTITION BY rider_id ORDER BY created_at DESC) AS rn
FROM {{ source ('beejan_ride_beejan_ride_', 'riders_raw')}}
WHERE rider_id IS NOT NULL)
SELECT rider_id, country, rider_signup_date, rider_created_date, referral_code
FROM cte
WHERE rn = 1