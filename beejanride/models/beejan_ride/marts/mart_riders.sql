{{ config(materialized='incremental') }}

SELECT DISTINCT rider_id, country, rider_signup_date, rider_created_date, referral_code
FROM {{ ref('stg_riders') }}

