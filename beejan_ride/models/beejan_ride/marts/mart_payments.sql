{{ config(materialized='incremental') }}

SELECT DISTINCT payment_id, trip_id, payment_status, payment_provider, amount, service_charge, currency, payment_created_date
FROM {{ ref('stg_payments')}}