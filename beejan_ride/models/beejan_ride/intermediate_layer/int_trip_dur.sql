{{ config(materialized='ephemeral') }}

SELECT trip_id AS int_trip_tripid, payment_id AS int_dup_paymentid,
dropoff_date - pickup_date as trip_duration 
FROM {{ ref ('int_trip')}}