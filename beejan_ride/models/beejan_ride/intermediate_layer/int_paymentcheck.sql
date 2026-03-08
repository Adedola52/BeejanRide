{{ config(materialized='ephemeral') }}

SELECT trip_id AS int_paymenttrip_id, payment_id AS int_paymentpay_id,
CASE WHEN payment_status = 'failed' AND trip_status = 'completed' 
     THEN 'Failed payment on successful trip'
WHEN payment_status ='success' AND trip_status = 'completed' 
     THEN 'Successful payment on successfull trip'
END AS payment_and_trip_checks
FROM {{ ref('int_trip') }}