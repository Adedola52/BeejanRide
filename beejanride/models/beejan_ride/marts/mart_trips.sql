{{ config(materialized='incremental') }}

WITH int_table AS ( 
    SELECT * 
    FROM {{ ref('int_trip') }}
),

trip_dur_min AS (
    SELECT int_trip_tripid, int_dup_paymentid, trip_duration  
    FROM {{ ref('int_trip_dur') }}
),

driver_lifetime AS ( 
    SELECT int_driver_driver_id, driver_lifetime_trips  
    FROM {{ ref('int_driver_ltv') }}
),

rider_lifetimevalue AS (
    SELECT int_rid_rider_id, int_rid_payment_id, rider_lifetime_value 
    FROM {{ ref('int_rider_ltv') }}
),

extreme_surge AS ( 
    SELECT sur_tripid, sur_payment_id, Surge_multiplier_indicator 
    FROM {{ ref('int_extreme_surge') }}
),

payment_check AS ( 
    SELECT int_paymenttrip_id, int_paymentpay_id, payment_and_trip_checks 
    FROM {{ ref('int_paymentcheck') }}
),

Coporate_check AS (
    SELECT cop_trip_id, cop_payment_id, is_corporate_ 
    FROM {{ ref('int_corporate_trip') }}
),

dup_checks AS (
    SELECT int_dup_tripid, int_dup_paymentid, duplicate_payment_flag 
    FROM {{ ref('int_dup_trips') }}
)
SELECT 
    a.*,
    b.trip_duration,
    c.driver_lifetime_trips,
    d.rider_lifetime_value,
    e.Surge_multiplier_indicator,
    f.payment_and_trip_checks,
    g.is_corporate_,
    h.duplicate_payment_flag
FROM int_table a
JOIN driver_lifetime c
    ON c.int_driver_driver_id = a.driver_id
JOIN trip_dur_min b
    ON a.trip_id = b.int_trip_tripid
    AND a.payment_id = b.int_dup_paymentid
JOIN extreme_surge e
    ON e.sur_tripid = a.trip_id
    AND e.sur_payment_id = a.payment_id
JOIN payment_check f
    ON f.int_paymenttrip_id = a.trip_id
    AND a.payment_id = f.int_paymentpay_id
JOIN Coporate_check g
    ON g.cop_trip_id = a.trip_id
    AND g.cop_payment_id = a.payment_id
JOIN dup_checks h
    ON a.trip_id = h.int_dup_tripid
    AND a.payment_id = h.int_dup_paymentid
JOIN rider_lifetimevalue d
    ON d.int_rid_rider_id = a.rider_id
    AND d.int_rid_payment_id = a.payment_id