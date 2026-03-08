{{ config(materialized='view') }}

SELECT 
{{ column_names() }}
    FROM {{ ref('stg_trips') }} t
    JOIN {{ ref('stg_payments') }} p
    ON p.trip_id = t.trip_id
    JOIN {{ ref('stg_drivers') }} d 
    ON t.driver_id = d.driver_id
    AND t.city_id = d.city_id
    AND t.vehicle_id = d.vehicle_id
    JOIN {{ ref('stg_cities') }} c 
    ON c.city_id = t.city_id
    JOIN {{ ref('stg_riders') }} r 
    ON r.rider_id = t.rider_id