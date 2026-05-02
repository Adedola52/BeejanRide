{{ config(materialized='incremental') }}

SELECT DISTINCT event_id, driver_id, event_status, event_timestamp
FROM {{ ref('stg_driver_status_events') }}