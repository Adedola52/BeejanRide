{{ config(materialized='ephemeral') }}

SELECT trip_id AS cop_trip_id, payment_id AS cop_payment_id,
CASE WHEN is_corporate = true 
    THEN 'Coporate Trip'
ELSE 'Non-Coporate Trip'
END AS is_corporate_
FROM {{ ref('int_trip') }}