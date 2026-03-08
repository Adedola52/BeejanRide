{{ config(materialized='incremental') }}

SELECT DISTINCT city_id, country, city_name, city_launch_date
FROM {{ ref('stg_cities') }}