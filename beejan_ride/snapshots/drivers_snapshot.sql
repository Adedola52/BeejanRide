{% snapshot drivers_snapshot %}

{{
  config(
    unique_key='driver_id',
    target_schema='beejan_ride_beejan_ride',
    strategy='check',
    check_cols=['driver_status', 'drivers_ratings', 'vehicle_id']
  )
}}

select * from {{ ref ('mart_drivers') }}

{% endsnapshot %}
