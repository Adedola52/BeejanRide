{% macro column_names() %}

t.trip_id, r.rider_id, d.driver_id, d.vehicle_id, c.city_id, c.country, c.city_name, c.city_launch_date, 
  t.dropoff_date, t.pickup_date, t.trip_request_date, 
  t.trip_status, p.payment_id, p.payment_status, p.payment_provider, p.amount, p.service_charge, p.currency, p.payment_created_date,
  t.estimated_fare, t.actual_fare, t.surge_multiplier, t.payment_method, t.is_corporate, t.trip_created_date, 
  t.trip_updated_date, r.rider_signup_date, r.rider_created_date, r.referral_code,
  d.driver_onborded_date, d.driver_status, d.drivers_created_date, d.drivers_updated_date,
  d.drivers_ratings

  {% endmacro %}