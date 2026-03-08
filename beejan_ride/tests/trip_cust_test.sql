select *
from {{ ref ('mart_trips')}}
where trip_duration < INTERVAL 0 MINUTE
and (trip_status  = 'completed' and payment_status = 'success')