select
    race_id,
    driver_id,
    stop,
    lap,
    time,
    duration,
    milliseconds
from {{ source('raw', 'pit_stops') }}