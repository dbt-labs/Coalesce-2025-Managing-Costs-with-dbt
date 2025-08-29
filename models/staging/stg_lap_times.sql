select
    race_id,
    driver_id,
    lap,
    position,
    time,
    milliseconds
from {{ source('raw','lap_times') }}