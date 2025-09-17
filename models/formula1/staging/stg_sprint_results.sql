select
    result_id,
    race_id,
    driver_id,
    constructor_id,
    number,
    grid,
    position,
    position_text,
    position_order,
    points,
    laps,
    time,
    milliseconds,
    fastest_lap,
    fastest_lap_time,
    status_id
from {{ source("raw", "sprint_results") }}
