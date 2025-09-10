select 
    driver_standings_id,
    race_id,
    driver_id,
    points,
    position,
    position_text,
    wins
from {{ source('raw', 'driver_standings') }}