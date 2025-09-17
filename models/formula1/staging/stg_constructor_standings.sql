select 
    constructor_standings_id,
    race_id,
    constructor_id,
    points,
    position,
    position_text,
    wins
from {{ source('raw', 'constructor_standings') }}