select 
    constructor_results_id, 
    race_id, 
    constructor_id, 
    points, 
    status
from {{ source("raw", "constructor_results") }}
