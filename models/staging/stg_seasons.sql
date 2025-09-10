select 
    year,
    url
from {{ source('raw', 'seasons') }}