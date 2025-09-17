select 
    circuit_id, 
    circuit_ref, 
    name, 
    location, 
    country, 
    lat, 
    lng, 
    alt, 
    url
from {{ source("raw", "circuits") }}