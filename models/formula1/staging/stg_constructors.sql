select 
    constructor_id,
    constructor_ref,
    name,
    nationality,
    url
from {{ source('raw', 'constructors') }}