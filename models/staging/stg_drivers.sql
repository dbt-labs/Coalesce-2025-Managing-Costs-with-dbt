select
    driver_id,
    driver_ref,
    number,
    code,
    forename,
    surname,
    dob,
    nationality,
    url
from {{ source('raw', 'drivers') }}