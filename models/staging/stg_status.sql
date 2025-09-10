select
    status_id,
    status
from {{ source('raw', 'status') }}