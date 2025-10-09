select
    r_regionkey,
    r_name
from
    {{ source('dbt_jstayton', 'region') }}