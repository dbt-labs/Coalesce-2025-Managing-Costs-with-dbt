select
    r_regionkey,
    r_name
from
    {{ source('tpch_sf100', 'region') }}