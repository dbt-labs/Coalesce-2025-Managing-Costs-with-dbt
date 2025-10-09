select
    n_nationkey,
    n_name,
    n_regionkey
from {{ source('dbt_jstayton', 'nation') }}