{{ config(
    tags=['test'],
    pre_hook="{{ get_warehouse_by_tags() }}"
)}}

select
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment
from {{ source('dbt_jstayton', 'customer') }}