{{
    config(
        materialized='table'
    )
}}

select
    -- lineitem measures
    l.l_extendedprice,
    l.l_discount,
    (l.l_extendedprice * (1 - l.l_discount)) as calculated_revenue,

    -- keys for joining and filtering
    l.l_orderkey,
    o.o_orderdate,
    c.c_custkey,
    c.c_nationkey

from
    {{ ref('stg_lineitem') }} as l
left join
    {{ ref('stg_orders') }} as o
    on l.l_orderkey = o.o_orderkey
left join
    {{ ref('stg_customer') }} as c
    on o.o_custkey = c.c_custkey