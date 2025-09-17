{{
    config(
        materialized='incremental'
    )
}}

select
    -- primary key
    l.l_orderkey || '-' || l.l_linenumber as order_item_key,

    -- foreign keys
    l.l_orderkey as order_key,
    l.l_partkey as part_key,
    l.l_suppkey as supplier_key,
    o.o_custkey as customer_key,
    c.c_nationkey as nation_key,

    -- timestamps & status
    o.o_orderdate as order_date,
    o.o_orderstatus as order_status,
    l.l_shipdate as ship_date,
    l.l_commitdate as commit_date,
    l.l_receiptdate as receipt_date,
    l.l_returnflag as return_flag,

    -- numeric measures
    l.l_quantity as quantity,
    l.l_extendedprice as extended_price,
    l.l_discount as discount,
    l.l_tax as tax,
    (l.l_extendedprice * (1 - l.l_discount)) as net_item_sales_amount

from
    {{ ref('stg_lineitem') }} as l
join
    {{ ref('stg_orders') }} as o
    on l.l_orderkey = o.o_orderkey
join
    {{ ref('stg_customer') }} as c
    on o.o_custkey = c.c_custkey

-- this logic will only be applied on an incremental run 

{% if is_incremental() %}

    where order_date > (select max(order_date) from {{ this }} )

{% endif %}