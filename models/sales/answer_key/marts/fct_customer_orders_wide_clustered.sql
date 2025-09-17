{{
    config(
        materialized='table',
        cluster_by=['o_orderdate']
    )
}}

select
    -- from lineitem
    l.l_orderkey,
    l.l_partkey,
    l.l_suppkey,
    l.l_linenumber,
    l.l_quantity,
    l.l_extendedprice,
    l.l_discount,
    l.l_tax,
    l.l_returnflag,
    l.l_linestatus,
    l.l_shipdate,
    l.l_commitdate,
    l.l_receiptdate,

    -- from orders
    o.o_custkey as customerkey,
    o.o_orderstatus,
    o.o_totalprice,
    o.o_orderdate,
    o.o_orderpriority,
    o.o_clerk,
    o.o_shippriority,

    -- from customer
    c.c_name as c_customer_name,
    c.c_nationkey,
    c.c_acctbal

from
    {{ ref('stg_lineitem') }} as l
left join
    {{ ref('stg_orders') }} as o
    on l.l_orderkey = o.o_orderkey
left join
    {{ ref('stg_customer') }} as c
    on o.o_custkey = c.c_custkey