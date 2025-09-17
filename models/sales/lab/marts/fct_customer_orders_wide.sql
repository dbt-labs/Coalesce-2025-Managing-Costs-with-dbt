{{
    config(
        materialized='table'
    )
}}

SELECT
    -- From Lineitem
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

    -- From Orders
    o.o_custkey as customerkey,
    o.o_orderstatus,
    o.o_totalprice,
    o.o_orderdate,
    o.o_orderpriority,
    o.o_clerk,
    o.o_shippriority,

    -- From Customer
    c.c_name AS c_customer_name,
    c.c_nationkey,
    c.c_acctbal

FROM
    {{ ref('stg_lineitem') }} AS l
LEFT JOIN
    {{ ref('stg_orders') }} AS o
    ON l.l_orderkey = o.o_orderkey
LEFT JOIN
    {{ ref('stg_customer') }} AS c
    ON o.o_custkey = c.c_custkey