with v1 as (
    select * from {{ ref('stg_customer') }}
),

v2 as (
    select * from {{ ref('stg_lineitem') }}
),

v3 as (
    select * from {{ ref('stg_nation') }}
),

v4 as (
    select * from {{ ref('stg_orders') }}
),

v5 as (
    select * from {{ ref('stg_region') }}
)

select 
    v1.c_custkey, -- join with orders
    v1.c_name,
    v1.c_address,
    v1.c_nationkey,
    v1.c_phone,
    v1.c_acctbal,
    v1.c_mktsegment,
    v1.c_comment,
    v2.l_commitdate,
    v2.l_discount,
    v2.l_extendedprice,
    v2.l_linenumber,
    v2.l_linestatus,
    v2.l_orderkey,
    v2.l_partkey,
    v2.l_quantity,
    v2.l_receiptdate,
    v2.l_returnflag,
    v2.l_shipdate,
    v2.l_shipinstruct,
    v2.l_shipmode,
    v2.l_suppkey,
    v2.l_tax,
    v3.n_nationkey,-- join with customers
    v3.n_name,
    v3.n_regionkey, -- join with region
    v4.o_orderkey, -- join with line items
    v4.o_custkey, -- join with customers
    v4.o_orderstatus,
    v4.o_totalprice,
    v4.o_orderdate,
    v4.o_orderpriority,
    v4.o_clerk,
    v4.o_shippriority,
    v5.r_regionkey,
    v5.r_name
from v1
join v4 on v4.o_custkey = v1.c_custkey
join v2 on v2.l_orderkey = v4.o_orderkey
join v3 on v3.n_nationkey = v1.c_nationkey
join v5 on v5.r_regionkey = v3.n_regionkey