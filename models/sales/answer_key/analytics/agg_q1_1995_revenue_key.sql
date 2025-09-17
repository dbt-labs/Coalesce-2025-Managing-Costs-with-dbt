-- Find the total revenue from high-priority orders in the first quarter of 1995.

select
    o_orderpriority,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    {{ ref('fct_customer_orders_wide_clustered') }}
where
    o_orderdate >= '1995-01-01'
    and o_orderdate < '1995-04-01'
    and o_orderpriority = '1-URGENT'
group by
    o_orderpriority