{{
    config(
        materialized='table'
    )
}}

-- use ctes to break down complex logic and improve readability

with european_customers as (
    select
        c.c_custkey,
        c.c_mktsegment
    from
        {{ ref('stg_customer') }} c
    join
        {{ ref('stg_nation') }} n on c.c_nationkey = n.n_nationkey
    join
        {{ ref('stg_region') }} r on n.n_regionkey = r.r_regionkey
    where
        -- filter as early as possible on region and segment
        r.r_name = 'EUROPE'
        and c.c_mktsegment = 'BUILDING'
),

late_order_items as (
    select
        customer_key,
        order_date,
        net_item_sales_amount
    from
        {{ ref('fct_order_items') }}
    where
        -- filter early on the "late" status and leveraging the clustering key
        receipt_date > commit_date
        and order_date >= '1995-01-01'
)

-- final select statement joins the two small, pre-filtered ctes
select
    extract(quarter from loi.order_date) as order_quarter,
    ec.c_mktsegment,
    sum(loi.net_item_sales_amount) as total_revenue
from
    late_order_items loi
join
    european_customers ec on loi.customer_key = ec.c_custkey
group by
    1, 2
order by
    1, 2