-- use ctes to break down complex logic and improve readability

with european_customers as (
    select
        c.c_custkey,
        c.c_mktsegment as market_segment
    from
        {{ ref('stg_customer') }} c
    join
        {{ ref('stg_nation') }} n on c.c_nationkey = n.n_nationkey
    join
        {{ ref('stg_region') }} r on n.n_regionkey = r.r_regionkey
    -- filter as early as possible
    where
        r.r_name = 'EUROPE'
    and c.c_mktsegment = 'BUILDING'
),

late_order_items as (
    select
        customer_key,
        order_date,
        net_item_sales_amount
    from
        {{ ref('int_order_items_incremental_clustered') }}
    where
        -- filter early and leveraging the clustering key
        receipt_date > commit_date
        and order_date >= '1995-01-01'
)

-- final select statement 
select
    extract(quarter from loi.order_date) as order_quarter,
    ec.market_segment,
    sum(loi.net_item_sales_amount) as total_revenue
from
    late_order_items loi
join
    european_customers ec on loi.customer_key = ec.c_custkey
group by
    1, 2
order by
    1, 2