-- Find the total revenue from high-priority orders in the first quarter of 1995.
select
    order_priority,
    sum(extended_price * (1 - discount)) as revenue
from
    {{ ref('fct_customer_orders') }}
where
    order_date >= '1995-01-01'
    and order_date < '1995-04-01'
    and order_priority = '1-URGENT'
group by
    order_priority