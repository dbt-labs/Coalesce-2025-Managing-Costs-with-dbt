-- this model is intentionally written with anti-patterns for the refactoring lab.
{{
    config(
        materialized='table'
    )
}}

select
    extract(quarter from order_date) as order_quarter,
    c.c_mktsegment,
    sum(net_item_sales_amount) as total_revenue
from
    {{ ref('fct_order_items') }} f
join
    {{ ref('stg_customer') }} c on f.customer_key = c.c_custkey
where
    -- anti-pattern: filtering late in the query after the join
    f.receipt_date > f.commit_date
    and c.c_mktsegment = 'BUILDING'
    and year(f.order_date) >= 1995 -- this filter is okay, but the year() function prevents us from being able to leverage the order_date clustering key from fct_order_items

    -- anti-pattern: using a complex subquery in the where clause instead of a cte
    and c.c_nationkey in (
        select n_nationkey
        from {{ ref('stg_nation') }} n
        join {{ ref('stg_region') }} r on n.n_regionkey = r.r_regionkey
        where r.r_name = 'EUROPE'
    )
group by
    1, 2
order by
    1, 2