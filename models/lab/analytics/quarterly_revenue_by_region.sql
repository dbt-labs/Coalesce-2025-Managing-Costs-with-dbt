-- this model is intentionally written with anti-patterns for the refactoring lab

select

    extract(quarter from order_date) as order_quarter,
    c.c_mktsegment as market_segment,
    sum(net_item_sales_amount) as total_revenue

from
    {{ ref('int_order_items') }} i
join
    {{ ref('stg_customer') }} c on i.customer_key = c.c_custkey

-- anti-pattern: filter early, before joins where possible
where
    c.c_mktsegment = 'BUILDING'
    -- anti-pattern: the function in our filtering will prevent us from being able to leverage a clustering key
    and substr(to_varchar(i.order_date), 1, 4) >= '1995' 
    and i.receipt_date > i.commit_date

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