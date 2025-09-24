select
    n.n_name as nation,
    extract(year from f.order_date) as order_year,
    sum(f.calculated_revenue) as total_revenue
from
    {{ ref('fct_revenue_by_customer_nation_clustered') }} f
join
    {{ ref('stg_nation') }} n on f.nation_key = n.n_nationkey
where
    n.n_name in ('FRANCE', 'GERMANY')
    and extract(year from f.order_date) between 1992 and 1996
group by
    1, 2
order by
    1