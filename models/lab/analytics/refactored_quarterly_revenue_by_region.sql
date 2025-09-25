{{
    config(
        enabled=false
    )
}}

-- This model file is for our final lab. You will refactor the quarterly_revenue_by_region model.
-- Copy the model SQL from the quarterly_revenue_by_region model and refactor here to fix the anti-patterns and support modularity
-- Remove the above config to run the model


-- CTE to identify European customers
-- filter early!
with european_customers as (



),

-- CTE to identify late orders
-- filter early!
-- re-write the filter to remove the functions wrapped around the order_date column
late_order_items as (



)


-- final select
select 



from european_customers ec
join late_order_items loi