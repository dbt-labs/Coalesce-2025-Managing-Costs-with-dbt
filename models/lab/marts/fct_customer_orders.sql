--Added cluster_by config to reduce cost when running the dashboard_query
-- Focus the clustering key on columns that are dates, or used in joins and filters so there is a ok number of partitions
-- and will be useful, not too many or too few.
{{
    config(
        materialized='table'
        , cluster_by = ['order_date']
    )
}}

with order_items as (

    select * from {{ ref('int_order_items') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

)

select
    -- columns from the intermediate model
    order_items.order_key,
    order_items.part_key,
    order_items.supplier_key,
    order_items.line_number,
    order_items.quantity,
    order_items.extended_price,
    order_items.discount,
    order_items.tax,
    order_items.return_flag,
    order_items.line_status,
    order_items.ship_date,
    order_items.receipt_date,
    order_items.order_status,
    order_items.total_price,
    order_items.order_date,
    order_items.order_priority,
    order_items.clerk,
    order_items.ship_priority,

    -- columns from the dimension model
    customers.customer_key,
    customers.customer_name,
    customers.nation,
    customers.account_balance

from
    order_items
left join
    customers on order_items.customer_key = customers.customer_key
