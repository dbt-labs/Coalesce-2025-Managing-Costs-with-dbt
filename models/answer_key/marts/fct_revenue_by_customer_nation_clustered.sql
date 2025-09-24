{{
    config(
        materialized='incremental',
        cluster_by=['nation_key']
    )
}}

with order_items as (

    select
        order_key,
        customer_key,
        order_date,
        extended_price,
        discount,
        net_item_sales_amount as calculated_revenue
    from
        {{ ref('int_order_items') }}

),

customers as (

    select
        customer_key,
        nation,
        nation_key
    from
        {{ ref('dim_customers') }}

)

select
    -- lineitem measures
    order_items.extended_price,
    order_items.discount,
    order_items.calculated_revenue,

    -- keys for joining and filtering
    order_items.order_key,
    order_items.order_date,
    customers.customer_key,
    customers.nation_key,
    customers.nation

from
    order_items
left join
    customers on order_items.customer_key = customers.customer_key

{% if is_incremental() %}

where order_date > (select max(order_date) from {{ this }})

{% endif %}