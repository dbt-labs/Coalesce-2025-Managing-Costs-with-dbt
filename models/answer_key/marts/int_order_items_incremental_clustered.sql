{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_item_key',
        cluster_by=['order_date']
    )
}}

with orders as (

    select
        o_orderkey as order_key,
        o_custkey as customer_key,
        o_orderdate as order_date,
        o_totalprice as total_price,
        o_orderstatus as order_status,
        o_orderpriority as order_priority,
        o_shippriority as ship_priority,
        o_clerk as clerk
    from
        {{ ref('stg_orders') }}

),

line_item as (

    select
        l_orderkey as order_key,
        l_partkey as part_key,
        l_suppkey as supplier_key,
        l_linenumber as line_number,
        l_linestatus as line_status,
        l_shipdate as ship_date,
        l_receiptdate as receipt_date,
        l_commitdate as commit_date,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount,
        l_tax as tax,
        l_returnflag as return_flag,
        (l_extendedprice * (1 - l_discount)) as net_item_sales_amount
    from
        {{ ref('stg_lineitem') }}

)

select
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['line_number', 'orders.order_key']) }} as order_item_key,

    -- Keys
    line_item.order_key,
    orders.customer_key,
    line_item.part_key,
    line_item.supplier_key,
    line_item.line_number,

    -- Order details
    orders.order_date,
    orders.order_status,
    orders.clerk,
    orders.order_priority,
    orders.ship_priority,
    line_item.ship_date,
    line_item.receipt_date,
    line_item.commit_date,
    line_item.return_flag,

    -- Line item measures
    line_item.quantity,
    line_item.line_status,
    orders.total_price,
    line_item.extended_price,
    line_item.discount,
    line_item.tax,
    line_item.net_item_sales_amount
from
    line_item
left join
    orders on line_item.order_key = orders.order_key

{% if is_incremental() %}

where order_date > (select max(order_date) from {{ this }} )

{% endif %}