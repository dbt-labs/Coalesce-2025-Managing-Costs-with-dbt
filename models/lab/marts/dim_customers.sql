{{
    config(
        materialized='table',
        tags=['test'],
        pre_hook="{{ get_models_by_tag('test', 'dbt_fundamentals') }}"
    )
}}

with customers as (

    select
        c_custkey as customer_key,
        c_name as customer_name,
        c_address as customer_address,
        c_nationkey as nation_key,
        c_phone as phone_number,
        c_acctbal as account_balance,
        c_mktsegment as market_segment
    from
        {{ ref('stg_customer') }}

),

nations as (

    select
        n_nationkey as nation_key,
        n_name as nation,
        n_regionkey as region_key
    from
        {{ ref('stg_nation') }}

),

regions as (

    select
        r_regionkey as region_key,
        r_name as region
    from
        {{ ref('stg_region') }}

),

final as (

    select
        customers.customer_key,
        customers.customer_name,
        customers.customer_address,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment,
        nations.nation,
        nations.nation_key,
        regions.region,
        regions.region_key
    from
        customers
    left join
        nations on customers.nation_key = nations.nation_key
    left join
        regions on nations.region_key = regions.region_key

)

select * from final