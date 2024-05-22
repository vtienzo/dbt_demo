{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}




with

orders as (

    select * from dbt_dev.staging.stg_tech_store__orders

        {% if is_incremental() %}

  -- this filter will only be applied on an incremental run

       where created_at >= (select max(created_at) from {{this}} WHERE created_at BETWEEN DATEADD(day, -7, GETDATE()) AND GETDATE())

    {% endif %}

),

transactions as (

    select * from dbt_dev.staging.stg_payment_app__transactions

),

products as (

    select * from dbt_dev.staging.stg_tech_store__products

),

customers as (

    select * from dbt_dev.staging.stg_tech_store__customers

),


final as (

    select
        orders.order_id,
        transactions.transaction_id,
        customers.customer_id,
        customers.customer_name,
        products.product_name,
        products.category,
        products.price,
        products.currency,
        orders.quantity,        
        transactions.cost_per_unit_in_usd,
           ROUND((transactions.cost_per_unit_in_usd * 0.8),2 ) as cost_per_unit_in_gbp,
        transactions.amount_in_usd,
           ROUND((transactions.amount_in_usd * 0.8),2 ) as amount_in_gbp,
        transactions.tax_in_usd,
             ROUND((transactions.tax_in_usd * 0.8),2 ) as tax_in_gbp,
        transactions.total_charged_in_usd,
          ROUND((transactions.total_charged_in_usd * 0.8),2 )  as total_charged_in_gbp,
       orders.created_at,
        convert_timezone('UTC', 'America/New_York', orders.created_at)  as created_at_est 

    from orders

    left join transactions
        on orders.order_id = transactions.order_id

    left join products
        on orders.product_id = products.product_id

    left join customers
        on orders.customer_id = customers.customer_id

)

select * from final