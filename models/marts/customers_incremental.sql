{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}






with

customers as (

    select * from {{ ref('stg_tech_store__customers') }}

    
        {% if is_incremental() %}

  -- this filter will only be applied on an incremental run

       where created_at >= ( select max(created_at)  from dbt_dev.staging.stg_tech_store__customers)

    {% endif %}


),

customers_and_locations_joined as (

    select * from {{ ref('int_customers_and_locations_joined') }}

),

employees as (

    select * from {{ ref('stg_tech_store__employees') }}

),

order_amounts_by_customer as (

    select * from {{ ref('int_order_amounts_agg_by_customer') }}

),

final as (

    select
        customers.customer_id,
        customers.customer_name,
        customers_and_locations_joined.city_name,
        customers_and_locations_joined.state_name,
        customers_and_locations_joined.zip_code,
        employees.full_name as main_employee,
        employees.is_active as main_employee_is_active,
        nvl(order_amounts_by_customer.total_revenue_in_usd, 0) 
            as total_revenue_in_usd,
        nvl(order_amounts_by_customer.total_quantity, 0) as total_quantity,
         customers.created_at,
        {{ utc_to_est('customers.created_at') }}  as created_at_est , -- new
        customers.updated_at,
        {{ utc_to_est('customers.updated_at') }}  as updated_at_est , -- new
        customers.is_active

    from customers

    left join customers_and_locations_joined
        on customers.customer_id = customers_and_locations_joined.customer_id
    
    left join employees
        on customers.main_employee_id = employees.employee_id

    left join order_amounts_by_customer
        on customers.customer_id = order_amounts_by_customer.customer_id

)

select * from final