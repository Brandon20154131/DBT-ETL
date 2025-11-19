{{ config(
    materialized='view'
) }}

-- Silver layer: clean and validate sales data

with cleaned as (
    select
        trim(sls_ord_num) as ord_num,
        trim(sls_prd_key) as prd_key,
        cast(sls_cust_id as int) as cst_id,
        case
            when length(sls_order_dt::text) != 8 or sls_order_dt = 0 then null
            else cast(cast(sls_order_dt as varchar) as date)
        end as order_date,
        case
            when length(sls_ship_dt::text) != 8 or sls_ship_dt = 0 then null
            else cast(cast(sls_ship_dt as varchar) as date)
        end as ship_date,
        case
            when length(sls_due_dt::text) != 8 or sls_due_dt = 0 then null
            else cast(cast(sls_due_dt as varchar) as date)
        end as due_date,
        case
            when sls_sales is null 
              or sls_sales <= 0
              or sls_sales != sls_quantity * abs(sls_price)
                then sls_quantity * abs(sls_price)
            else sls_sales
        end as sales_amount,
        coalesce(cast(sls_quantity as int), 0) as quantity,
        case
            when sls_price is null or sls_price <= 0
                then (sls_sales / nullif(sls_quantity, 0))
            else sls_price
        end as price
    from {{ source('bronze', 'sales_details') }}
    where sls_cust_id is not null
    and sls_prd_key is not null
)

select * from cleaned
