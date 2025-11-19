{{ config(
    materialized='table'
) }}

-- Gold: unified transactional fact table combining sales, customers, and products

with sales as (
    select
        ord_num,
        prd_key,
        cst_id,
        order_date,
        ship_date,
        due_date,
        sales_amount,
        quantity,
        price
    from {{ source('silver', 'sales_clean') }}
),

recency_calc as (
    select
        cst_id,
        EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
        max(order_date) as last_order_date
    from {{ source('silver', 'sales_clean') }}
    group by cst_id
),

max_date as (
    select 
        max(order_date) as curr_age 
    from {{ source('silver', 'sales_clean') }}
),

customers as (
    select
        cst_id,
        CONCAT(first_name, ' ', last_name) as cst_name,
        gender,
        marital_status,
        country,
        (md.curr_age - birthdate)::int / 365 as age
    from {{ source('silver', 'customers') }}
    cross join max_date md
),

products as (
    select
        prd_key,
        prd_nm,
        category,
        subcategory,
        maintenance,
        prd_cost,
        prd_line,
        start_date,
        end_date,
        case 
            when end_date is null then 'Active' 
            else 'Discontinued' 
        end as prd_status
    from {{ source('silver', 'products') }}
)

select
    s.ord_num as order_number,
    s.order_date,
    s.ship_date,
    s.due_date,
    s.sales_amount,
    s.quantity,
    s.price,
    s.cst_id as customer_id,
    c.cst_name as customer_name,
    c.age,
    CASE 
		WHEN c.age < 30 THEN '29 and Below'
	    WHEN c.age BETWEEN 30 AND 39 THEN '30-39'
	    WHEN c.age BETWEEN 40 AND 49 THEN '40-49'
	    ELSE '50 and Above'
	END as age_bracket,
    c.gender,
    c.marital_status,
    c.country,
    s.prd_key as product_key,
    p.prd_nm as product_name,
    p.category,
    p.subcategory,
    p.prd_line as product_line,
    p.prd_status,
    p.prd_cost as product_cost,
    (s.price-p.prd_cost) * s.quantity as profit,
    round(((cast(s.price as numeric) - cast(p.prd_cost as numeric))/cast(s.price as numeric)) * 100,2) as profit_margin,
    count(*) over (partition by s.cst_id) as total_orders,
    sum(s.sales_amount) over (partition by s.cst_id) as total_spend,
    round(avg(s.sales_amount) over (partition by s.cst_id),2) as avg_order_revenue,
    r.lifespan,
    r.last_order_date
from sales s
left join customers c on s.cst_id = c.cst_id
left join recency_calc r on s.cst_id = r.cst_id
left join products  p on s.prd_key = p.prd_key
where c.age is not null
and s.order_date is not null


