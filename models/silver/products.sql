{{ config(
    materialized='view'
) }}


with prd as (
    select
        cast(prd_id as int) as prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id, 
        substring(prd_key, 7, length(prd_key)) as prd_key,      
        trim(prd_nm) as prd_nm,
        coalesce(prd_cost, 0) as prd_cost,                    
        case upper(trim(prd_line))
            when 'R' then 'Road'
            when 'M' then 'Mountain'
            when 'S' then 'Other Sales'
            when 'T' then 'Touring'
            else 'Unknown'
        end as prd_line,                                         
        cast(prd_start_dt as date) as start_date,
        cast(
            lead(cast(prd_start_dt as date))
            over (partition by prd_key order by prd_start_dt) - interval '1 day' as date
            ) as end_date
    from {{ source('bronze', 'prd_info') }}
),

cat as (
    select
        trim("ID") as cat_id,
        trim("CAT") as category,
        trim("SUBCAT") as subcategory,
        trim("MAINTENANCE") as maintenance
    from {{ source('bronze', 'px_cat_g1v2') }}
)

select distinct on (prd_key)
    p.prd_id,
    p.cat_id,
    p.prd_key,
    p.prd_nm,
    p.prd_cost,
    p.prd_line,
    p.start_date::date as start_date,
    p.end_date::date as end_date,
    c.category,
    c.subcategory,
    c.maintenance
from prd p
left join cat c
    on p.cat_id = c.cat_id
order by prd_key, p.start_date



