{{ config(
    materialized='view'
) }}

-- Silver model: merge CRM + ERP customer data and enrich with cleaned location info

with crm as (
    select
        cast(cst_id as int) as cst_id,
        trim(cst_key) as cst_key,
        trim(cst_firstname) as first_name,
        trim(cst_lastname) as last_name,
        case 
            when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else 'Unknown'
        end as marital_status,
        case 
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'Unknown'
        end as cst_gndr
    from {{ source('bronze', 'cust_info') }}
),

erp as (
    select
        CASE
	        WHEN "CID" LIKE 'NAS%' THEN SUBSTRING("CID",4,LENGTH("CID")) 
	        ELSE "CID"
        END as cst_key,
        CASE 
            WHEN "BDATE" > NOW()::date THEN NULL 
            ELSE "BDATE"
        END as birthdate,
        case 
            when upper(trim("GEN")) IN ('F', 'FEMALE') then 'Female'
            when upper(trim("GEN")) IN ('M', 'MALE') then 'Male'
            else 'Unknown'
        end as gen
    from {{ source('bronze', 'cust_az12') }}
),

loc as (
    select
        replace(trim("CID"), '-', '') as cst_key,
        case 
            when trim("CNTRY") = 'DE' then 'Germany'
            when trim("CNTRY") in ('US', 'USA') then 'United States'
            when trim("CNTRY") = '' or "CNTRY" is null then 'Unknown'
            else trim("CNTRY")
        end as country
    from {{ source('bronze', 'loc_a101') }}
)

select distinct on (cst_id)
    crm.cst_id,
    crm.cst_key,
    crm.first_name,
    crm.last_name,
    crm.marital_status,
    case
        when crm.cst_gndr != 'Unknown' then crm.cst_gndr
        else coalesce(erp.gen, 'Unknown')
    end as gender,
    erp.birthdate,
    loc.country
from crm
left join erp on crm.cst_key = erp.cst_key
left join loc on crm.cst_key = loc.cst_key
where crm.cst_id is not null
order by cst_id

