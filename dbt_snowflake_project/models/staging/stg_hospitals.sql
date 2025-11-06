{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

-- Step 1: Read raw hospitals table
with base as (
    select * from {{ ref('hospitals') }}
),

-- Step 2: Clean and standardize data
cleaned as (
    select
        {{ cast_string('hospital_id') }} as hospital_id,  -- Ensure ID is string
        {{ text_clean('hospital_name') }} as hospital_name,
        {{ text_clean('city') }} as city,
        {{ cast_numeric('capacity_beds') }} as capacity_beds,
        {{ convert_date('record_start') }} as record_start,
        {{ convert_date('record_end') }} as record_end,
        {{ scd2_rn('hospital_id', 'record_start') }}       -- Add SCD2 row number
    from base
    where hospital_id is not null
      and hospital_name is not null
      and city is not null
      and capacity_beds is not null
      and record_start is not null
)

-- Step 3: Keep only latest SCD2 row
select *
from cleaned
where rn = 1