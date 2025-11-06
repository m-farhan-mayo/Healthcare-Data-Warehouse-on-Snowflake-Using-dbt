{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('patients') }}
),

cleaned as (
    select
        {{ cast_string('patient_id') }} as patient_id,
        {{ text_clean('first_name') }} as first_name,
        {{ text_clean('last_name') }} as last_name,
        upper(trim(gender)) as gender,
        {{ convert_date('dob') }} as dob,
        {{ phone_clean('phone_no') }} as phone_no,
        lower(trim(email)) as email,
        {{ text_clean('address') }} as address,
        {{ text_clean('city') }} as city,
        {{ cast_string('insurance_plan') }} as insurance_plan,
        {{ convert_date('record_start') }} as record_start,
        {{ convert_date('record_end') }} as record_end,
        {{ scd2_rn('patient_id', 'record_start') }}
    from base
    where patient_id is not null
      and first_name is not null
      and last_name is not null
      and gender is not null
      and dob is not null
      and phone_no is not null
      and email is not null
      and address is not null
      and city is not null
),

-- Validate FK to insurance_plans
validated as (
    select p.*
    from cleaned p
    left join {{ ref('stg_insurance_plans') }} i
      on p.insurance_plan = i.insurance_plan_id
    where i.insurance_plan_id is not null  -- keep only patients with valid plan
)

select *
from validated
where rn = 1