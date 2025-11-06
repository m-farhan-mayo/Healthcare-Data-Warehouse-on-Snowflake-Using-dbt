{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('insurance_plans') }}
),

cleaned as (
    select
        {{ cast_string('insurance_plan_id') }} as insurance_plan_id,
        {{ text_clean('provider_name') }} as provider_name,
        {{ text_clean('plan_name') }} as plan_name,
        {{ cast_numeric('coverage_percent') }} as coverage_percent,
        {{ convert_date('record_start') }} as record_start,
        {{ convert_date('record_end') }} as record_end,
        {{ scd2_rn('insurance_plan_id', 'record_start') }}
    from base
    where insurance_plan_id is not null
      and provider_name is not null
      and plan_name is not null
      and coverage_percent is not null
      and record_start is not null
)

select *
from cleaned
where rn = 1