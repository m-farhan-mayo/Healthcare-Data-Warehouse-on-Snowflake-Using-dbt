{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('treatments') }}
),

cleaned as (
    select
        {{ cast_string('treatment_id') }} as treatment_id,
        {{ cast_string('visit_id') }} as visit_id,
        {{ text_clean('treatment_code') }} as treatment_code,
        {{ text_clean('treatment_desc') }} as treatment_desc,
        {{ cast_numeric('treatment_cost') }} as treatment_cost
    from base
    where treatment_id is not null
      and visit_id is not null
      and treatment_code is not null
      and treatment_desc is not null
      and treatment_cost is not null
)

select * from cleaned