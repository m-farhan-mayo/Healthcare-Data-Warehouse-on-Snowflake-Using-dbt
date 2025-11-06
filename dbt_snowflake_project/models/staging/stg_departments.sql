{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('departments') }}
),

-- Step 1: Clean mandatory fields
cleaned as (
    select
        {{ cast_string('department_id') }} as department_id,
        {{ text_clean('department_name') }} as department_name,
        {{ cast_string('hospital_id') }} as hospital_id
    from base
    where department_id is not null
      and department_name is not null
      and hospital_id is not null
),

-- Step 2: Validate foreign key to hospitals
validated as (
    select d.*
    from cleaned d
    join {{ ref('stg_hospitals') }} h
      on d.hospital_id = h.hospital_id
)

select *
from validated