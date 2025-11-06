{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('doctors') }}
),

cleaned as (
    select
        {{ cast_string('doctor_id') }} as doctor_id,
        {{ text_clean('full_name') }} as full_name,
        {{ text_clean('specialization') }} as specialization,
        {{ cast_string('department_id') }} as department_id,
        {{ cast_string('hospital_id') }} as hospital_id,
        {{ phone_clean('phone_no') }} as phone_no,
        {{ convert_date('record_start') }} as record_start,
        {{ convert_date('record_end') }} as record_end,
        {{ scd2_rn('doctor_id', 'record_start') }}
    from base
    where doctor_id is not null
      and full_name is not null
      and specialization is not null
      and department_id is not null
      and hospital_id is not null
      and phone_no is not null
      and record_start is not null
),

-- Validate department and hospital FKs
validated as (
    select d.*
    from cleaned d
    join {{ ref('stg_departments') }} dep
      on d.department_id = dep.department_id
    join {{ ref('stg_hospitals') }} h
      on d.hospital_id = h.hospital_id
)

select *
from validated
where rn = 1