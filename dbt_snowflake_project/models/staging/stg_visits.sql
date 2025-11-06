{{
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    )
}}

with base as (
    select * from {{ ref('visits') }}
),

cleaned as (
    select
        {{ cast_string('visit_id') }} as visit_id,
        {{ cast_string('patient_id') }} as patient_id,
        {{ cast_string('doctor_id') }} as doctor_id,
        {{ cast_string('hospital_id') }} as hospital_id,
        {{ convert_date('visit_date') }} as visit_date,
        {{ text_clean('diagnosis_code') }} as diagnosis_code,
        {{ text_clean('visit_type') }} as visit_type,
        {{ cast_numeric('height_cm') }} as height_cm,
        {{ cast_numeric('weight_kg') }} as weight_kg,
        {{ scd2_rn('visit_id', 'visit_date') }}
    from base
    where visit_id is not null
      and patient_id is not null
      and doctor_id is not null
      and hospital_id is not null
      and visit_date is not null
      and diagnosis_code is not null
      and visit_type is not null
),

-- Validate FKs
validated as (
    select v.*
    from cleaned v
    join {{ ref('stg_patients') }} p on v.patient_id = p.patient_id
    join {{ ref('stg_doctors') }} d on v.doctor_id = d.doctor_id
    join {{ ref('stg_hospitals') }} h on v.hospital_id = h.hospital_id
)

select *
from validated
where rn = 1