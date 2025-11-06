{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_visits') }}
)

select
    visit_id,
    patient_id,
    doctor_id,
    hospital_id,
    visit_date,
    visit_type,
    diagnosis_code,
    height_cm,
    weight_kg
from base
