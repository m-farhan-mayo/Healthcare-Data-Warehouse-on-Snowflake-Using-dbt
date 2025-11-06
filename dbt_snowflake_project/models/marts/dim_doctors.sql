{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_doctors') }}
)

select
    doctor_id,
    full_name,
    specialization,
    department_id,
    hospital_id,
    phone_no
from base
