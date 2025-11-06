{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_patients') }}
)

select
    patient_id,
    first_name,
    last_name,
    gender,
    dob,
    phone_no,
    email,
    address,
    city,
    insurance_plan
from base
