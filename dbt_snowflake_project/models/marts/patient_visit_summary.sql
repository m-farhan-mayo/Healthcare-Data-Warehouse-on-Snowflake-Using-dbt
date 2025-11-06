{{ config(materialized='view') }}

select
    v.visit_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.city,
    d.full_name as doctor_name,
    h.hospital_name,
    v.visit_type,
    v.diagnosis_code,
    b.total_amount as billed_amount,
    b.patient_payable,
    b.payment_status
from {{ ref('fact_visits') }} v
left join {{ ref('dim_patients') }} p on v.patient_id = p.patient_id
left join {{ ref('dim_doctors') }} d on v.doctor_id = d.doctor_id
left join {{ ref('dim_hospitals') }} h on v.hospital_id = h.hospital_id
left join {{ ref('fact_billing') }} b on v.visit_id = b.visit_id
