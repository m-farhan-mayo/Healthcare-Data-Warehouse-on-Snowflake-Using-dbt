{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_billing') }}
)

select
    bill_id,
    visit_id,
    total_amount,
    insurance_covered,
    patient_payable,
    payment_status,
    bill_date
from base
