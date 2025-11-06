{{ 
    config(
        materialized='table',
        database='HEALTHCARE_DB',
        schema='STAGING'
    ) 
}}

with base as (
    select * from {{ ref('billing') }}
),

cleaned as (
    select
        {{ cast_string('bill_id') }} as bill_id,
        {{ cast_string('visit_id') }} as visit_id,
        {{ cast_numeric('total_amount') }} as total_amount,
        {{ cast_numeric('insurance_covered') }} as insurance_covered,
        {{ cast_numeric('patient_payable') }} as patient_payable,
        upper(trim(payment_status)) as payment_status,
        {{ convert_date('bill_date') }} as bill_date,
        {{ scd2_rn('bill_id', 'bill_date') }}
    from base
      where bill_id is not null
      and visit_id is not null
      and total_amount is not null
      and insurance_covered is not null
      and patient_payable is not null
      and payment_status is not null
      and bill_date is not null
),

validated as (
    select b.*
    from cleaned b
    join {{ ref('stg_visits') }} v on b.visit_id = v.visit_id
)

select *
from validated