{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_treatments') }}
)

select
    treatment_id,
    visit_id,
    treatment_code,
    treatment_desc,
    treatment_cost
from base
