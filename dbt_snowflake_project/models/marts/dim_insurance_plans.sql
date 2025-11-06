{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_insurance_plans') }}
)

select
    insurance_plan_id,
    plan_name,
    coverage_percent
from base
