{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_departments') }}
)

select
    department_id,
    department_name,
    hospital_id
from base
