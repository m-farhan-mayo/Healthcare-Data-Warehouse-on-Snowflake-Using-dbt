{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_hospitals') }}
)

select
    hospital_id,
    hospital_name,
    city,
    capacity_beds
from base
