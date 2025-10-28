with src as (
  select
    LOW_LVL_ORG_ID,
    SBU_SSU_ID,
    ORG_ANCHR_ID
  from {{ source('raw','INT_ORG_RLUP') }}
)
select * from src
