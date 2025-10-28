with src as (
  select
    LOW_LVL_ORG_ID,
    BU_ID
  from {{ source('raw','INT_ORG_RLUP_MM_SNPSHT_BU_ID') }}
)
select * from src
