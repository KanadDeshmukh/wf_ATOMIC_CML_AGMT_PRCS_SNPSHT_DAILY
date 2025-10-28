with src as (
  select
    ANCHR_ID,
    NEW_USED_IND,
    MK_MD_ID
  from {{ source('raw','VEH_MK_MDL_NW_USD_IND') }}
)
select * from src
