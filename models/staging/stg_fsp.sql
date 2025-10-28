with src as (
  select 
    EXTL_REFR_CD as EXTL_REFR_CD_FSP,
    NM as NM_FSP,
    MKT_PRD_SPEC_ID
  from {{ source('raw','FSP') }}
)
select * from src
