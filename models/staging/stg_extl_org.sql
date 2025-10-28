with src as (
  select
    ROL_PLYR_ANCHR_ID,
    OPERT_CO_CD
  from {{ source('raw','EXTL_ORG') }}
  where ORG_TYP_CD = 'COMPANY'
)
select * from src
