with src as (
  select
    ROL_PLYR_ANCHR_ID,
    ROL_TYP_CD,
    AGMT_ANCHR_ID,
    EFF_FM_TISTMP,
    EFF_TO_TISTMP,
    EXTL_ORG_ANCHR_ID,
    INT_ORG_ANCHR_ID
  from {{ source('raw','FINC_SERVS_ROL') }}
  where ROL_TYP_CD in ('INSURER','POLICY_ADMINISTRATOR')
)
select * from src
