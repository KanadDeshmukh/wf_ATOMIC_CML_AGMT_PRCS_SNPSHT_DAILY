with src as (
  select
    INMY_AGMT_ANCHR_ID,
    EXTL_REFR_CD_NTV_SRC_CD
  from {{ source('raw','INMY_AGMT') }}
)
select * from src
