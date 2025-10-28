with src as (
  select
    ROL_PLYR_ANCHR_ID,
    NTR_ID,
    EXTL_REFR_CD
  from {{ source('raw','ROL_PLYR_CAT_REL') }}
  where NTR_ID in ('MARKET_BASKET_GROUP','ACTUARIAL_GROUP')
)
select * from src
