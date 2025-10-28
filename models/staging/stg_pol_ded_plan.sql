with src as (
  select
    POL_ANCHR_ID,
    DED_PLN_TYP_CD,
    MAX_DED_AMT,
    EFF_FM_TISTMP,
    EFF_TO_TISTMP,
    row_number() over (partition by POL_ANCHR_ID, DED_PLN_TYP_CD order by coalesce(EFF_TO_TISTMP,'9999-12-31') desc) as rn
  from {{ source('raw','POL_DED_PLAN') }}
)
select * from src where rn = 1
