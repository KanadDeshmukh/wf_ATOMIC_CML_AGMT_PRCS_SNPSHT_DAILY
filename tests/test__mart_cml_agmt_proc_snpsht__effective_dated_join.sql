-- For effective-dated ded logic:
-- Each AGMT_ANCHR_ID should only have the most recent MAX_DED_AMT (per effective date logic)
with base as (
    select AGMT_ANCHR_ID, MAX_DED_AMT, row_number() over (partition by AGMT_ANCHR_ID order by MAX_DED_AMT desc, ULT_POL_EFF_DT desc) as rn
    from {{ ref('mart_cml_agmt_proc_snpsht') }}
)
select * from base where rn > 1
