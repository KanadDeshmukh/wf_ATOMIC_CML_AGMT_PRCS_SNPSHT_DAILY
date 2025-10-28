-- Test: All mart business logic is correctly implemented
with base as (
    select * from {{ ref('mart_cml_agmt_proc_snpsht') }}
),
-- Check MAX_DED_CD bucket
max_ded_bucket as (
    select AGMT_ANCHR_ID, MAX_DED_AMT, MAX_DED_CD,
        case
            when MAX_DED_AMT is null or MAX_DED_AMT<=0 then 'N'
            when MAX_DED_AMT<25000 then 'L'
            when MAX_DED_AMT>=25000 and MAX_DED_AMT<50000 then 'M'
            when MAX_DED_AMT>=50000 then 'H'
            else 'N'
        end as expected_bucket
    from base
)
select * from max_ded_bucket
where coalesce(MAX_DED_CD,'') <> expected_bucket
union all
-- COALESCE fallback for ULT_POL_NBR/EFF_DT
select AGMT_ANCHR_ID, POL_NBR as input_val, ULT_POL_NBR as result_val
from base
where (ULT_POL_NBR is distinct from POL_NBR) and (ULT_POL_NBR is null)
union all
select AGMT_ANCHR_ID, POL_EFF_DT as input_val, ULT_POL_EFF_DT as result_val
from base
where (ULT_POL_EFF_DT is distinct from POL_EFF_DT) and (ULT_POL_EFF_DT is null)
union all
-- CAPTV_IND reflects proper control table logic (using RAW_UNDG_PGM_CD)
select AGMT_ANCHR_ID, RAW_UNDG_PGM_CD, CAPTV_IND
from (
    select a.AGMT_ANCHR_ID, a.CAPTV_IND, cml.RAW_UNDG_PGM_CD, ref.v_CAPTV_IND as expected
    from {{ ref('mart_cml_agmt_proc_snpsht') }} a
    left join {{ ref('stg_cml_agmt') }} cml on cml.AGMT_ANCHR_ID = a.AGMT_ANCHR_ID
    left join {{ ref('stg_additional_reference') }} ref on 1=1
) where (CAPTV_IND = 'Y' and RAW_UNDG_PGM_CD <> expected)
   or (CAPTV_IND = 'N' and RAW_UNDG_PGM_CD = expected)
union all
-- TYP_INS_IND assignment (should be 'F' if POL_SYM_CD matches control, else 'P')
select AGMT_ANCHR_ID, POL_SYM_CD, TYP_INS_IND
from (
    select a.AGMT_ANCHR_ID, a.POL_SYM_CD, a.TYP_INS_IND, ref.v_CD_VAL
    from {{ ref('mart_cml_agmt_proc_snpsht') }} a
    left join {{ ref('stg_additional_reference') }} ref on 1=1
)
where (TYP_INS_IND = 'F' and POL_SYM_CD <> v_CD_VAL)
   or (TYP_INS_IND = 'P' and POL_SYM_CD = v_CD_VAL)
;
