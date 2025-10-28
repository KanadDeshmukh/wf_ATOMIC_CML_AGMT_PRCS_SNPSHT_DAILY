-- Negative/edge test cases for mart_cml_agmt_proc_snpsht business logic validation

with negative_cases as (
    -- Simulate invalid or edge scenario rows joined from mock/ref tables. The seeds or exposures should set up these rows.
    select * from {{ ref('mart_cml_agmt_proc_snpsht') }} where 1=1
    and (
          -- MAX_DED_AMT negative (should be defaulted to 0 or rejected by test)
          MAX_DED_AMT < 0
          or -- Invalid CAPTV_IND logic
          CAPTV_IND not in ('Y','N')
          or -- Unexpected code for TYP_INS_IND
          TYP_INS_IND not in ('F','P')
          or -- Unexpected value for MAX_DED_CD
          MAX_DED_CD not in ('N','L','M','H')
          or -- Null ULT_POL_NBR (should not occur)
          ULT_POL_NBR is null
          or -- Invalid NEW_USED_PLN_CD
          NEW_USED_PLN_CD not in ('0','N','U')
          or -- UNCLCT_DED_POL_IND not 'Y' or 'N'
          UNCLCT_DED_POL_IND not in ('Y','N')
    )
)
select * from negative_cases
