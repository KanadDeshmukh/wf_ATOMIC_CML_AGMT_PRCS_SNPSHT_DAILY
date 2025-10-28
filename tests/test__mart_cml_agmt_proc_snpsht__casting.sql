-- Test: Data type casting and defaulting in mart_cml_agmt_proc_snpsht
with base as (
    select * from {{ ref('mart_cml_agmt_proc_snpsht') }}
)
select * from base
where (
    typeof(MAX_DED_AMT) not in ('integer','float','decimal') or
    typeof(AGMT_ANCHR_ID) not in ('string','varchar','bigint','integer') or
    typeof(BNDL_CD) not in ('string','varchar')
)
union all
select * from base
where (MKT_PRD_SPEC_ID = -1 and PGM_MKT_PRDT_CD <> '0')
   or (MKT_PRD_SPEC_ID is null and PGM_MKT_PRDT_CD is not null and PGM_MKT_PRDT_CD <> '0')
;
