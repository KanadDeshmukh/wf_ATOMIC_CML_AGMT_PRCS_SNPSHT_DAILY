-- Test: Check for duplicates in AGMT_ANCHR_ID
with base as (
    select AGMT_ANCHR_ID, count(*) as cnt
    from {{ ref('mart_cml_agmt_proc_snpsht') }}
    group by AGMT_ANCHR_ID
    having count(*) > 1
)
select * from base;
