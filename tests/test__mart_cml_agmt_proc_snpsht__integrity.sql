-- Data integrity and referential checks for mart_cml_agmt_proc_snpsht
with base as (select * from {{ ref('mart_cml_agmt_proc_snpsht') }}),
stg_agmt as (select AGMT_ANCHR_ID from {{ ref('stg_cml_agmt') }}),
stg_finc as (select AGMT_ANCHR_ID from {{ ref('stg_finc_servs_rol') }}),
stg_role as (select ROL_PLYR_ANCHR_ID from {{ ref('stg_rol_plyr_cat_rel') }}),
-- 1. Every AGMT_ANCHR_ID in mart must exist in stg_cml_agmt
not_in_stg as (
    select AGMT_ANCHR_ID from base
    where AGMT_ANCHR_ID not in (select AGMT_ANCHR_ID from stg_agmt)
),
-- 2. Foreign keys to role player must exist in role player category relation
orphaned_roles as (
    select AGMT_ANCHR_ID, ROL_PLYR_ANCHR_ID_INT_ORG
    from base
    where ROL_PLYR_ANCHR_ID_INT_ORG not in (select ROL_PLYR_ANCHR_ID from stg_role)
       or ROL_PLYR_ANCHR_ID_EXTL_ORG not in (select ROL_PLYR_ANCHR_ID from stg_role)
),
-- 3. All agreements from stg_cml_agmt should be in mart (no lost rows)
missing_fact as (
    select AGMT_ANCHR_ID from stg_agmt
    where AGMT_ANCHR_ID not in (select AGMT_ANCHR_ID from base)
)
select 'not_in_stg_cml_agmt' as test_type, * from not_in_stg
union all
select 'orphaned_role', * from orphaned_roles
union all
select 'missing_fact_row', AGMT_ANCHR_ID from missing_fact;
