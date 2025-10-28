-- Edge-case scenarios for mart_cml_agmt_proc_snpsht
-- 1. Policies without group lookups
select AGMT_ANCHR_ID, ORG_GP_CD, MKT_BSKT_GRP_CD
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where (ORG_GP_CD is null or MKT_BSKT_GRP_CD is null)
union all
-- 2. Zero or negative MAX_DED_AMT
select AGMT_ANCHR_ID, MAX_DED_AMT, MAX_DED_CD
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where MAX_DED_AMT <= 0 and MAX_DED_CD <> 'N'
union all
-- 3. Null ultimate policy fields
select AGMT_ANCHR_ID, ULT_POL_NBR, ULT_POL_EFF_DT, POL_NBR, POL_EFF_DT
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where ULT_POL_NBR is null and POL_NBR is not null
   or ULT_POL_EFF_DT is null and POL_EFF_DT is not null
union all
-- 4. Orphaned role player keys
select AGMT_ANCHR_ID, ROL_PLYR_ANCHR_ID_INT_ORG, ROL_PLYR_ANCHR_ID_EXTL_ORG
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where (ROL_PLYR_ANCHR_ID_INT_ORG is null or ROL_PLYR_ANCHR_ID_EXTL_ORG is null)
union all
-- 5. Unmapped or null FINC_SERVS_ROL values
select AGMT_ANCHR_ID, CUR_PRDR_ID
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where CUR_PRDR_ID is null or CUR_PRDR_ID = '';
