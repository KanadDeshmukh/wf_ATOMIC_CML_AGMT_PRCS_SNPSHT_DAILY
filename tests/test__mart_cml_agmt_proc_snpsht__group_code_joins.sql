-- Ensures group code attributes (ACTUARIAL, MARKET_BASKET) are mapped only where rol_cat/staging lookups exist, or are null otherwise.
select
  AGMT_ANCHR_ID, ORG_GP_CD, MKT_BSKT_GRP_CD
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where (
      (ORG_GP_CD is not null and ORG_GP_CD = '')
   or (MKT_BSKT_GRP_CD is not null and MKT_BSKT_GRP_CD = '')
)
