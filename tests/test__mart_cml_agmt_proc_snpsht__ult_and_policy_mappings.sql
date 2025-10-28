-- Ensures ULT_POL_NBR and ULT_POL_EFF_DT fallback correctly to policy values
select
  POL_NBR, POL_EFF_DT, ULT_POL_NBR, ULT_POL_EFF_DT
from {{ ref('mart_cml_agmt_proc_snpsht') }}
where (
      (ULT_POL_NBR is null or ULT_POL_NBR = '')
   or (
        ULT_POL_NBR = POL_NBR and ULT_POL_EFF_DT != POL_EFF_DT and ULT_POL_EFF_DT is not null
     )
)
