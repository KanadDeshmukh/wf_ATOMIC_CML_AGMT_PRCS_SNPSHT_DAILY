with 
  cml as (select * from {{ ref('stg_cml_agmt') }}),
  insurer as (select * from {{ ref('stg_finc_servs_rol') }} where ROL_TYP_CD = 'INSURER'),
  pol_admin as (select * from {{ ref('stg_finc_servs_rol') }} where ROL_TYP_CD = 'POLICY_ADMINISTRATOR'),
  inmy as (select * from {{ ref('stg_inmy_agmt') }}),
  pol_ded as (select * from {{ ref('stg_pol_ded_plan') }}),
  rol_cat as (select * from {{ ref('stg_rol_plyr_cat_rel') }}),
  extl_org as (select * from {{ ref('stg_extl_org') }}),
  int_org as (select * from {{ ref('stg_int_org_rlup') }}),
  int_org_bu as (select * from {{ ref('stg_int_org_rlup_mm_snpsht_bu_id') }}),
  veh as (select * from {{ ref('stg_veh_mk_mdl_nw_usd_ind') }}),
  fsp as (select * from {{ ref('stg_fsp') }}),
  ref_tables as (select * from {{ ref('stg_additional_reference') }})
select
  -- Direct/pass-through fields
  cml.AGMT_ANCHR_ID,
  cml.POL_NBR,
  cml.POL_EFF_DT,
  cml.TYP_ID,
  cml.INMY_AGMT_ANCHR_ID,
  cml.ACQN_CD,
  cml.ADT_FREQ_CD,
  iif(cml.BNDL_CD in ('U','B'),'B',cml.BNDL_CD) as BNDL_CD,
  cml.CAN_DT,
  cml.CAN_RSN_CD,
  -- CAPTV_IND logic
  iif(cml.RAW_UNDG_PGM_CD = ref_tables.v_CAPTV_IND, 'Y', 'N') as CAPTV_IND,
  cml.CIID_MATCH_IND,
  cml.CMS_PTY_ID,
  cml.CPAN_IND,
  cml.CUST_NBR,
  cml.DAC_CD,
  cml.DIVD_PAR_CD,
  cml.EXC_CD,
  cml.INSTL_CD,
  cml.MODU_NBR,
  cml.MP_CD,
  cml.PLND_END_DT,
  cml.POL_CONTR_TYP_CD,
  cml.POL_SYM_CD,
  cml.PORT_IND,
  cml.PRI_NMD_INSD_NM,
  cml.PRCG_OFC_CD,
  cml.PRDR_SRC_CD,
  cml.RATG_ORI_CD,
  cml.REINS_WAQS_TRTY_CD,
  cml.RENL_IND,
  cml.RETRO_RATG_CD,
  cml.RSK_TYP_CD,
  cml.SELF_INSD_RETN_IND,
  cml.SIC_CD,
  cml.SRC_POL_NBR,
  cml.SRC_POL_EFF_DT,
  cml.SRC_POL_SYM_CD,
  cml.STS_CD,
  cml.UNDG_PGM_CD,
  cml.V_INV_POOL_IND,
  cml.BIL_TYP_CD,
  -- LOW_LVL_ORG_ID logic via INT_ORG_RLUP (lookup via AGMT_ANCHR_ID, joins omitted)
  int_org.LOW_LVL_ORG_ID,
  -- ROL_PLYR_ANCHR_ID_INT_ORG resolves via pol_admin join
  pol_admin.ROL_PLYR_ANCHR_ID as ROL_PLYR_ANCHR_ID_INT_ORG,
  extl_org.OPERT_CO_CD,
  insurer.ROL_PLYR_ANCHR_ID as ROL_PLYR_ANCHR_ID_EXTL_ORG,
  -- CUR_PRDR_ID via enrichment from inmy_agmt or reference logic
  coalesce(inmy.EXTL_REFR_CD_NTV_SRC_CD,'') as CUR_PRDR_ID,
  -- ORG_GP_CD via group lookup/join, e.g.:
  (select r.EXTL_REFR_CD from rol_cat r where r.NTR_ID = 'ACTUARIAL_GROUP' and r.ROL_PLYR_ANCHR_ID = pol_admin.ROL_PLYR_ANCHR_ID limit 1) as ORG_GP_CD,
  -- INTRM_FINC_LOW_LVL_ORG_ID
  int_org.LOW_LVL_ORG_ID as INTRM_FINC_LOW_LVL_ORG_ID,
  -- UNCLCT_DED_POL_IND
  case when cml.POL_NBR in (select WRK_POL_NBR from ref_tables) then 'Y' else 'N' end as UNCLCT_DED_POL_IND,
  int_org.SBU_SSU_ID,
  -- MAX_DED_AMT (effective-dated join on POL_ANCHR_ID)
  coalesce(pol_ded.MAX_DED_AMT,0) as MAX_DED_AMT,
  -- MKT_BSKT_GRP_CD
  (select r.EXTL_REFR_CD from rol_cat r where r.NTR_ID = 'MARKET_BASKET_GROUP' and r.ROL_PLYR_ANCHR_ID = insurer.ROL_PLYR_ANCHR_ID limit 1) as MKT_BSKT_GRP_CD,
  -- MAX_DED_CD bucketing logic
  case 
    when pol_ded.MAX_DED_AMT is null or pol_ded.MAX_DED_AMT<=0 then 'N'
    when pol_ded.MAX_DED_AMT<25000 then 'L'
    when pol_ded.MAX_DED_AMT>=25000 and pol_ded.MAX_DED_AMT<50000 then 'M'
    when pol_ded.MAX_DED_AMT>=50000 then 'H'
    else 'N' end as MAX_DED_CD,
  cml.AGMT_TERM_IN_MNTH,
  cml.CONTR_TYPE_CD,
  cml.PERF_PROT_IND,
  cml.SALE_DT,
  cml.MKT_CD,
  -- TYP_INS_IND: e.g., F or P based on POL_SYM_CD against code
  case when cml.POL_SYM_CD = ref_tables.v_CD_VAL then 'F' else 'P' end as TYP_INS_IND,
  -- LOW_LVL_SALE_ORG_ID: complex field, possibly joined from other staged reference
  coalesce(int_org_bu.BU_ID,'') as LOW_LVL_SALE_ORG_ID,
  -- OCCR_LMT_AMT: assume direct join or calculation, placeholder
  cml.OCCR_LMT_AMT,
  cml.MKT_PRD_SPEC_ID,
  iif(cml.MKT_PRD_SPEC_ID = -1, '0', fsp.EXTL_REFR_CD_FSP) as PGM_MKT_PRDT_CD,
  iif(cml.MKT_PRD_SPEC_ID = -1, '0', fsp.NM_FSP) as PGM_MKT_PRDT_DESC,
  cml.PYMT_PLN_CD,
  cml.MSTR_POL_NBR,
  cml.TAIL_EXPI_DT,
  cml.SRC_CERT_NBR,
  -- ULT_POL_NBR logic: prefer ultimate if not null, otherwise policy number
  coalesce(ref_tables.ULT_POL_NBR, cml.POL_NBR) as ULT_POL_NBR,
  -- ULT_POL_EFF_DT logic: prefer ultimate eff dt if not null
  coalesce(ref_tables.ULT_POL_EFF_DT, cml.POL_EFF_DT) as ULT_POL_EFF_DT,
  cml.PARNT_CHILD_CONTR_TYP_CD,
  cml.PRI_NMD_INSD_FEIN_NBR,
  cml.ASSM_PTNR_CTRY_POL_ID,
  cml.ASSM_US_POL_NBR,
  cml.CEDE_PTNR_CTRY_ANCHR_ID,
  cml.DRVD_CONTR_CRCY_CD,
  cml.FRNT_CD,
  cml.NON_RENL_RSN_CD,
  cml.ZI_POL_SYS_CONTR_ID,
  cml.CROP_INS_TYP_NBR,
  cml.CROP_REINS_YY,
  cml.DRVD_SNDBL_TO_GVMNT_IND,
  cml.POL_PRCG_HLD_RSN_CD,
  cml.POL_PRCG_STS_CD,
  cml.SRC_CO_CD,
  cml.SRC_SYS_CD,
  -- NEW_USED_PLN_CD field via veh model
  coalesce(veh.NEW_USED_IND,'0') as NEW_USED_PLN_CD,
  coalesce(veh.MK_MD_ID,'0') as VEH_MK_MDL_ID
from cml
left join insurer on insurer.AGMT_ANCHR_ID = cml.AGMT_ANCHR_ID
left join pol_admin on pol_admin.AGMT_ANCHR_ID = cml.AGMT_ANCHR_ID
left join inmy on inmy.INMY_AGMT_ANCHR_ID = cml.INMY_AGMT_ANCHR_ID
left join pol_ded on pol_ded.POL_ANCHR_ID = cml.AGMT_ANCHR_ID
left join rol_cat rc1 on rc1.ROL_PLYR_ANCHR_ID = pol_admin.ROL_PLYR_ANCHR_ID and rc1.NTR_ID = 'ACTUARIAL_GROUP'
left join rol_cat rc2 on rc2.ROL_PLYR_ANCHR_ID = insurer.ROL_PLYR_ANCHR_ID and rc2.NTR_ID = 'MARKET_BASKET_GROUP'
left join extl_org on extl_org.ROL_PLYR_ANCHR_ID = insurer.ROL_PLYR_ANCHR_ID
left join int_org on int_org.ORG_ANCHR_ID = pol_admin.INT_ORG_ANCHR_ID
left join int_org_bu on int_org_bu.LOW_LVL_ORG_ID = int_org.LOW_LVL_ORG_ID
left join veh on veh.ANCHR_ID = cml.AGMT_ANCHR_ID
left join fsp on fsp.MKT_PRD_SPEC_ID = cml.MKT_PRD_SPEC_ID
left join ref_tables on ref_tables.POL_NBR = cml.POL_NBR
