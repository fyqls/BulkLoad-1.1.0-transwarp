DROP TABLE IF EXISTS sample_CBM_INDIV_DEP_DTL;
CREATE EXTERNAL TABLE sample_CBM_INDIV_DEP_DTL (
  ITL_ACT string,
  DM_SEQ string,
  TXN_SEQ_NUM string,
  TXN_NUM string,
  BIL_YM_DTE string,
  OVE_DTE string,
  TXN_DTE string,
  ACT_NUM string,
  TXN_TME string,
  TXN_TYP_CD string,
  TXN_CNL_CD string,
  SYS_FLG string,
  TXN_ORG_ID string,
  TXN_SUR string,
  CSR_ID string,
  CRY_CD string,
  DEB_CRE_IND string,
  CAS_TFR_FLG string,
  TXN_AMT string,
  BAL string,
  IVC_BRF string,
  SRV_CODE string,
  PDT_ID string,
  DEB_CTE_KIN string,
  DEB_CTE_NUM string,
  DEB_DM_ACT string,
  CRD_DM_ACT string,
  CRD_CTE_KIN string,
  CRD_CTE_NUM string,
  TXN_ITM_TYP string,
  PO_ID string,
  OPP_CNY_CD string,
  OPP_BNK_CD string,
  OPP_ACT string,
  CHO_IND string,
  INT_DTE string,
  SRC_CRY string,
  SRC_TXN_AMT string,
  SBE_NME string,
  SBE_IDE_TYP_CD string,
  SBE_IDE_NUM string,
  DEB_SAFE_CD string,
  CRD_SAFE_CD string,
  SAFE_APP_CD string,
  BIG_TRANS_TEL string,
  BIG_TRANS_INF string,
  OGL_PYE_ACT string,
  FLU_SEQ string,
  FCHO_TXN_SEQ_NUM string,
  SEQ_STS string,
  DEB_ACT_NME string,
  DEB_BKN_NME string,
  CRD_ACT_NME string,
  CRD_BKN_NME string,
  PRO_SRV_CODE string,
  CHG_ITM_CD string,
  CHANNEL string,
  REMARK string,
  DAY_ACT_IND string,
  NF_IND string,
  ETL_LAD_DTE string,
  CUST_01 string,
  CUST_02 string,
  CUST_03 string,
  CUSTOM_01 string,
  CUSTOM_02 string,
  CUSTOM_03 string,
  CUSTOM_04 string,
  CUSTOM_05 string,
  CUSTOM_06 string,
  CUSTOM_07 string,
  PAY_CUST01 string,
  PAY_CUST02 string,
  RCV_CUST01 string,
  RCV_CUST02 string,
  FLG_RECT string,
  CUST_05 string,
  CUST_06 string,
  CUST_07 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\033'
STORED AS TEXTFILE
LOCATION '/import_data/cbmdata';
