drop table hyperbase_CBM_INDIV_DEP_DTL;
CREATE EXTERNAL TABLE hyperbase_CBM_INDIV_DEP_DTL (
  rk struct<TXN_SEQ_NUM:STRING>,
  TXN_TME STRING,
  CSR_ID STRING,
  ACT_NUM STRING,
  TXN_TYP_CD STRING,
  TXN_ORG_ID STRING,
  TXN_SUR STRING,
  CRD_DM_ACT STRING
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f:TT,f:CI,f:AN,f:TTC,f:TOI,f:TS,f:CDA"
)TBLPROPERTIES ("hbase.table.name" = "CBM_INDIV_DEP_DTL");