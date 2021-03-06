drop table hyperbase_FIN_CUST_INFO_BASE;
CREATE EXTERNAL TABLE hyperbase_FIN_CUST_INFO_BASE (
  rk struct<CUSTNO:STRING>,
  CUSTNAME STRING,
  CUSTAGE STRING,
  SEX STRING,
  BRITHDAY STRING,
  CIVILSTATE STRING,
  FAMILYNO STRING,
  DUTY STRING,
  VOCATION STRING,
  TRADETYPE STRING,
  VOCATIONNAME STRING,
  TIPTOPDEGREE STRING,
  TIPTOPLEVEL STRING,
  ISCMBCEMPLOYEE STRING,
  ISCMBCCUSTOMER STRING,
  CUSTTYPE STRING,
  CUSTYEAR STRING,
  ACTFLAG STRING,
  CUSTCLASSIFICATION STRING
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f:CUSTNAME,f:CUSTAGE,f:SEX,f:BRITHDAY,f:CIVILSTATE,f:FAMILYNO,f:DUTY,f:VOCATION,f:TRADETYPE,f:VOCATIONNAME,f:TIPTOPDEGREE,f:TIPTOPLEVEL,f:ISCMBCEMPLOYEE,f:ISCMBCCUSTOMER,f:CUSTTYPE,f:CUSTYEAR,f:ACTFLAG,f:CUSTCLASSIFICATION"
)TBLPROPERTIES ("hbase.table.name" = "FIN_CUST_INFO_BASE");
