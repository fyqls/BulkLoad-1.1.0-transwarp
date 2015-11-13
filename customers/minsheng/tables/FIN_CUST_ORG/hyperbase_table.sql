drop table hyperbase_FIN_CUST_ORG;
CREATE EXTERNAL TABLE hyperbase_FIN_CUST_ORG (
  rk struct<CUSTNO:STRING>,
  ORGID STRING
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f:ORGID"
)TBLPROPERTIES ("hbase.table.name" = "FIN_CUST_ORG");
