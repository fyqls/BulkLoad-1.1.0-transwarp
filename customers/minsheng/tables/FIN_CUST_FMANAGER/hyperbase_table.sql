drop table hyperbase_FIN_CUST_FMANAGER;
CREATE EXTERNAL TABLE hyperbase_FIN_CUST_FMANAGER (
  rk struct<CUSTNO:STRING>,
  FEMPID STRING 
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f:FEMPID"
)TBLPROPERTIES ("hbase.table.name" = "FIN_CUST_FMANAGER");
