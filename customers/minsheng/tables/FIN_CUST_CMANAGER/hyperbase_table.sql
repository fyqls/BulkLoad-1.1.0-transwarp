drop table hyperbase_FIN_CUST_CMANAGER;
CREATE EXTERNAL TABLE hyperbase_FIN_CUST_CMANAGER (
  rk struct<CUSTNO:STRING>,
  CEMPID STRING
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f:CEMPID"
)TBLPROPERTIES ("hbase.table.name" = "FIN_CUST_CMANAGER");
