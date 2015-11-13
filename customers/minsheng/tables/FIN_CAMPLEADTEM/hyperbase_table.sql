drop table hyperbase_FIN_CAMPLEADTEM;
CREATE EXTERNAL TABLE hyperbase_FIN_CAMPLEADTEM (
  rk struct<CLTID:STRING>,
  EXECUTEDATE STRING
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,f1:EXECUTEDATE"
)TBLPROPERTIES ("hbase.table.name" = "FIN_CAMPLEADTEM");
