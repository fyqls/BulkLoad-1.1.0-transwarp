DROP TABLE IF EXISTS sample_FIN_CUST_INFO_BASE;
CREATE EXTERNAL TABLE FIN_CUST_INFO_BASE (
        allvalue string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/import_data/sadata/FIN_CUST_INFO_BASE';
