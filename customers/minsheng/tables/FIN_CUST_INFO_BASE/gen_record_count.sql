SET hive.exec.reducers.max=1; 
SET hive.exec.compress.output=false;

--set mapred.reduce.tasks=1;
INSERT OVERWRITE LOCAL DIRECTORY '/root/transwarp/hbase-bulkload-1.0.0/customers/minsheng/tables/FIN_CUST_INFO_BASE/sample'
select c1 from 
(select concat(substring(allvalue,1,10), ',', count(*)) as c1, substring(allvalue,1,10)  as c2, count(*) as c3 from FIN_CUST_INFO_BASE
group by substring(allvalue,1,10) order by c2) t;
