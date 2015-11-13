SET hive.exec.reducers.max=1; 
SET hive.exec.compress.output=false;

--set mapred.reduce.tasks=1;
INSERT OVERWRITE LOCAL DIRECTORY '/root/transwarp/hbase-bulkload-1.0.0/customers/minsheng/tables/FIN_LEADOPPS/sample'
select c1 from 
(select concat(substring(allvalue,1,10), ',', count(*)) as c1, substring(allvalue,1,10)  as c2, count(*) as c3 from sample_FIN_LEADOPPS
group by substring(allvalue,1,10) order by c2) t;
