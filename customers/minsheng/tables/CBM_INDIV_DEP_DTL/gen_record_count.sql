SET hive.exec.reducers.max=1; 
SET hive.exec.compress.output=false;

--set mapred.reduce.tasks=1;
INSERT OVERWRITE LOCAL DIRECTORY '/root/transwarp/hbase-bulkload-1.0.0/customers/minsheng/tables/CBM_INDIV_DEP_DTL/sample'
select c1 from 
(select concat(substring(ACT_NUM,1,10), ',', count(*)) as c1, substring(ACT_NUM,1,10)  as c2, count(*) as c3 from sample_CBM_INDIV_DEP_DTL
group by substring(ACT_NUM,1,10) order by c2) t;
