create table xxoo tblproperties('cache'='ram') as 
select concat_ws(',') from hyperbase_fin_cust_info_base as base

