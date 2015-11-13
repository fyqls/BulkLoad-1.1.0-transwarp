set hyperbase.reader=true;

drop table if exists MINSHENG_USER;

create table MINSHENG_USER AS
select concat_ws(',', base.rk.custno, base.custname,
		      base.sex,
		      base.custtype,
		      sub.AGEPART,
		      sub.DEPOSITBALAVGL3MPART,
	 	      sub.FASSETBALPART,
		      con.DEPOSITBALAVGL3M,
		      con.FASSETBAL,
		      con.FASSETBALMAX,
		      con.FASSETBALAVGMMAX,
		      con.FASSETBALAVGL3M)
from hyperbase_fin_cust_info_base base 
join
  hyperbase_fin_cust_info_sub sub
on base.rk.custno = sub.rk.custno 
join 
  hyperbase_fin_confinprofile_base con
on base.rk.custno = con.rk.custno;

