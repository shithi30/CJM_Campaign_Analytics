/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1255400141
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=94011202
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Distributions
	Earn, churn, winback segment distribution by biz type (first date of each month)
	ex: take january 01 SU and February 01 SU.
	measure earn,churn, winback
	show distribution by biz type
*/

-- monthly SUs
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *, dense_rank() over(order by report_date) yr_mon_seq
from 
	(select to_char(report_date, 'YYYY-MM') yr_mon, max(report_date) report_date
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type in('SPU')
		and report_date<current_date
	group by 1
	) tbl1 
	
	inner join 
	
	(select mobile_no, report_date
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type in('SPU')
		and report_date<current_date
	) tbl2 using(report_date); 

-- biz types by Mahmud
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	mobile_no, 
	case 
		when business_type in('BAKERY_AND_CONFECTIONERY') then 'SWEETS AND CONFECTIONARY'
		when business_type in('ELECTRONICS') then 'ELECTRONICS STORE'
		when business_type in('MFS_AGENT','MFS_MOBILE_RECHARGE') then 'MFS-MOBILE RECHARGE STORE'
		when business_type in('GROCERY') then 'GROCERY'
		when business_type in('DISTRIBUTOR_OR_WHOLESALE','WHOLESALER','DEALER') then 'OTHER WHOLESELLER'
		when business_type in('HOUSEHOLD_AND_FURNITURE') then 'FURNITURE SHOP'
		when business_type in('STATIONERY') then 'STATIONARY BUSINESS'
		when business_type in('TAILORS') then 'TAILERS'
		when business_type in('PHARMACY') then 'PHARMACY'
		when business_type in('SHOE_STORE') then 'SHOE STORE'
		when business_type in('MOTOR_REPAIR') then 'VEHICLE-CAR SERVICING'
		when business_type in('COSMETICS') then 'COSMETICS AND PERLOUR'
		when business_type in('ROD_CEMENT') then 'CONSTRUCTION RAW MATERIAL'
		when business_type='' then upper(case when new_bi_business_type!='Other Business' then new_bi_business_type else null end) 
		else null 
	end biz_type
from 
	(select id, business_type 
	from public.register_tallykhatauser 
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(id) id
	from public.register_tallykhatauser 
	group by 1
	) tbl2 using(id)
	
	inner join 
	
	(select 
		mobile mobile_no, 
		max(new_bi_business_type) new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl3 using(mobile_no); 

-- earn/churn distribution
select 
	tbl1.yr_mon, 
	tbl1.yr_mon_seq,
	case when tbl5.biz_type is null then 'OTHER BUSINESS' else biz_type end biz_type,  
	count(tbl1.mobile_no) sus, 
	count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) sus_cont, 
	count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) sus_earned, 
	count(case when tbl2.mobile_no is null and tbl1.yr_mon_seq=min_yr_mon_seq then tbl1.mobile_no else null end) sus_earned_new, 
	count(case when tbl2.mobile_no is null and tbl1.yr_mon_seq!=min_yr_mon_seq then tbl1.mobile_no else null end) sus_earned_winback, 
	count(case when tbl4.mobile_no is null then tbl1.mobile_no else null end) sus_churned
from 
	(select yr_mon, yr_mon_seq, mobile_no 
	from data_vajapora.help_a
	) tbl1 
	
	left join 
	
	(select yr_mon, yr_mon_seq, mobile_no 
	from data_vajapora.help_a
	) tbl2 on(tbl2.yr_mon_seq=tbl1.yr_mon_seq-1 and tbl1.mobile_no=tbl2.mobile_no)
	
	left join 
		
	(select yr_mon, yr_mon_seq, mobile_no 
	from data_vajapora.help_a
	) tbl4 on(tbl4.yr_mon_seq=tbl1.yr_mon_seq+1 and tbl1.mobile_no=tbl4.mobile_no)
	
	left join 
	
	(select mobile_no, min(yr_mon_seq) min_yr_mon_seq
	from data_vajapora.help_a 
	group by 1
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
	
	left join 
	
	data_vajapora.help_b tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
group by 1, 2, 3; 
