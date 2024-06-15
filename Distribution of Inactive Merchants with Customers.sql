/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=2042706131
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
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct mobile_no 
from tallykhata.tallykhata_fact_info_final 
where created_datetime>=current_date-30 and created_datetime<current_date; 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile_no, count(id) added_customers 
from public.account 
where 
	"type"=2
	and is_active is true
	and date(create_date)<current_date-30
group by 1; 

-- frequency distribution
select 
	added_customers/5, 
	concat(min(added_customers)::text, ' to ', max(added_customers)::text) customers_added, 
	count(mobile_no) merchants_with_no_txn_last_30_days
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select distinct mobile_no 
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl2 using(mobile_no)
	
	left join 
	
	data_vajapora.help_a tbl3 using(mobile_no) 
where 
	tbl3.mobile_no is null
	and added_customers<150
group by 1; 

-- TG of interest
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select mobile_no
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select distinct mobile_no 
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl2 using(mobile_no)
	
	left join 
	
	data_vajapora.help_a tbl3 using(mobile_no) 
where 
	tbl3.mobile_no is null
	and added_customers>=50; 

-- biz types by Mahmud
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
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

-- biz-type distribution
select biz_type, count(mobile_no) merchants
from 
	data_vajapora.help_c tbl1 
	left join 
	data_vajapora.help_d tbl2 using(mobile_no)
group by 1 
order by 2 desc; 

-- last txn-day distribution
select to_char(max_txn_date, 'YYYY-MM') year_month, count(mobile_no) merchants_last_txned 
from 
	(select mobile_no, max(created_datetime) max_txn_date
	from 
		data_vajapora.help_c tbl1 
		left join 
		(select mobile_no, created_datetime 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		where created_datetime<current_date
		) tbl2 using(mobile_no) 
	group by 1 
	) tbl1 
group by 1
order by 1; 

-- TG distribution
select 
	case 
		when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
		when tg in('LTUCb','LTUTa') then 'LTU'
		when tg in('NT--') then 'NT'
		when tg in('NB0','NN1','NN2-6') then 'NN'
		when tg in('PSU') then 'PSU'
		when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
		when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
		else 'rest'
	end segment,  
	count(mobile_no) merchants
from 
	(select mobile_no, max_txn_date, max(tg) tg 
	from 
		(select mobile_no, max(created_datetime) max_txn_date
		from 
			data_vajapora.help_c tbl1 
			left join 
			(select mobile_no, created_datetime 
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime<current_date
			) tbl2 using(mobile_no) 
		group by 1 
		) tbl1 
		
		left join 
		
		(select mobile_no, report_date max_txn_date, tg
		from cjm_segmentation.retained_users 
		) tbl2 using(mobile_no, max_txn_date) 
	group by 1, 2	
	) tbl1 
group by 1; 

-- age distribution
select to_char(reg_date, 'YYYY-MM') year_month, count(mobile_no) merchants
from 
	(select mobile_no, max(created_datetime) max_txn_date
	from 
		data_vajapora.help_c tbl1 
		left join 
		(select mobile_no, created_datetime 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		where created_datetime<current_date
		) tbl2 using(mobile_no) 
	group by 1 
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl2 using(mobile_no)
group by 1
order by 1; 
