/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.retained_12_Aug_cash, data_vajapora.retained_12_Aug_cust
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Re: Request for User Data
- Notes (if any): Entry made to details sheet: https://docs.google.com/spreadsheets/d/1Q2bNuJIDwV8kvszFdfrZ4reCIuQ3D6p0Z8bWt9dR5-8/edit#gid=0
*/

/* for live */

select mobile_no
from 
	(select device_id, created_at app_status_created_at, app_status
	from public.notification_fcmtoken 
	) tbl1 
	
	inner join 
	
	(select id, device_id, mobile mobile_no, created_at device_status_created_at, device_status
	from public.registered_users 
	) tbl2 using(device_id)
	
	inner join 

	(select mobile mobile_no, max(id) id
	from public.registered_users 
	group by 1
	) tbl3 using(mobile_no, id)
where app_status='ACTIVE' and device_status='active' 
	
-- import from live to DWH via Python: data_vajapora.retained_12_Aug

/* for DWH */

select *
from data_vajapora."retained_12_Aug"; 

-- for cash txns

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no, count(auto_id) cash_txns, max(created_datetime) last_cash_txn_date
from tallykhata.tallykhata_fact_info_final 
where txn_type in('CASH_SALE', 'CASH_PURCHASE')
group by 1; 

drop table if exists data_vajapora.retained_12_Aug_cash;
create table data_vajapora.retained_12_Aug_cash as
select *
from 
	data_vajapora."retained_12_Aug" tbl1 
	left join 
	(select mobile_no
	from data_vajapora.help_a 
	where 
		last_cash_txn_date>=current_date-30 
		or 
		cash_txns>5
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

select *
from data_vajapora.retained_12_Aug_cash;

-- for cust adds

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, count(auto_id) cust_adds, max(created_datetime) last_cust_add_date
from tallykhata.tallykhata_fact_info_final 
where txn_type in('Add Customer')
group by 1;

drop table if exists data_vajapora.retained_12_Aug_cust;
create table data_vajapora.retained_12_Aug_cust as
select *
from 
	data_vajapora."retained_12_Aug" tbl1 
	left join 
	(select mobile_no
	from data_vajapora.help_b
	where 
		last_cust_add_date>=current_date-30 
		or 
		cust_adds>5
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

select *
from data_vajapora.retained_12_Aug_cust;