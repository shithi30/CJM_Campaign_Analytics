/*
- Viz: https://docs.google.com/spreadsheets/d/12OXMBFt5qQu1CzI-jQHmHCruLkVxirbjLz-y04BvbiA/edit#gid=1653096062
- Data: https://docs.google.com/spreadsheets/d/12OXMBFt5qQu1CzI-jQHmHCruLkVxirbjLz-y04BvbiA/edit#gid=0
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

/* lifetime: to draw curve */

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	count(mobile_no) total_reg,
	count(rau_3_mobile_no) total_3rau,
	count(rau_10_mobile_no) total_10rau,
	count(pu_mobile_no) total_pu,
	count(txn_mobile_no) total_txn_dau
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	left join 
	
	(select distinct mobile_no as rau_3_mobile_no
	from tallykhata.regular_active_user_event
	where rau_category=3
	) tbl2 on(tbl1.mobile_no=tbl2.rau_3_mobile_no)
	
	left join 
	
	(select distinct mobile_no as rau_10_mobile_no
	from tallykhata.regular_active_user_event
	where rau_category=10
	) tbl3 on(tbl1.mobile_no=tbl3.rau_10_mobile_no)
	
	left join 
	
	(select distinct mobile_no as pu_mobile_no
	from tallykhata.tk_power_users_10
	) tbl4 on(tbl1.mobile_no=tbl4.pu_mobile_no)
	
	left join 
	
	(select distinct mobile_no as txn_mobile_no
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	) tbl5 on(tbl1.mobile_no=tbl5.txn_mobile_no)
group by 1
order by 1; 
select *
from data_vajapora.help_a;

/* last 3 months: to draw curve */

drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	count(mobile_no) total_reg,
	count(rau_3_mobile_no) total_3rau,
	count(rau_10_mobile_no) total_10rau,
	count(pu_mobile_no) total_pu,
	count(txn_mobile_no) total_txn_dau
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	left join 
	
	(select distinct mobile_no as rau_3_mobile_no
	from tallykhata.regular_active_user_event
	where 
		rau_category=3
		and report_date::date>=current_date-30 and report_date::date<current_date
	) tbl2 on(tbl1.mobile_no=tbl2.rau_3_mobile_no)
	
	left join 
	
	(select distinct mobile_no as rau_10_mobile_no
	from tallykhata.regular_active_user_event
	where 
		rau_category=10
		and report_date::date>=current_date-30 and report_date::date<current_date
	) tbl3 on(tbl1.mobile_no=tbl3.rau_10_mobile_no)
	
	left join 
	
	(select distinct mobile_no as pu_mobile_no
	from tallykhata.tk_power_users_10
	where report_date>=current_date-30 and report_date<current_date
	) tbl4 on(tbl1.mobile_no=tbl4.pu_mobile_no)
	
	left join 
	
	(select distinct mobile_no as txn_mobile_no
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-30 and created_datetime<current_date
	) tbl5 on(tbl1.mobile_no=tbl5.txn_mobile_no)
group by 1
order by 1; 
select *
from data_vajapora.help_d;

/* lifetime: to pivot */

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
-- 3RAU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as rau_3_mobile_no, '3RAU' grp
	from tallykhata.regular_active_user_event
	where rau_category=3
	) tbl2 on(tbl1.mobile_no=tbl2.rau_3_mobile_no)
where grp is not null
group by 1, 2

union

-- 10RAU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as rau_10_mobile_no, '10RAU' grp
	from tallykhata.regular_active_user_event
	where rau_category=10
	) tbl3 on(tbl1.mobile_no=tbl3.rau_10_mobile_no)
where grp is not null
group by 1, 2

union 

-- PU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as pu_mobile_no, 'PU' grp
	from tallykhata.tk_power_users_10
	) tbl4 on(tbl1.mobile_no=tbl4.pu_mobile_no)
where grp is not null
group by 1, 2

union

-- Txn Active
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as txn_mobile_no, 'Transacting Active' grp
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	) tbl5 on(tbl1.mobile_no=tbl5.txn_mobile_no)
where grp is not null
group by 1, 2; 
select *
from data_vajapora.help_b; 

/* last 30 days: to pivot */

drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
-- 3RAU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as rau_3_mobile_no, '3RAU' grp
	from tallykhata.regular_active_user_event
	where 
		rau_category=3
		and report_date::date>=current_date-30 and report_date::date<current_date
	) tbl2 on(tbl1.mobile_no=tbl2.rau_3_mobile_no)
where grp is not null
group by 1, 2

union

-- 10RAU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as rau_10_mobile_no, '10RAU' grp
	from tallykhata.regular_active_user_event
	where 
		rau_category=10
		and report_date::date>=current_date-30 and report_date::date<current_date
	) tbl3 on(tbl1.mobile_no=tbl3.rau_10_mobile_no)
where grp is not null
group by 1, 2

union 

-- PU
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as pu_mobile_no, 'PU' grp
	from tallykhata.tk_power_users_10
	where report_date>=current_date-30 and report_date<current_date
	) tbl4 on(tbl1.mobile_no=tbl4.pu_mobile_no)
where grp is not null
group by 1, 2

union

-- Txn Active
select 
	to_char(reg_date, 'YYYY-MM') year_month, 
	grp,
	count(mobile_no) merchants
from 
	(select mobile_number as mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where to_char(created_at, 'YYYY-MM')>='2020-07' and to_char(created_at::date, 'YYYY-MM')<='2021-07'
	) tbl1 
	
	inner join 
	
	(select distinct mobile_no as txn_mobile_no, 'Transacting Active' grp
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>=current_date-30 and created_datetime<current_date
	) tbl5 on(tbl1.mobile_no=tbl5.txn_mobile_no)
where grp is not null
group by 1, 2; 
select *
from data_vajapora.help_c; 

