/*
- Viz: https://docs.google.com/spreadsheets/d/1OUp5jYnkGKikO-VG5hdcEHPNYaKsK4QBwlrHTJRL8Pg/edit#gid=266703093
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any):
*/

-- today: '2021-08-31'::date

-- retained merchants of today
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as 
select tbl2.mobile_no, reg_date, '2021-08-31'::date-reg_date+1 days_with_tk
from 
	(select concat('0', mobile_no) mobile_no
	from data_vajapora.retained_today
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	where date(created_at)<='2021-08-31'::date
	) tbl2 using(mobile_no); 

-- 3RAU statistics
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no, max(rau_date) max_3_rau_date, count(rau_date) rau_days
from tallykhata.tallykhata_regular_active_user
where 
	rau_category=3
	and rau_date<='2021-08-31'::date
group by 1; 

-- PU statistics
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select mobile_no, max(report_date) max_pu_date, count(distinct report_date) pu_days
from tallykhata.tk_power_users_10
where report_date<='2021-08-31'::date
group by 1; 

-- all features combined
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	tbl1.*, 
	case when tbl2.created_datetime is null then 0 else 1 end if_first_day_active,
	case when tbl3.txn_days is null then 0 else tbl3.txn_days end txn_days,
	case when tbl4.rau_days is null then 0 else tbl4.rau_days end days_in_3_rau,
	tbl4.max_3_rau_date, 
	'2021-08-31'::date-tbl4.max_3_rau_date days_after_last_3_rau,
	case when tbl5.mobile_no is null then 0 else 1 end if_pu_yesterday,
	case when tbl7.mobile_no is null then 0 else 1 end if_3_rau_yesterday, 
	case when active_last_30_days is null then 0 else active_last_30_days end active_last_30_days,
	case when tbl8.pu_days is null then 0 else tbl8.pu_days end days_in_pu,
	tbl8.max_pu_date
from 
	data_vajapora.help_d tbl1 
	
	left join 
	
	(select mobile_no, created_datetime 
	from tallykhata.tallykhata_transacting_user_date_sequence_final 
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.reg_date=tbl2.created_datetime)
	
	left join 
	
	(select mobile_no, count(created_datetime) txn_days 
	from tallykhata.tallykhata_transacting_user_date_sequence_final  
	where created_datetime<='2021-08-31'::date
	group by 1 
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
	
	left join 
	
	data_vajapora.help_a tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tk_power_users_10
	where report_date='2021-08-31'::date-1
	) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
	
	left join 
	
	(select mobile_no, count(created_datetime) active_last_30_days
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where created_datetime>='2021-08-31'::date-30 and created_datetime<'2021-08-31'::date 
	group by 1
	) tbl6 on(tbl1.mobile_no=tbl6.mobile_no)
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3
		and rau_date='2021-08-31'::date-1
	) tbl7 on(tbl1.mobile_no=tbl7.mobile_no)
	
	left join 
	
	data_vajapora.help_c tbl8 on(tbl1.mobile_no=tbl8.mobile_no); 

-- distribution of segments
select cjm_segment, count(*) merchants
from 
	(select 
		*,
		case
			when days_with_tk=1 and if_first_day_active=0 then 'age 1: day-01 not transacted'
			when days_with_tk=1 and if_first_day_active=1 then 'age 1: day-01 transacted'
			
			when days_with_tk>=2 and days_with_tk<=7 and txn_days=0 then 'age 2-7: no transaction'
			when days_with_tk>=2 and days_with_tk<=7 and days_in_3_rau=0 then 'age 2-7: did not enter 3RAU'
			when days_with_tk>=2 and days_with_tk<=7 and days_in_3_rau!=0 then 'age 2-7: entered 3RAU'
			
			when days_with_tk>=8 and days_with_tk<=29 and if_pu_yesterday=1 then 'age 8-29: PU yesterday'
			when days_with_tk>=8 and days_with_tk<=29 and txn_days=0 then 'age 8-29: no txn till now'
			when days_with_tk>=8 and days_with_tk<=29 and '2021-08-31'::date-max_3_rau_date=0 and days_in_3_rau=1 then 'age 8-29: today 3RAU 1st'
			when days_with_tk>=8 and days_with_tk<=29 and '2021-08-31'::date-max_3_rau_date=0 and days_in_3_rau>1 then 'age 8-29: today 3RAU cont.'
			when days_with_tk>=8 and days_with_tk<=29 and '2021-08-31'::date-max_3_rau_date=1 then 'age 8-29: was 3RAU yesterday'
			when days_with_tk>=8 and days_with_tk<=29 and '2021-08-31'::date-max_3_rau_date>1 then 'age 8-29: gaps after 3RAU'
			when days_with_tk>=8 and days_with_tk<=29 and max_3_rau_date is null then 'age 8-29: low usage'
			
			when days_with_tk>29 and txn_days=0 then 'age >29: no transaction'
			when days_with_tk>29 and active_last_30_days=0 then 'age >29: inactive last 29 days'
			when days_with_tk>29 and if_pu_yesterday=1 and active_last_30_days<20 then 'age >29: was PU yesterday'
			when days_with_tk>29 and if_pu_yesterday=1 and active_last_30_days>=20 then 'age >29: was SPU yesterday'
			when days_with_tk>29 and days_in_3_rau=0 then 'age >29: not entered 3RAU till now'
			when days_with_tk>29 and if_3_rau_yesterday=1 then 'age >29: 3RAU yesterday'
			
			-- remaining
			when days_with_tk>29 and days_in_pu=0 then 'age >29: never became PU'
			when days_with_tk>29 and days_in_pu>0 and '2021-08-31'::date-max_pu_date!=0 then 'age >29: once PU currently not'
			when days_with_tk>29 and days_in_pu>0 and '2021-08-31'::date-max_pu_date=0 then 'age >29: once PU currently PU'
			
			else 'unknown'
		end cjm_segment
	from data_vajapora.help_b
	) tbl1 	
group by 1;

-- this query's result minus count(segmented merchants) is 'uninstalled'
select count(distinct mobile_number)
from public.register_usermobile

/*
-- PU equalization with DB

select *
from 
	(select distinct mobile_no
	from tallykhata.tk_power_users_10
	where report_date='2021-08-31'::date-1
	) tbl1
	
	left join 
	
	(select mobile_no
	from data_vajapora.help_b 
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

-- 157851 on DB, got 153920, rest 2921 missing from today's retained base
*/






