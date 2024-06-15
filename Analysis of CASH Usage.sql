/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=725604785
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1541103639
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: to be collected
- Email thread: 
- Notes (if any): 
*/

-- 39706 unique users of CASH in the last 30 days
select count(distinct mobile_no) merchants_last_30_days
from tallykhata.tallykhata_fact_info_final 
where
	created_datetime>=current_date-30 and created_datetime<current_date 
	and txn_type in('CASH_PURCHASE', 'CASH_SALE'); 

-- DAUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select distinct created_datetime date, mobile_no
from tallykhata.tallykhata_fact_info_final 
where
	created_datetime>=current_date-30 and created_datetime<current_date 
	and txn_type in('CASH_PURCHASE', 'CASH_SALE');
	
-- PUs
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select distinct report_date date, mobile_no 
from tallykhata.tk_power_users_10 
where report_date>=current_date-30 and report_date<current_date;
	
-- 3RAUs
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select rau_date date, mobile_no
from tallykhata.tallykhata_regular_active_user 
where 
	rau_category=3
	and rau_date>=current_date-30 and rau_date<current_date;

-- combined daily info. 
select 
	date, 
	cash_merchants,
	pus_in_cash_merchants, pus_in_cash_merchants_pct, pus, pus_in_cash_merchants*1.00/pus pus_using_cash_pct,
	raus_in_cash_merchants, raus_in_cash_merchants_pct, raus, raus_in_cash_merchants*1.00/raus raus_using_cash_pct
from 
	(select 
		tbl1.date, 
		count(tbl1.mobile_no) cash_merchants,
		count(tbl2.mobile_no) pus_in_cash_merchants,
		count(tbl3.mobile_no) raus_in_cash_merchants,
		count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) pus_in_cash_merchants_pct,
		count(tbl3.mobile_no)*1.00/count(tbl1.mobile_no) raus_in_cash_merchants_pct
	from 
		data_vajapora.help_a tbl1 
		left join 
		data_vajapora.help_b tbl2 on(tbl1.date=tbl2.date and tbl1.mobile_no=tbl2.mobile_no)
		left join 
		data_vajapora.help_c tbl3 on(tbl1.date=tbl3.date and tbl1.mobile_no=tbl3.mobile_no)
	group by 1
	) tbl1 
	
	inner join 
	
	(select date, count(mobile_no) pus
	from data_vajapora.help_b
	group by 1
	) tbl2 using(date)
	
	inner join 
		
	(select date, count(mobile_no) raus
	from data_vajapora.help_c
	group by 1
	) tbl3 using(date)
order by 1 asc; 
	
-- temp table for usage distribution
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select distinct mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where
		created_datetime>=current_date-30 and created_datetime<current_date 
		and txn_type in('CASH_PURCHASE', 'CASH_SALE')
	) tbl1 
	
	inner join 
	
	(select mobile_no, txn_type, created_datetime, created_timestamp, entry_type, auto_id 
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>=current_date-30 and created_datetime<current_date 
	) tbl2 using(mobile_no);

-- usage distribution in terms of active days
select usage_frequency, count(mobile_no) cash_merchants
from 
	(select 
		*,
		case 
			when cash_txn_dates_pct>=0.0 and cash_txn_dates_pct<0.1 then 'used_cash_00_to_10_pct_days'
			when cash_txn_dates_pct>=0.1 and cash_txn_dates_pct<0.2 then 'used_cash_10_to_20_pct_days'
			when cash_txn_dates_pct>=0.2 and cash_txn_dates_pct<0.3 then 'used_cash_20_to_30_pct_days'
			when cash_txn_dates_pct>=0.3 and cash_txn_dates_pct<0.4 then 'used_cash_30_to_40_pct_days'
			when cash_txn_dates_pct>=0.4 and cash_txn_dates_pct<0.5 then 'used_cash_40_to_50_pct_days'
			when cash_txn_dates_pct>=0.5 and cash_txn_dates_pct<0.6 then 'used_cash_50_to_60_pct_days'
			when cash_txn_dates_pct>=0.6 and cash_txn_dates_pct<0.7 then 'used_cash_60_to_70_pct_days'
			when cash_txn_dates_pct>=0.7 and cash_txn_dates_pct<0.8 then 'used_cash_70_to_80_pct_days'
			when cash_txn_dates_pct>=0.8 and cash_txn_dates_pct<0.9 then 'used_cash_80_to_90_pct_days'
			when cash_txn_dates_pct>=0.9 and cash_txn_dates_pct<=1.0 then 'used_cash_90_to_100_pct_days'
			else null
		end usage_frequency
	from 
		(select 
			mobile_no, 
			/*count(auto_id) all_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end) cash_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end)*1.00/count(auto_id) cash_txns_pct,*/
			count(distinct created_datetime) all_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end) cash_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end)*1.00/count(distinct created_datetime) cash_txn_dates_pct
		from data_vajapora.help_a 
		group by 1
		) tbl1 
	) tbl1
group by 1
order by 1; 

-- usage distribution in terms of txns
select usage_frequency_txns, count(mobile_no) cash_merchants
from 
	(select 
		*,
		case 
			when cash_txns_pct>=0.0 and cash_txns_pct<0.1 then 'used_cash_in_00_to_10_pct_txns'	
			when cash_txns_pct>=0.1 and cash_txns_pct<0.2 then 'used_cash_in_10_to_20_pct_txns'					
			when cash_txns_pct>=0.2 and cash_txns_pct<0.3 then 'used_cash_in_20_to_30_pct_txns'					
			when cash_txns_pct>=0.3 and cash_txns_pct<0.4 then 'used_cash_in_30_to_40_pct_txns'					
			when cash_txns_pct>=0.4 and cash_txns_pct<0.5 then 'used_cash_in_40_to_50_pct_txns'					
			when cash_txns_pct>=0.5 and cash_txns_pct<0.6 then 'used_cash_in_50_to_60_pct_txns'					
			when cash_txns_pct>=0.6 and cash_txns_pct<0.7 then 'used_cash_in_60_to_70_pct_txns'					
			when cash_txns_pct>=0.7 and cash_txns_pct<0.8 then 'used_cash_in_70_to_80_pct_txns'					
			when cash_txns_pct>=0.8 and cash_txns_pct<0.9 then 'used_cash_in_80_to_90_pct_txns'					
			when cash_txns_pct>=0.9 and cash_txns_pct<=1.0 then 'used_cash_in_90_to_100_pct_txns'									
			else null
		end usage_frequency_txns
	from 
		(select 
			mobile_no, 
			count(auto_id) all_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end) cash_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end)*1.00/count(auto_id) cash_txns_pct
			/*count(distinct created_datetime) all_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end) cash_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end)*1.00/count(distinct created_datetime) cash_txn_dates_pct*/
		from data_vajapora.help_a 
		group by 1
		) tbl1 
	) tbl1
group by 1; 

-- usage distribution in terms of active days in the last 30 days, with range
select usage_frequency, concat(min(cash_txn_dates), ' to ', max(cash_txn_dates), ' days') day_limit, count(mobile_no) cash_merchants
from 
	(select 
		*,
		case 
			when cash_txn_dates_pct>=0.0 and cash_txn_dates_pct<0.1 then 'used_cash_00_to_10_pct_days'
			when cash_txn_dates_pct>=0.1 and cash_txn_dates_pct<0.2 then 'used_cash_10_to_20_pct_days'
			when cash_txn_dates_pct>=0.2 and cash_txn_dates_pct<0.3 then 'used_cash_20_to_30_pct_days'
			when cash_txn_dates_pct>=0.3 and cash_txn_dates_pct<0.4 then 'used_cash_30_to_40_pct_days'
			when cash_txn_dates_pct>=0.4 and cash_txn_dates_pct<0.5 then 'used_cash_40_to_50_pct_days'
			when cash_txn_dates_pct>=0.5 and cash_txn_dates_pct<0.6 then 'used_cash_50_to_60_pct_days'
			when cash_txn_dates_pct>=0.6 and cash_txn_dates_pct<0.7 then 'used_cash_60_to_70_pct_days'
			when cash_txn_dates_pct>=0.7 and cash_txn_dates_pct<0.8 then 'used_cash_70_to_80_pct_days'
			when cash_txn_dates_pct>=0.8 and cash_txn_dates_pct<0.9 then 'used_cash_80_to_90_pct_days'
			when cash_txn_dates_pct>=0.9 and cash_txn_dates_pct<=1.0 then 'used_cash_90_to_100_pct_days'
			else null
		end usage_frequency
	from 
		(select 
			mobile_no, 
			/*count(auto_id) all_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end) cash_txns,
			count(case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then auto_id else null end)*1.00/count(auto_id) cash_txns_pct,*/
			count(distinct created_datetime) all_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end) cash_txn_dates,
			count(distinct case when txn_type in('CASH_PURCHASE', 'CASH_SALE') then created_datetime else null end)*1.00/30 cash_txn_dates_pct
		from data_vajapora.help_a 
		group by 1
		) tbl1 
	) tbl1
group by 1
order by 1; 
