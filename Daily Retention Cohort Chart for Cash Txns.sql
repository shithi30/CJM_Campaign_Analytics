/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=75643027
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

-- first cash txns
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no, min(created_datetime) min_cash_txn_date
from tallykhata.tallykhata_fact_info_final 
where txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_PURCHASE', 'CASH_SALE', 'CASH_ADJUSTMENT')
group by 1
having min(created_datetime)>='2021-09-29' and min(created_datetime)<current_date;
	
-- all cash txns
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, created_datetime cash_txn_date
from tallykhata.tallykhata_fact_info_final 
where 
	txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_PURCHASE', 'CASH_SALE', 'CASH_ADJUSTMENT')	
	and created_datetime<current_date;
	
-- retention cohort with numbers
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	min_cash_txn_date,
	cash_txn_date-min_cash_txn_date days_passed,
	count(distinct mobile_no) merchants_retained
from 
	data_vajapora.help_a tbl1 

	inner join 

	data_vajapora.help_b tbl2 using(mobile_no)
group by 1, 2
order by 1, 2; 	

-- retention cohort with pct
select *, merchants_retained*1.00/merchants_retained_day_0 merchants_retained_pct
from 
	data_vajapora.help_c tbl1
	
	inner join 

	(select min_cash_txn_date, merchants_retained merchants_retained_day_0
	from data_vajapora.help_c
	where days_passed=0
	) tbl2 using(min_cash_txn_date); 
