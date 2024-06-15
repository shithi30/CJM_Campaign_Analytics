/*
- Viz: 
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
	- numbers: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=195286261
	- pct: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=587014217 
	- data: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=1450142254
	- charts: https://docs.google.com/spreadsheets/d/1YAmv4xXw5SbFm5GM0RlD-PctNQBj1JrmJcyu1n7QSuM/edit#gid=272192865
	
*/

-- sequencing TK months 
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as 
select *, row_number() over(order by tk_month) tk_month_seq
from 
	(select distinct left(event_date::text, 7) tk_month
	from tallykhata.tallykhata_user_date_sequence_final
	) tbl1; 

-- min activity month
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select *
from 
	(select mobile_no, left(min(event_date)::text, 7) min_event_month 
	from 
		tallykhata.tallykhata_user_date_sequence_final tbl1
		inner join 
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile
		) tbl2 using(mobile_no)
	where event_date>=reg_date and event_date<current_date
	group by 1
	) tbl1 
	
	inner join 
	
	(select tk_month min_event_month, tk_month_seq 
	from data_vajapora.help_c
	) tbl2 using(min_event_month);  

-- all activity months
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as 
select *
from 
	(select distinct mobile_no, left(event_date::text, 7) event_month 
	from 
		tallykhata.tallykhata_user_date_sequence_final tbl1
		inner join 
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile
		) tbl2 using(mobile_no)
	where event_date>=reg_date and event_date<current_date
	) tbl1 

	inner join 
	
	(select tk_month event_month, tk_month_seq 
	from data_vajapora.help_c
	) tbl2 using(event_month); 

-- cohort: numbers 
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as 
select tbl1.min_event_month, tbl2.event_month, count(tbl1.mobile_no) merchants, row_number() over(partition by tbl1.min_event_month order by tbl2.event_month asc) seq
from 
	(select left(date(created_at)::text, 7) min_event_month, mobile_number mobile_no 
	from public.register_usermobile
	) tbl0 
	inner join
	data_vajapora.help_a tbl1 using(min_event_month, mobile_no)
	inner join 
	data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
	inner join 
	data_vajapora.help_b tbl3 on(tbl3.mobile_no=tbl2.mobile_no and tbl3.tk_month_seq=tbl2.tk_month_seq-1)
group by 1, 2; 

-- cohort: pct
select min_event_month, event_month, seq, reg, merchants, merchants_start, merchants*1.00/merchants_start merchants_start_pct
from 
	data_vajapora.help_d tbl1 
	
	inner join 
	
	(select min_event_month, merchants merchants_start
	from data_vajapora.help_d  
	where seq=1
	) tbl2 using(min_event_month) 
	
	inner join 
	
	(-- registrations
	select left(date(created_at)::text, 7) min_event_month, count(mobile_number) reg
	from public.register_usermobile
	group by 1
	) tbl3 using(min_event_month)
where min_event_month>='2021-01'
order by 1, 2; 

