/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=880641970
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=794605038
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- weeks against merchants
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select distinct mobile_no, to_char(event_date, 'YYYY-WW') act_year_week 
	from tallykhata.event_transacting_fact 
	where event_date>='2020-01-01' and event_date<current_date 
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no
	from public.register_usermobile 
	where date(created_at)>='2020-01-01' and date(created_at)<current_date
	) tbl2 using(mobile_no); 

-- weekly cohort table: numbers
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *, row_number() over(partition by min_act_year_week order by act_year_week asc) week_diff
from 
	(select min_act_year_week, act_year_week, count(distinct tbl1.mobile_no) merchants 
	from 
		(select mobile_no, min(act_year_week) min_act_year_week 
		from data_vajapora.help_a 
		group by 1 
		) tbl1 
		
		inner join 
		
		(select mobile_no, act_year_week 
		from data_vajapora.help_a 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
	group by 1, 2
	order by 1 asc, 2 asc
	) tbl1; 

-- weekly cohort table: ratios
select *, merchants*1.00/init_merchants merchants_retained_pct
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select min_act_year_week, merchants init_merchants 
	from data_vajapora.help_b
	where week_diff=1
	) tbl2 using(min_act_year_week)
where min_act_year_week>='2021-12' -- limit for viz.
