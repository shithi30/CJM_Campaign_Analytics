/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.merchants_weekly_info_temp
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- merchants' daily TRT, TACS
drop table if exists data_vajapora.daily_trt_tacs; 
create table data_vajapora.daily_trt_tacs as
select 
	mobile_no, 
	created_datetime,
	count(case when entry_type=1 then auto_id else null end) lft_trt,
	count(case when entry_type=2 then auto_id else null end) lft_tacs
from tallykhata.tallykhata_fact_info_final 
group by 1, 2; 

-- weekly time spent
drop table if exists data_vajapora.help_x; 
create table data_vajapora.help_x as
select mobile_no, to_char(event_date, 'YYYY-WW') event_year_week, sum(sec_with_tk) sec_spent_in_week
from tallykhata.daily_times_spent_individual_data 
group by 1, 2; 
	
-- weekly active days
drop table if exists data_vajapora.help_y; 
create table data_vajapora.help_y as
select *, row_number() over(partition by mobile_no order by event_year_week asc) week_seq 
from
	(select mobile_no, to_char(event_date, 'YYYY-WW') event_year_week, count(date_sequence) active_days
	from tallykhata.tallykhata_user_date_sequence_final
	group by 1, 2
	) tbl1;

-- weekly TRT/TACS
drop table if exists data_vajapora.help_z; 
create table data_vajapora.help_z as
select mobile_no, to_char(created_datetime, 'YYYY-WW') event_year_week, sum(lft_trt) trt, sum(lft_tacs) tacs
from data_vajapora.daily_trt_tacs 
group by 1, 2;    

-- weekly RAU-days
drop table if exists data_vajapora.help_p; 
create table data_vajapora.help_p as
select mobile_no, to_char(report_date::date, 'YYYY-WW') event_year_week, count(report_date) rau_3_days
from tallykhata.regular_active_user_event as s 
where rau_category=3 
group by 1, 2; 

-- weekly data, combined
drop table if exists data_vajapora.merchants_weekly_info_temp; 
create table data_vajapora.merchants_weekly_info_temp as
select tbl1.mobile_no, reg_date, new_bi_business_type, tbl1.event_year_week, week_seq, active_days, sec_spent_in_week, trt, tacs, rau_3_days
from 
	data_vajapora.help_y tbl1 
	left join 
	data_vajapora.help_x tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_year_week=tbl2.event_year_week)
	left join 
	data_vajapora.help_z tbl3 on(tbl1.mobile_no=tbl3.mobile_no and tbl1.event_year_week=tbl3.event_year_week)
	left join 
	data_vajapora.help_p tbl5 on(tbl1.mobile_no=tbl5.mobile_no and tbl1.event_year_week=tbl5.event_year_week)
	left join 
	(select mobile mobile_no, registration_date reg_date, new_bi_business_type 
	from tallykhata.tallykhata_user_personal_info
	) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
order by 1, 3; 

-- see the weekly metrics of merchant
select *
from data_vajapora.merchants_weekly_info_temp
where reg_date>='2021-04-01'; 
