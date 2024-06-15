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
- Email thread: Request for MAU campaign data!
- Notes (if any): 
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select distinct mobile_no 
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_name='app_opened' 
	and event_date>'2021-12-31' and event_date<current_date; 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as 
select mobile_no
from cjm_segmentation.retained_users 
where report_date=current_date; 
	
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select mobile_no
from 
	data_vajapora.help_b tbl1 
	left join 
	data_vajapora.help_a tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

select * 
from data_vajapora.help_c; 
