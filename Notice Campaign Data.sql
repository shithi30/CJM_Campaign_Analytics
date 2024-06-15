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
- Email thread: Requesting for Notice Campaign Data!
- Notes (if any): 
*/

-- opened app this month
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select distinct mobile_no 
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_name='app_opened' 
	and event_date>concat(left(current_date::text, 7), '-01')::date-1 and event_date<current_date; 

-- transacted this month
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no 
from tallykhata.tallykhata_transacting_user_date_sequence_final 
where created_datetime>concat(left(current_date::text, 7), '-01')::date-1 and created_datetime<current_date; 

-- app opening MAUs who did no txn this month
drop table if exists data_vajapora.nontxn_notice_mau_data; 
create table data_vajapora.nontxn_notice_mau_data as
select *
from 
	(select mobile_no
	from 
		data_vajapora.help_a tbl1 
		left join 
		data_vajapora.help_b tbl2 using(mobile_no)
	where tbl2.mobile_no is null
	) tbl1 
	
	inner join 
	
	(select mobile_no 
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl3 using(mobile_no); 

select *
from data_vajapora.nontxn_notice_mau_data; 
