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

CREATE OR REPLACE FUNCTION data_vajapora.fn_inactive_mau_data()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	var_date date:=concat(left(current_date::text, 7), '-01')::date-1; 
begin 
	drop table if exists data_vajapora.inactive_mau_help_a; 
	create table data_vajapora.inactive_mau_help_a as 
	-- opened app
	select mobile_no 
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_name='app_opened' 
		and event_date>var_date and event_date<current_date
	
	union 

	-- transacted
	select mobile_no 
	from public.journal 
	where date(txn_date)>var_date and date(txn_date)<current_date; 
	
	drop table if exists data_vajapora.inactive_mau_help_b; 
	create table data_vajapora.inactive_mau_help_b as 
	select mobile_no
	from cjm_segmentation.retained_users 
	where report_date=current_date; 
		
	drop table if exists data_vajapora.inactive_mau_data; 
	create table data_vajapora.inactive_mau_data as
	select mobile_no
	from 
		data_vajapora.inactive_mau_help_b tbl1 
		left join 
		data_vajapora.inactive_mau_help_a tbl2 using(mobile_no)
	where tbl2.mobile_no is null; 	
END;
$function$
;

select data_vajapora.fn_inactive_mau_data(); 

select *
from data_vajapora.inactive_mau_data; 



