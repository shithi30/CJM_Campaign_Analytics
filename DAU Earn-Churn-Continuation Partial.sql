/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=1672659306
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

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	loop	
		delete from data_vajapora.help_b 
		where report_date=var_date;
	
		-- DAUs 
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		
		select mobile_no, created_datetime report_date
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime in(var_date, var_date-1)
		
		union 
		
		select mobile_no, event_date report_date
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date in(var_date, var_date-1)
			and event_name not in ('in_app_message_received','inbox_message_received')
			
		union 
			
		select ss.mobile_number mobile_no, ss.created_at::date report_date
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date in(var_date, var_date-1);  
			
		insert into data_vajapora.help_b
		select 
			var_date report_date, 
			count(tbl1.mobile_no) dau, 
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) continued_dau, 
			count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) wonback_dau
		from 
			(select mobile_no 
			from data_vajapora.help_a 
			where report_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile_no 
			from data_vajapora.help_a 
			where report_date=var_date-1
			) tbl2 using(mobile_no); 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date-20 then exit;
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.help_b
order by 1; 
