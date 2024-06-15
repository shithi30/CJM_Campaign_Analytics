/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=631431184
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
	var_date date:='2022-05-01'::date; -- month start date
	var_month text:=left(var_date::text, 7); 
begin
	raise notice 'New OP goes below:'; 

	-- MAU (to be accumulated)
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b(mobile_no text); 

	-- first open through inbox MAU
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select mobile_no, report_date, notification_id
	from 
		(select mobile_no, min(report_date) report_date
		from data_vajapora.mom_cjm_performance_detailed
		where 
			left(report_date::text, 7)=var_month
			and id is not null
		group by 1 
		) tbl1
		
		inner join 
		
		(select mobile_no, report_date, notification_id
		from data_vajapora.mom_cjm_performance_detailed
		where 
			left(report_date::text, 7)=var_month
			and id is not null
		) tbl2 using(mobile_no, report_date); 
	
	loop
		delete from data_vajapora.mau_campaign_results
		where report_date=var_date;
	
		-- DAU
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		
		select mobile_no
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union
		
		select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received', 'inbox_message_received')
			
		union
			
		select ss.mobile_number mobile_no
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date; 
		
		insert into data_vajapora.mau_campaign_results
		select 
			var_date, 
			count(mobile_no) new_mau, 
			count(case when notification_id in(2672, 2681, 2666, 2668, 2662, 2935, 2682, 2574, 2745, 2693, 2573, 2687, 2667, 2838, 3007, 3032, 3031) then mobile_no else null end) first_open_through_inbox_mau_message,                 
			count(case when notification_id not in(2672, 2681, 2666, 2668, 2662, 2935, 2682, 2574, 2745, 2693, 2573, 2687, 2667, 2838, 3007, 3032, 3031) then mobile_no else null end) first_open_through_inbox_other_message                 
		from 
			(-- new MAU
			select mobile_no 
			from 
				data_vajapora.help_a tbl1
				left join 
				data_vajapora.help_b tbl2 using(mobile_no)
			where tbl2.mobile_no is null
			) tbl1 
			
			left join 
			
			(select mobile_no, notification_id 
			from data_vajapora.help_c 
			where report_date=var_date
			) tbl2 using(mobile_no); 
		
		-- new MAU accumulation
		insert into data_vajapora.help_b 
		select mobile_no 
		from 
			data_vajapora.help_a tbl1
			left join 
			data_vajapora.help_b tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
			
		commit; 
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit; -- month+1 start date/current_date
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.mau_campaign_results
order by 1; 

select 
	left(report_date::text, 7) year_month, 
	sum(new_mau) mau, 
	sum(first_open_through_inbox_mau_message) mau_through_inbox_mau_message, 
	sum(first_open_through_inbox_other_message) mau_through_inbox_other_message, 
	sum(new_mau)-sum(first_open_through_inbox_mau_message)-sum(first_open_through_inbox_other_message) mau_organic
from data_vajapora.mau_campaign_results 
group by 1 
order by 1; 