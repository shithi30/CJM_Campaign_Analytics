/*
- Viz: 
	- my analysis for Inapps: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=238771523
	- Khalid Bhai's analysis for all: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2028887975
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
	var_date date:='2022-02-01'::date; 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.inapp_stats_1
		where report_date=var_date;
	
		drop table if exists data_vajapora.temp_a; 
		create table data_vajapora.temp_a as
		select id, mobile_no, event_name, notification_id
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name in('app_opened', 'in_app_message_received', 'in_app_message_open'); 
			
		insert into data_vajapora.inapp_stats_1
		select 
			var_date report_date, 
			intended_inapp_tg, 
			inapp_tg_claimed_successful,
			intended_inapp_send_events, 
			inapp_send_events_claimed_successful, 
			inapp_received_merchants,
			inapp_received_events,
			app_open_through_inapp_merchants,
			app_open_through_inapp_events,
			inapp_opened_merchants,
			inapp_opened_events
		from 
			(select 
				sum(intended_receiver_count) intended_inapp_send_events, 
				sum(claimed_total_success) inapp_send_events_claimed_successful, 
				max(intended_receiver_count) intended_inapp_tg, 
				max(claimed_total_success) inapp_tg_claimed_successful
			from 
				(select 
					message_id, 
					sum(receiver_count) intended_receiver_count, 
					sum(total_success) claimed_total_success
				from 
					(select message_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count, total_success    
					from public.notification_bulknotificationsendrequest
					) tbl1 
					
					inner join 
					
					(select id message_id
					from public.notification_pushmessage
					where "type" ='POPUP_MESSAGE'
					) tbl2 using(message_id)
				where 
					schedule_date=var_date
					and message_id is not null
				group by 1 
				) tbl1
			) tbl0, 
		
			(select count(distinct tbl1.mobile_no) app_open_through_inapp_merchants, count(tbl1.id) app_open_through_inapp_events
			from 
				data_vajapora.temp_a tbl1
				inner join 
				data_vajapora.temp_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.id=tbl2.id-1) 
			where 
				tbl1.event_name='in_app_message_received' 
				and tbl2.event_name in('app_opened', 'app_launched')
			) tbl1, 
				
			(select count(distinct mobile_no) inapp_received_merchants, count(id) inapp_received_events 
			from data_vajapora.temp_a
			where event_name='in_app_message_received'
			) tbl2,
			
			(select count(distinct mobile_no) inapp_opened_merchants, count(id) inapp_opened_events 
			from data_vajapora.temp_a
			where event_name='in_app_message_open'
			) tbl3; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.inapp_stats_1
order by 1; 
