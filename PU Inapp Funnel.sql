/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1791596767
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
	var_date date:='2022-01-23'::date; 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.pu_inapp_funnels
		where scheduled_date=var_date;
	
		-- bulk PU inapp campaigns scheduled on date
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select *
		from 
			(-- all scheduled
			select message_id, request_id bulk_notification_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count intended_receiver_count, total_success
			from public.notification_bulknotificationsendrequest
			) tbl1 
			
			inner join 
			
			(-- inapps
			select id message_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message 
			from public.notification_pushmessage
			where "type" ='POPUP_MESSAGE'
			) tbl2 using(message_id)
			
			inner join 
		
			(-- campaign IDs
			select id bulk_notification_id, title campaign_id
		    from public.notification_bulknotificationrequest
		    ) tbl3 using(bulk_notification_id)
		    
		    inner join 
		    
		    (-- targeting PUs
		    select "Campaign ID" campaign_id
			from data_vajapora.inapp_pu_campaign_ids
			where "TG" ilike '%pu%'
			) tbl4 using(campaign_id)
		where schedule_date=var_date; 
		
		-- bulk inapp campaigns' DB footprint
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select bulk_notification_id, event_name, mobile_no, id
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date>=var_date
			and event_name in('in_app_message_received', 'in_app_message_open')
			and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_b); 
		
		-- retained PUs on date
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select mobile_no 
		from cjm_segmentation.retained_users 
		where 
			report_date=var_date
			and tg ilike '%pu%'; 
		
		-- summary metrics
		insert into data_vajapora.pu_inapp_funnels
		select 
			var_date scheduled_date,
			coalesce(pu_inapp_campaign_count, 0) pu_inapp_campaign_count,
			coalesce(intended_inapp_send_events, 0) intended_inapp_send_events,
			coalesce(inapp_send_events_claimed_successful, 0) inapp_send_events_claimed_successful,
			coalesce(in_app_received_users, 0) in_app_received_users,
			coalesce(in_app_received_events, 0) in_app_received_events,
			coalesce(in_app_opened_users, 0) in_app_opened_users,
			coalesce(in_app_opened_events, 0) in_app_opened_events
		from 
			(select
				count(distinct campaign_id) pu_inapp_campaign_count,
				sum(intended_receiver_count) intended_inapp_send_events, 
				sum(total_success) inapp_send_events_claimed_successful
			from data_vajapora.help_b
			) tbl1,
			
			(select  
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) in_app_received_users, 
				count(case when event_name='in_app_message_received' then id else null end) in_app_received_events,
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) in_app_opened_users, 
				count(case when event_name='in_app_message_open' then id else null end) in_app_opened_events
			from 
				data_vajapora.help_a tbl1 
				inner join 
				data_vajapora.help_c tbl2 using(mobile_no)
			) tbl2; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.pu_inapp_funnels; 

-- 0 investigation
select
	intended_schedule_date,
	campaign_id,
	
	bulk_notification_id,
	
	schedule_date,
	intended_receiver_count,
	total_success,
	
	if_inapp, 
	message_id,
	message
from 
	(-- targeting PUs
	select "Campaign ID" campaign_id, "Starting Date"::date intended_schedule_date
	from data_vajapora.inapp_pu_campaign_ids
	where "TG" ilike '%pu%'
	) tbl1
	
	left join
	
	(-- campaign IDs
	select id bulk_notification_id, title campaign_id
	from public.notification_bulknotificationrequest
	) tbl2 using(campaign_id)
	
	left join
	
	(-- all scheduled
	select message_id, request_id bulk_notification_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count intended_receiver_count, total_success
	from public.notification_bulknotificationsendrequest
	) tbl3 using(bulk_notification_id)
	
	left join 
	
	(-- inapps
	select id message_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 1 if_inapp                     
	from public.notification_pushmessage
	where "type" ='POPUP_MESSAGE'
	) tbl4 using(message_id)
order by 1; 
