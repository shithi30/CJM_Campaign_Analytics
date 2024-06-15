/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1403410654
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=859112593
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
	We are doing New Natal campaign through force video from 2 March. Could you please give us some insights of this campaign? 
	Here are the messages IDs for your reference:
	2786
	2787
*/

do $$

declare 
	var_date date:='2022-02-22'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.forced_video_daily_analysis
		where schedule_date=var_date;
	
		insert into data_vajapora.forced_video_daily_analysis
		select 
			schedule_date,
			bulk_notification_id,
			message_id,
			message,
			coalesce(intended_receiver_count, 0) intended_tg,
			total_success claimed_success, 
			coalesce(in_app_received_users, 0) in_app_received_users,
			coalesce(video_started_users, 0) video_started_users,
			coalesce(video_skipped_users, 0) video_skipped_users, 
			coalesce(video_ended_users, 0) video_ended_users
		from 
			(-- bulk inapp campaigns scheduled on date
			select *
			from 
				(select message_id, request_id bulk_notification_id, coalesce(date(schedule_time), date(updated_at)) schedule_date, receiver_count intended_receiver_count, total_success
				from public.notification_bulknotificationsendrequest
				) tbl1 
				
				inner join 
				
				(select id message_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message 
				from public.notification_pushmessage
				where "type" ='POPUP_MESSAGE'
				) tbl2 using(message_id)
			where 
				message_id in(2786, 2787) -- intended forced video message IDs
				and schedule_date=var_date
			) tbl1 
			
			left join 
						
			(-- bulk inapp campaigns' DB footprint 
			select 
				bulk_notification_id, 
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) in_app_received_users, 
				count(distinct case when event_name='forced_video_message_playback_start' then mobile_no else null end) video_started_users, 
				count(distinct case when event_name='forced_video_message_skip_button_pressed' then mobile_no else null end) video_skipped_users, 
				count(distinct case when event_name='forced_video_message_video_ended' then mobile_no else null end) video_ended_users
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date>=var_date
				and event_name in('in_app_message_received', 'forced_video_message_video_ended', 'forced_video_message_skip_button_pressed', 'forced_video_message_playback_start')
			group by 1
			) tbl2 using(bulk_notification_id); 
				
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.forced_video_daily_analysis; 
