/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=471362259
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
	
	Set-A
	Campaign ID: FV220511-01
	Message ID: 3088
	
	Set-B
	Campaign ID: FV220511-02
	Message ID: 3090
	
	Start: 2022-05-11
*/

do $$ 

declare 
	var_campaign_id text:='FV220511-02';	-- change
	var_date date:='2022-05-11'::date;		-- start
begin 
	-- watched video
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as 
	select mobile_no
	from tallykhata.tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date>=var_date
		and event_name='forced_video_message_playback_start'
		and bulk_notification_id in 
			(select distinct bulk_notification_id
			from data_vajapora.all_sch_stats
			where campaign_id in(var_campaign_id)
			); 
		
	-- solicited statistics
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as 
	select * 
	from 
		(select distinct campaign_id, bulk_notification_id, message_id, message 
		from data_vajapora.all_sch_stats
		where campaign_id in(var_campaign_id)
		) tbl0, 
		
		(select 
			sum(intended_receiver_count) tg_size, 
			sum(total_success) fb_success
		from data_vajapora.all_sch_stats 
		where 
			campaign_id in(var_campaign_id)
			and schedule_date<current_date
		) tbl1, 
		
		(select 
			count(distinct case when event_name='in_app_message_received' then mobile_no else null end) received_video_inapp, 
			count(distinct case when event_name='forced_video_message_playback_start' then mobile_no else null end) watched_forced_video 
		from tallykhata.tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date>=var_date
			and bulk_notification_id in 
				(select distinct bulk_notification_id
				from data_vajapora.all_sch_stats
				where campaign_id in(var_campaign_id)
				)
		) tbl2, 
			
		(select 
			avg(date_part('hour', next_event_timestamp-event_timestamp)*3600
			+date_part('minute', next_event_timestamp-event_timestamp)*60
			+date_part('second', next_event_timestamp-event_timestamp)
			) avg_sec_watched_video
		from 
			(select 
				mobile_no, 
				event_name, event_timestamp, 
				lead(event_name, 1) over(partition by mobile_no order by event_timestamp asc) next_event, lead(event_timestamp, 1) over(partition by mobile_no order by event_timestamp asc) next_event_timestamp                                                              
			from tallykhata.tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date>=var_date
				and bulk_notification_id in 
					(select distinct bulk_notification_id
					from data_vajapora.all_sch_stats
					where campaign_id in(var_campaign_id)
					)
			) tbl1 
		where 
			event_name='forced_video_message_playback_start' 
			and next_event in('forced_video_message_skip_button_pressed', 'forced_video_message_video_ended')
			and 
				-- ignoring buffer delay
				date_part('hour', next_event_timestamp-event_timestamp)*3600
				+date_part('minute', next_event_timestamp-event_timestamp)*60
				+date_part('second', next_event_timestamp-event_timestamp)<=600
		) tbl3, 
		
		(select count(distinct mobile_no) added_customer 
		from 
			(select mobile_no 
			from tallykhata.tallykhata_fact_info_final 
			where 
				created_datetime>=var_date
				and txn_type in('Add Customer')
			) tbl1 
			
			inner join 
				
			data_vajapora.help_a tbl2 using(mobile_no)
		) tbl7; 
end $$; 

select * 
from data_vajapora.help_b; 
