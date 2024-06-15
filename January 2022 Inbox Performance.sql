/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=360481953
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=655363703
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- TG
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	notification_id, 
	max(tg_intended) tg_intended, 
	max(total_success) tg_claimed_successful, 
	count(distinct schedule_date) frequency
from 
	(select schedule_date, notification_id, sum(receiver_count) tg_intended, sum(total_success) total_success
	from 
		(select request_id, date(schedule_time) schedule_date, total_success, message_id notification_id, receiver_count       
		from public.notification_bulknotificationsendrequest
		where
			date(schedule_time)>='2022-01-01' and date(schedule_time)<'2022-02-01'
			and message_id is not null
		) tbl1 
	group by 1, 2
	) tbl1 
group by 1; 

-- receive
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	notification_id, 
	count(distinct mobile_no) inbox_message_received_merchants, 
	count(id) inbox_message_received_events
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_date>='2022-01-01' and event_date<'2022-02-01'
	and event_name in('inbox_message_received')
	and notification_id is not null
group by 1; 

-- open
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	notification_id, 
	mobile_no, 
	count(id) msg_open_events, 
	count(distinct event_date) msg_open_days
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_date>='2022-01-01' and event_date<'2022-02-01'
	and event_name in('inbox_message_open')
	and notification_id is not null
group by 1, 2; 

-- summary
select 
	notification_id, 
	case 
		when notification_id in(select "Testimonials Msg IDs" from data_vajapora.message_categories where "Testimonials Msg IDs" is not null) then 'testimonial'
		when notification_id in(select "Ad Hoc Msg IDs" from data_vajapora.message_categories where "Ad Hoc Msg IDs" is not null) then 'adhoc'
		when notification_id in(select "Impulse Msg IDs" from data_vajapora.message_categories where "Impulse Msg IDs" is not null) then 'impulse'
		when notification_id in(select "Personalized Msg IDs" from data_vajapora.message_categories where "Personalized Msg IDs" is not null) then 'personalized'
		when notification_id in(select "Regular Msg IDs" from data_vajapora.message_categories where "Regular Msg IDs" is not null) then 'cjm'
		else 'others'
	end msg_category, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end title, 
	
	frequency, 
	
	tg_intended, 
	tg_claimed_successful, 
	
	inbox_message_received_events, 
	inbox_message_received_merchants, 
	
	open_through_inbox_events, 
	open_through_inbox_merchants, 
	first_open_through_inbox_events,
	first_open_through_inbox_merchants, 
	 
	inbox_message_opened_events, 
	inbox_message_opened_merchants, 
	inbox_message_opened_merchants_1_to_5_days, 
	inbox_message_opened_merchants_6_to_10_days, 
	inbox_message_opened_merchants_11_to_20_days, 
	inbox_message_opened_merchants_more_than_20_days
from 
	data_vajapora.help_a tbl1 
	
	inner join 
	
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)

	inner join 
	
	data_vajapora.help_b tbl3 using(notification_id) 
	
	inner join 
	
	(select 
		notification_id, 
		count(mobile_no) inbox_message_opened_merchants, 
		count(case when msg_open_days>=1 and msg_open_days<=5 then mobile_no else null end) inbox_message_opened_merchants_1_to_5_days, 
		count(case when msg_open_days>=6 and msg_open_days<=10 then mobile_no else null end) inbox_message_opened_merchants_6_to_10_days, 
		count(case when msg_open_days>=11 and msg_open_days<=20 then mobile_no else null end) inbox_message_opened_merchants_11_to_20_days, 
		count(case when msg_open_days>20 then mobile_no else null end) inbox_message_opened_merchants_more_than_20_days, 
		sum(msg_open_events) inbox_message_opened_events
	from data_vajapora.help_d 
	group by 1
	) tbl4 using(notification_id)
	
	inner join 

	(select 
		notification_id,
		count(mobile_no) open_through_inbox_events, 
		count(distinct mobile_no) open_through_inbox_merchants, 
		count(case when id is not null then mobile_no else null end) first_open_through_inbox_events,
		count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
	from data_vajapora.mom_cjm_performance_detailed
	where left(report_date::text, 7)='2022-01'
	group by 1
	) tbl5 using(notification_id); 
