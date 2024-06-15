/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1ZEx9B77qvssgKUodKzcgtk8KSGxzYRE9flilnAcPIOQ/edit#gid=0
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

-- inapp
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select id, mobile_no, event_name, notification_id, bulk_notification_id, event_timestamp 
from tallykhata.tallykhata_sync_event_fact_final
where 
	(notification_id in(2634, 2601)
	or 
	bulk_notification_id in 
	(select request_id
	from 
		(select 
	        request_id,
	        schedule_time start_datetime, 
	        schedule_time+interval '24 hours' end_datetime,
	        date(schedule_time) start_date
	    from public.notification_bulknotificationsendrequest
	    ) tbl1 
	
	    inner join 
	
	    (select id request_id, title campaign_id
	    from public.notification_bulknotificationrequest
	    ) tbl2 using(request_id) 
	where
		campaign_id in     
	    ('PC211227-02',
		'PC220104-01',
		'PC220106-02',
		'PC220107-02',
		'PC220108-02',
		'PC220109-02')
	))
	and event_name like '%in_app%'; 

select * 
from data_vajapora.help_c; 

select id notification_id, title, summary
from public.notification_pushmessage
where id in	
	(select distinct notification_id
	from data_vajapora.help_c
	); 

select 
	notification_id, 
	count(distinct case when event_name='in_app_message_received' then mobile_no else null end) merchants_received_message, 
	count(distinct case when event_name='in_app_message_open' then mobile_no else null end) merchants_viewed_message, 
	count(case when event_name='in_app_message_open' then id else null end) message_views, 
	count(distinct case when event_name='in_app_message_close' then mobile_no else null end) merchants_closed_message, 
	count(distinct case when event_name='in_app_message_link_tap' then mobile_no else null end) merchants_acted_on_message
from data_vajapora.help_c
group by 1; 

select id notification_id, title, summary
from public.notification_pushmessage
where id in(2634, 2601); 

-- inbox 
-- sequenced events of DAUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select id, mobile_no, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
from tallykhata.tallykhata_sync_event_fact_final
where created_date>'2021-12-26' and created_date<'2022-01-10'; 
	
-- all push-open cases
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select tbl1.id, tbl1.notification_id, tbl1.mobile_no
from 
	data_vajapora.help_a tbl1
	inner join 
	data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
where 
	tbl1.event_name='inbox_message_open'
	and tbl2.event_name='app_opened'; 

-- necessary metrics
select
	notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end title, 
	open_through_inbox_cases, 
	open_through_inbox_merchants,
	taps_on_message, 
	merchants_opened_message,
	action_pressed, 
	merchants_acted_on_message
from 
	(select 
		notification_id, 
		count(case when event_name='inbox_message_open' then id else null end) taps_on_message,
		count(distinct case when event_name='inbox_message_open' then mobile_no else null end) merchants_opened_message, 
		count(case when event_name='inbox_message_action' then id else null end) action_pressed,
		count(distinct case when event_name='inbox_message_action' then mobile_no else null end) merchants_acted_on_message
	from data_vajapora.help_a
	where notification_id in(2605, 2635)
	group by 1
	) tbl1 
	
	inner join 
	
	(select
		notification_id,
		count(id) open_through_inbox_cases,
		count(distinct mobile_no) open_through_inbox_merchants
	from data_vajapora.help_b
	where notification_id in(2605, 2635)
	group by 1
	) tbl2 using(notification_id)
	
	left join 
	
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl3 using(notification_id); 

