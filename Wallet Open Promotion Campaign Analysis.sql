/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1368511333
- Function: 
- Table:
- Instructions: 
- Format: https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit#gid=1091397744
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

/*Announcement (open wallet)*/

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when message_type='POPUP_MESSAGE' then (select id from public.notification_popupmessage where push_message_id=message_id)
		else message_id 
	end notification_id 
from data_vajapora.all_sch_stats
where 
	campaign_id in('ANN220922-01', 'ANN220922-02', 'ANN220922-24-01', 'ANN220922-24-02', 'ANN220922-24-03', 'ANN220922-24-04', 'REG220922-01', 'AMC220922-01', 'REG220923-25-01', 'REG220923-25-02', 'REG220923-25-03', 'BAP220923-01', 'AMC220923-01', 'BAP220924-01', 'AMC220924-01', 'BAP220925-01', 'AMC220925-01', 'ANN220926-30-01', 'ANN220926-30-02', 'ANN220926-30-03', 'ANN220926-30-04', 'REG220926-01', 'REG220926-02', 'BAP220926-01', 'AMC220926-01') 
	and campaign_id like 'ANN%'; 

-- events of interest
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	id, mobile_no, 
	event_date, event_timestamp, event_name,
	bulk_notification_id, notification_id
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date-1
	and (notification_id, bulk_notification_id, event_date) in(select notification_id, bulk_notification_id, schedule_date from data_vajapora.help_a) 
	
union all 
	
select 
	id, mobile_no, 
	date(created_at) event_date, created_at event_timestamp, 'Announcement (open wallet)' event_name, 
	null bulk_notification_id, null notification_id
from 
	public.wallet_logininfo tbl1 
	inner join 
	(select mobile_number mobile_no, tallykhata_user_id 
	from public.register_usermobile
	) tbl2 using(tallykhata_user_id); 

-- metrics of interest
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	event_date schedule_date, bulk_notification_id, notification_id, 
	count(tbl1.mobile_no) message_received, 
	count(tbl2.mobile_no) message_opened, 
	count(tbl3.mobile_no) opened_app_through_inbox, 
	count(tbl4.mobile_no) first_opened_app_through_inbox, 
	count(tbl6.mobile_no) clicked_link, 
	count(tbl5.mobile_no) took_action, 
	count(case when wallet_open_time>first_msg_open then tbl5.mobile_no else null end) took_action_after_seeing_msg
from 
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b
	where event_name like '%message_received'
	) tbl1
	
	left join
	
	(select event_date, bulk_notification_id, notification_id, mobile_no, min(event_timestamp) first_msg_open 
	from data_vajapora.help_b
	where event_name like '%message_open'
	group by 1, 2, 3, 4
	) tbl2 using(event_date, bulk_notification_id, notification_id, mobile_no)
	
	left join 
	
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b 
	where event_name in('in_app_message_link_tap', 'inbox_message_action')
	) tbl6 using(event_date, bulk_notification_id, notification_id, mobile_no)

	left join 
	
	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	) tbl3 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 

	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	where id is not null
	) tbl4 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 
	
	(select mobile_no, min(event_date) event_date, min(event_timestamp) wallet_open_time 
	from data_vajapora.help_b 
	where event_name='Announcement (open wallet)'
	group by 1
	) tbl5 using(mobile_no, event_date)
group by 1, 2, 3; 

-- in desired format
select 
	message_type, 
	tag_name, 
	tag_id, 
	campaign_id, 
	notification_id, 
	schedule_date event_date, 
	'Announcement (open wallet)' event_name, 
	message, 
	intended_receiver_count, 
	message_received, message_opened, opened_app_through_inbox, first_opened_app_through_inbox, clicked_link, took_action, took_action_after_seeing_msg   
from 
	data_vajapora.help_d tbl1 
	
	inner join 
	
	(select message_type, tag_name, tag_id, campaign_id, bulk_notification_id, notification_id, schedule_date, message, max(intended_receiver_count) intended_receiver_count 
	from data_vajapora.help_a 
	group by 1, 2, 3, 4, 5, 6, 7, 8
	) tbl2 using(schedule_date, bulk_notification_id, notification_id)
order by bulk_notification_id;

/*Baki aday through payment link*/

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when message_type='POPUP_MESSAGE' then (select id from public.notification_popupmessage where push_message_id=message_id)
		else message_id 
	end notification_id 
from data_vajapora.all_sch_stats
where 
	campaign_id in('ANN220922-01', 'ANN220922-02', 'ANN220922-24-01', 'ANN220922-24-02', 'ANN220922-24-03', 'ANN220922-24-04', 'REG220922-01', 'AMC220922-01', 'REG220923-25-01', 'REG220923-25-02', 'REG220923-25-03', 'BAP220923-01', 'AMC220923-01', 'BAP220924-01', 'AMC220924-01', 'BAP220925-01', 'AMC220925-01', 'ANN220926-30-01', 'ANN220926-30-02', 'ANN220926-30-03', 'ANN220926-30-04', 'REG220926-01', 'REG220926-02', 'BAP220926-01', 'AMC220926-01') 
	and campaign_id like 'BAP%'; 

-- events of interest
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	id, mobile_no, 
	event_date, event_timestamp, event_name,
	bulk_notification_id, notification_id
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date-1
	and (notification_id, bulk_notification_id, event_date) in(select notification_id, bulk_notification_id, schedule_date from data_vajapora.help_a) 
	
union all 
	
select 
	id, mobile_no, 
	date(created_at) event_date, created_at event_timestamp, 'Baki aday through payment link' event_name, 
	null bulk_notification_id, null notification_id
from 
	public.payment_creditcollection tbl1 
	inner join 
	(select mobile_number mobile_no, tallykhata_user_id 
	from public.register_usermobile
	) tbl2 using(tallykhata_user_id)
where status=7; 

-- metrics of interest
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	event_date schedule_date, bulk_notification_id, notification_id, 
	count(tbl1.mobile_no) message_received, 
	count(tbl2.mobile_no) message_opened, 
	count(tbl3.mobile_no) opened_app_through_inbox, 
	count(tbl4.mobile_no) first_opened_app_through_inbox, 
	count(tbl6.mobile_no) clicked_link, 
	count(tbl5.mobile_no) took_action, 
	count(case when baki_aday_time>first_msg_open then tbl5.mobile_no else null end) took_action_after_seeing_msg
from 
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b
	where event_name like '%message_received'
	) tbl1
	
	left join
	
	(select event_date, bulk_notification_id, notification_id, mobile_no, min(event_timestamp) first_msg_open 
	from data_vajapora.help_b
	where event_name like '%message_open'
	group by 1, 2, 3, 4
	) tbl2 using(event_date, bulk_notification_id, notification_id, mobile_no)
	
	left join 
	
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b 
	where event_name in('in_app_message_link_tap', 'inbox_message_action')
	) tbl6 using(event_date, bulk_notification_id, notification_id, mobile_no)

	left join 
	
	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	) tbl3 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 

	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	where id is not null
	) tbl4 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 
	
	(select event_date, mobile_no, max(event_timestamp) baki_aday_time 
	from data_vajapora.help_b 
	where event_name='Baki aday through payment link'
	group by 1, 2
	) tbl5 using(mobile_no, event_date)
group by 1, 2, 3; 

-- in desired format
select 
	message_type, 
	tag_name, 
	tag_id, 
	campaign_id, 
	notification_id, 
	schedule_date event_date, 
	'Baki aday through payment link' event_name, 
	message, 
	intended_receiver_count, 
	message_received, message_opened, opened_app_through_inbox, first_opened_app_through_inbox, clicked_link, took_action, took_action_after_seeing_msg   
from 
	data_vajapora.help_d tbl1 
	
	inner join 
	
	(select message_type, tag_name, tag_id, campaign_id, bulk_notification_id, notification_id, schedule_date, message, max(intended_receiver_count) intended_receiver_count 
	from data_vajapora.help_a 
	group by 1, 2, 3, 4, 5, 6, 7, 8
	) tbl2 using(schedule_date, bulk_notification_id, notification_id)
order by bulk_notification_id;

/*Add money from card*/

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when message_type='POPUP_MESSAGE' then (select id from public.notification_popupmessage where push_message_id=message_id)
		else message_id 
	end notification_id 
from data_vajapora.all_sch_stats
where 
	campaign_id in('ANN220922-01', 'ANN220922-02', 'ANN220922-24-01', 'ANN220922-24-02', 'ANN220922-24-03', 'ANN220922-24-04', 'REG220922-01', 'AMC220922-01', 'REG220923-25-01', 'REG220923-25-02', 'REG220923-25-03', 'BAP220923-01', 'AMC220923-01', 'BAP220924-01', 'AMC220924-01', 'BAP220925-01', 'AMC220925-01', 'ANN220926-30-01', 'ANN220926-30-02', 'ANN220926-30-03', 'ANN220926-30-04', 'REG220926-01', 'REG220926-02', 'BAP220926-01', 'AMC220926-01') 
	and campaign_id like 'AMC%'; 

-- events of interest
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	id, mobile_no, 
	event_date, event_timestamp, event_name,
	bulk_notification_id, notification_id
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date-1
	and (notification_id, bulk_notification_id, event_date) in(select notification_id, bulk_notification_id, schedule_date from data_vajapora.help_a) 
	
union all 
	
/*
-- from Nobopay DWH
select 
	id, mobile_no, 
	date(txn_time) event_date, txn_time event_timestamp, 'Add money from card' event_name, 
	null bulk_notification_id, null notification_id
from 
	backend_db.np_txn_log tbl1 
	
	inner join 
	
	(select user_id to_id, wallet_no mobile_no
	from backend_db.profile
	) tbl2 using(to_id)
where 
	txn_type='CASH_IN_FROM_CARD'
	and status='COMPLETE';
*/

select id, concat('0', mobile_no::text) mobile_no, event_date::date, event_timestamp::timestamp, event_name, bulk_notification_id, notification_id
from data_vajapora.nobopay_data; 

-- metrics of interest
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select 
	event_date schedule_date, bulk_notification_id, notification_id, 
	count(tbl1.mobile_no) message_received, 
	count(tbl2.mobile_no) message_opened, 
	count(tbl3.mobile_no) opened_app_through_inbox, 
	count(tbl4.mobile_no) first_opened_app_through_inbox, 
	count(tbl6.mobile_no) clicked_link, 
	count(tbl5.mobile_no) took_action, 
	count(case when add_money_time>first_msg_open then tbl5.mobile_no else null end) took_action_after_seeing_msg
from 
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b
	where event_name like '%message_received'
	) tbl1
	
	left join
	
	(select event_date, bulk_notification_id, notification_id, mobile_no, min(event_timestamp) first_msg_open 
	from data_vajapora.help_b
	where event_name like '%message_open'
	group by 1, 2, 3, 4
	) tbl2 using(event_date, bulk_notification_id, notification_id, mobile_no)
	
	left join 
	
	(select distinct event_date, bulk_notification_id, notification_id, mobile_no
	from data_vajapora.help_b 
	where event_name in('in_app_message_link_tap', 'inbox_message_action')
	) tbl6 using(event_date, bulk_notification_id, notification_id, mobile_no)

	left join 
	
	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	) tbl3 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 

	(select distinct report_date event_date, bulk_notification_id, notification_id, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	where id is not null
	) tbl4 using(event_date, bulk_notification_id, notification_id, mobile_no) 
	
	left join 
	
	(select event_date, mobile_no, max(event_timestamp) add_money_time 
	from data_vajapora.help_b 
	where event_name='Add money from card'
	group by 1, 2
	) tbl5 using(mobile_no, event_date)
group by 1, 2, 3; 

-- in desired format
select 
	message_type, 
	tag_name, 
	tag_id, 
	campaign_id, 
	notification_id, 
	schedule_date event_date, 
	'Add money from card' event_name, 
	message, 
	intended_receiver_count, 
	message_received, message_opened, opened_app_through_inbox, first_opened_app_through_inbox, clicked_link, took_action, took_action_after_seeing_msg   
from 
	data_vajapora.help_d tbl1 
	
	inner join 
	
	(select message_type, tag_name, tag_id, campaign_id, bulk_notification_id, notification_id, schedule_date, message, max(intended_receiver_count) intended_receiver_count 
	from data_vajapora.help_a 
	group by 1, 2, 3, 4, 5, 6, 7, 8
	) tbl2 using(schedule_date, bulk_notification_id, notification_id)
order by bulk_notification_id;
