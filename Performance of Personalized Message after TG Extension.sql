/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=810202930
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

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from 
	(select schedule_date, bulk_notification_id, message_id, message, intended_receiver_count, total_success firebase_success 
	from data_vajapora.all_sch_stats
	where message_id in(2864, 2851, 3114, 3115, 2852, 2854, 2855, 2856, 2857, 3116, 2858, 2859, 3117, 2788, 3113) -- given 
	) tbl1
	
	left join 
	
	(select event_date schedule_date, bulk_notification_id, sum(merchants_commited_event) merchants_received_message
	from data_vajapora.message_received_opened_stats
	where event_commited='received'	
	group by 1, 2
	) tbl2 using(schedule_date, bulk_notification_id)
	
	left join 
	
	(select event_date schedule_date, bulk_notification_id, sum(merchants_commited_event) merchants_opened_message
	from data_vajapora.message_received_opened_stats
	where event_commited='opened'	
	group by 1, 2
	) tbl3 using(schedule_date, bulk_notification_id)
	
	left join 
	
	(select 
		report_date schedule_date, 
		bulk_notification_id, 
		count(distinct tbl1.mobile_no) open_through_inbox_merchants, 
		count(distinct case when id is not null then tbl1.mobile_no else null end) first_open_through_inbox_merchants, 
		count(distinct case when id is not null and tbl2.mobile_no is not null then tbl1.mobile_no else null end) first_open_through_inbox_merchants_txn
	from 
		data_vajapora.mom_cjm_performance_detailed tbl1 
		left join 
		(select created_datetime report_date, mobile_no 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		where created_datetime>=current_date-30
		) tbl2 using(report_date, mobile_no)
	group by 1, 2
	) tbl4 using(schedule_date, bulk_notification_id)
order by 1;

select 
	schedule_date,	
	message_id,
	message,	
	sum(intended_receiver_count) intended_receiver_count, 
	sum(firebase_success) firebase_success,
	sum(merchants_received_message) merchants_received_message,	
	sum(merchants_opened_message) merchants_opened_message,	
	sum(open_through_inbox_merchants) open_through_inbox_merchants,	
	sum(first_open_through_inbox_merchants) first_open_through_inbox_merchants,
	sum(first_open_through_inbox_merchants_txn) first_open_through_inbox_merchants_txn
from data_vajapora.help_a
where schedule_date<current_date
group by 1, 2, 3
order by 2, 1; 
