/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1377393918
- Data: 
- Function: 
- Table: data_vajapora.all_sch_stats
- Instructions: 
- Format: 
- File: 
- Path: http://localhost:8888/notebooks/Import%20from%20csv%20to%20DB/Live%20to%20DWH%20(truncate).ipynb
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Tables to have updated: 
	- public.notification_bulknotificationrequest
	- public.notification_bulknotificationsendrequest
	- public.notification_bulknotificationschedule
	- public.notification_pushmessage
	- public.notification_popupmessage
	- public.register_tag
	
	Use script in 'Path' to update. 
	
	Match counts in live and DWH: 
	select count(*) from public.notification_bulknotificationrequest; 
	select count(*) from public.notification_bulknotificationsendrequest; 
	select count(*) from public.notification_bulknotificationschedule; 
	select count(*) from public.notification_pushmessage; 
	select count(*) from public.notification_popupmessage; 
	select count(*) from public.register_tag;
	
	Investigate for a single case:
	select * from public.notification_bulknotificationrequest where id=12086; 					-- create
	select * from public.notification_bulknotificationsendrequest where request_id=12086; 		-- schedule
	select * from public.notification_bulknotificationschedule where request_id=12086; 			-- periodic schedule
	
	Calendar (for personalized messages): https://docs.google.com/spreadsheets/d/17rdRfZkXLqLhwgnXagi7Wq5oBdD5kBV2JcyvbDZfsSY/edit#gid=1750445076 
	
	Portal: https://web.tallykhata.com/notification/message/bulk/10723/send-request
*/

-- all schedules
do $$

declare 
	var_date date:=current_date-30; 
begin  
	raise notice 'New OP goes below:'; 

	truncate table data_vajapora.all_sch_stats; 

	-- auto TG create+schedule+results
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		campaign_id, message_id, bulk_notification_id, created_at, created_by, 
		schedule_id, schedule_type, schedule_time, should_repeat, repeat_days, repeat_until, schedule_dates, 
		result_date, intended_receiver_count, total_success, 
		tag_id, tag_name, tag_description, 
		message, message_type,
		coalesce(tbl3.status, tbl1.status) status
	from   
		(select 
			title campaign_id, message_id, id bulk_notification_id, created_at, created_by, receiving_tag_id tag_id, 
			case 
				when status=1 then 'processing'
				when status=2 then 'ready' 
			end status
		from public.notification_bulknotificationrequest
		) tbl1
	
		inner join 
			
		(select 
			request_id bulk_notification_id, id schedule_id, schedule_type, schedule_time, 
			should_repeat, repeat_days, repeat_until,
			(select string_agg(series_dates::text, ', ') limited_dates 
			from 
				(select generate_series(0, repeat_until::date-created_at::date, 1)+created_at::date series_dates
				) tbl1
			where right(series_dates::text, 2) in 
				(select case when length(specified_dates)=1 then concat('0', specified_dates) else specified_dates end specified_dates 
				from 
					(select unnest(string_to_array(trim(translate(repeat_days, '[]', '  ')), ', ')) specified_dates
					) tbl1
				)
			) schedule_dates
		from public.notification_bulknotificationschedule 
		) tbl2 using(bulk_notification_id)
		
		left join 
			    
		(select 
			request_id bulk_notification_id, 
			coalesce(schedule_time::date, updated_at::date) result_date, 
			receiver_count intended_receiver_count, 
			total_success, 
			case 
				when status=1 then 'scheduled'
				when status=2 then 'processing' 
				when status=3 then 'complete' 
				when status=4 then 'canceled' 
				when status=5 then 'in progress' 
			end status
		from public.notification_bulknotificationsendrequest
		) tbl3 using(bulk_notification_id)
		
		left join 
		
		(select 
			id tag_id, 
			tag_name, 
			case 
				when tag_description is not null then tag_description
				when tag_name='NBAll' then 'NB0_NN1_NN2-6'
				when tag_name='LTUAll' then 'LTUCb_LTUTa'
				when tag_name='3RAUAll' then '3RAUCb_3RAU Set-A_3RAU Set-B_3RAU Set-C_3RAUTa_3RAUTa+Cb_3RAUTacs'
				when tag_name='PUAll' then 'PUCb_PU Set-A_PU Set-B_PU Set-C_PUTa_PUTa+Cb_PUTacs'
				when tag_name='ZAll' then 'ZCb_ZTa_ZTa+Cb'
				when tag_name in(select distinct tg from cjm_segmentation.retained_users where report_date=current_date-1) then tag_name 
				else null
			end tag_description
		from public.register_tag
		) tbl4 using(tag_id)
	
		inner join 
		
		(select 
			id message_id, 
			case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 
			"type" message_type
		from public.notification_pushmessage
		) tbl5 using(message_id);

	loop
		/*delete from data_vajapora.all_sch_stats
		where schedule_date=var_date;*/ 
	
		insert into data_vajapora.all_sch_stats
		
		-- auto TG campaign status on a single day (result_date=var_date or result_date is null)
		select 
			var_date schedule_date, schedule_time, schedule_id, schedule_type, 
			created_at, created_by,
			campaign_id, bulk_notification_id,
			(case 
				when intended_receiver_count is not null then intended_receiver_count
				when tag_description is null then null
				else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=created_at::date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
			end 
			) intended_receiver_count, total_success, 
			message_id, message, message_type,
			tag_id, tag_name, tag_description,
			status
		from data_vajapora.help_a 
		where 
			schedule_dates like concat('%', var_date::text, '%')
			and (result_date=var_date or result_date is null)
			and campaign_id like '%-%' and campaign_id not ilike '%test%'
			
		union all 
		
		-- auto TG campaign status on a single day (result_date<var_date)
		select 
			var_date schedule_date, schedule_time, schedule_id, schedule_type, 
			created_at, created_by,
			campaign_id, bulk_notification_id,
			(case 
				when intended_receiver_count is not null then intended_receiver_count
				when tag_description is null then null
				else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=created_at::date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
			end 
			) intended_receiver_count, null total_success, 
			message_id, message, message_type,
			tag_id, tag_name, tag_description,
			'scheduled' status
		from 
			data_vajapora.help_a tbl1 
			
			inner join 
		
			(select schedule_id, max(result_date) result_date
			from data_vajapora.help_a
			group by 1
			) tbl2 using(schedule_id, result_date)
		where 
			schedule_dates like concat('%', var_date::text, '%')
			and result_date<var_date
			and campaign_id like '%-%' and campaign_id not ilike '%test%'
		
		union all
		
		-- bulk TG campaign status on a single day
		select 
			var_date schedule_date, schedule_time, null schedule_id, null schedule_type, 
			created_at, created_by,
			campaign_id, bulk_notification_id,
			(case 
				when intended_receiver_count is not null then intended_receiver_count
				when tag_description is null then null
				else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=created_at::date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
			end 
			) intended_receiver_count, total_success, 
			message_id, message, message_type,
			tag_id, tag_name, tag_description,
			coalesce(tbl2.status, tbl1.status) status
		from   
			(select 
				title campaign_id, message_id, id bulk_notification_id, created_at, created_by, receiving_tag_id tag_id, 
				case 
					when status=1 then 'processing'
					when status=2 then 'ready' 
				end status
			from public.notification_bulknotificationrequest
			) tbl1
			
			inner join 
				    
			(select 
				request_id bulk_notification_id, 
				coalesce(schedule_time::date, updated_at::date) schedule_date,
				coalesce(schedule_time::time, updated_at::time) schedule_time,
				receiver_count intended_receiver_count, 
				total_success, 
				case 
					when status=1 then 'scheduled'
					when status=2 then 'processing' 
					when status=3 then 'complete' 
					when status=4 then 'canceled' 
					when status=5 then 'in progress' 
				end status
			from public.notification_bulknotificationsendrequest
			) tbl2 using(bulk_notification_id)
			
			left join 
			
			(select 
				id tag_id, 
				tag_name, 
				case 
					when tag_description is not null then tag_description
					when tag_name='NBAll' then 'NB0_NN1_NN2-6'
					when tag_name='LTUAll' then 'LTUCb_LTUTa'
					when tag_name='3RAUAll' then '3RAUCb_3RAU Set-A_3RAU Set-B_3RAU Set-C_3RAUTa_3RAUTa+Cb_3RAUTacs'
					when tag_name='PUAll' then 'PUCb_PU Set-A_PU Set-B_PU Set-C_PUTa_PUTa+Cb_PUTacs'
					when tag_name='ZAll' then 'ZCb_ZTa_ZTa+Cb'
					when tag_name in(select distinct tg from cjm_segmentation.retained_users where report_date=current_date-1) then tag_name 
					else null
				end tag_description
			from public.register_tag
			) tbl3 using(tag_id)
		
			inner join 
			
			(select 
				id message_id, 
				case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 
				"type" message_type
			from public.notification_pushmessage
			) tbl4 using(message_id)
			
			left join 
			
			(select distinct bulk_notification_id
			from data_vajapora.help_a 
			where schedule_dates like concat('%', var_date::text, '%')
			) tbl5 using(bulk_notification_id)
		where 
			tbl5.bulk_notification_id is null
			and schedule_date=var_date
			and campaign_id like '%-%' and campaign_id not ilike '%test%'; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date+90 then exit; 
		end if; 
	end loop; 
end $$; 

-- message receive, open, click
do $$

declare 
	var_date date:=(select max(event_date)-7 from data_vajapora.message_received_opened_stats); 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.message_received_opened_stats
		where event_date=var_date; 
	
		insert into data_vajapora.message_received_opened_stats
		select
			event_date, 
			bulk_notification_id, 
			count(distinct mobile_no) merchants_commited_event, 
			'received' event_commited
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name like '%_message_received'
		group by 1, 2
		
		union all 
		
		select
			event_date, 
			bulk_notification_id, 
			count(distinct mobile_no) merchants_commited_event, 
			'opened' event_commited
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name like '%_message_open'
		group by 1, 2
	
		union all
		
		select
			event_date, 
			bulk_notification_id, 
			count(distinct mobile_no) merchants_commited_event, 
			'action link tap' event_commited
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name in('in_app_message_link_tap', 'inbox_message_action', 'refer_button_pressed')
		group by 1, 2;
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

-- all stats
select 
	tbl1.*, 
	merchants_received_message, 
	merchants_opened_message, 
	open_through_inbox_merchants, 
	first_open_through_inbox_merchants, 
	merchants_tapped_link, 
	now() report_last_updated
from 
	(select schedule_date, schedule_time, schedule_id, schedule_type, created_at, created_by, campaign_id, bulk_notification_id, case when message_type='POPUP_MESSAGE' then (select id from public.notification_popupmessage where push_message_id=message_id) else message_id end notification_id, intended_receiver_count, total_success, message_id, message, message_type, tag_id, tag_name, tag_description, status
	from data_vajapora.all_sch_stats
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
	
	(select event_date schedule_date, bulk_notification_id, sum(merchants_commited_event) merchants_tapped_link
	from data_vajapora.message_received_opened_stats
	where event_commited='action link tap'	
	group by 1, 2
	) tbl5 using(schedule_date, bulk_notification_id)
	
	left join 
	
	(select 
		report_date schedule_date, 
		bulk_notification_id, 
		count(distinct mobile_no) open_through_inbox_merchants, 
		count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
	from tallykhata.mom_cjm_performance_detailed
	group by 1, 2
	) tbl4 using(schedule_date, bulk_notification_id)
order by 1, 2;

/*
-- failed yesterday 
select *
from data_vajapora.all_sch_stats
where 
	schedule_date=current_date-1
	and total_success=0
	and intended_receiver_count!=0;
*/