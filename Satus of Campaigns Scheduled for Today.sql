/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1397496602
- Data: 
- Function: tallykhata.fn_today_schedule_stats()
- Table: tallykhata.today_schedule_stats
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
	- public.register_tag
	
	Use script in 'Path' to update. 
	
	Match counts in live and DWH: 
	select count(*) from public.notification_bulknotificationrequest; 
	select count(*) from public.notification_bulknotificationsendrequest; 
	select count(*) from public.notification_bulknotificationschedule; 
	select count(*) from public.notification_pushmessage; 
	select count(*) from public.register_tag;
	
	Investigate for a single case:
	select * from public.notification_bulknotificationrequest where id=12511; 					-- create
	select * from public.notification_bulknotificationsendrequest where request_id=12511; 		-- schedule
	select * from public.notification_bulknotificationschedule where request_id=12511; 			-- periodic schedule
*/

-- created+scheduled
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select
	created_at::date create_date, 
	concat(schedule_date::text, ', ', schedule_dates) schedule_dates, coalesce(tbl3.schedule_time, tbl2.schedule_time) schedule_time, 
	campaign_id, created_by, bulk_notification_id, message_id,
	intended_receiver_count, total_success, 
	coalesce(tbl2.status, tbl1.status) status, 
	schedule_id, schedule_type, should_repeat, repeat_days, repeat_until,
	message, msg_type, 
	tag_id , tag_name, tag_description
from   
	(select 
		title campaign_id, message_id, id bulk_notification_id, created_at, created_by, receiving_tag_id tag_id, 
		case 
			when status=1 then 'processing'
			when status=2 then 'ready' 
		end status
	from public.notification_bulknotificationrequest
	) tbl1
	
	left join 
	    
	(select 
		request_id bulk_notification_id, 
		coalesce(schedule_time::time, updated_at::time) schedule_time, 
		coalesce(schedule_time::date, updated_at::date) schedule_date, 
		receiver_count intended_receiver_count, total_success, 
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
	) tbl5 using(tag_id)

	inner join 
	
	(select 
		id message_id, 
		case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 
		"type" msg_type
	from public.notification_pushmessage
	) tbl4 using(message_id);
	
-- status of today's schedules
select 
	current_date schedule_date, schedule_time, schedule_id, schedule_type, 
	create_date, created_by,
	campaign_id, bulk_notification_id,
	(case 
		when intended_receiver_count is not null then intended_receiver_count
		when tag_description is null then null
		else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=create_date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
	end 
	) intended_receiver_count, total_success, 
	message_id, message, msg_type,
	tag_id, tag_name, tag_description,
	status
from data_vajapora.help_a 
where 
	schedule_dates like concat('%', current_date::text, '%')
	and campaign_id like '%-%'
	and campaign_id not ilike '%test%'; 

-- bring to a function
CREATE OR REPLACE FUNCTION tallykhata.fn_today_schedule_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Tables to have updated: 
	- public.notification_bulknotificationrequest
	- public.notification_bulknotificationsendrequest
	- public.notification_bulknotificationschedule
	- public.notification_pushmessage
	- public.register_tag
Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1397496602
*/

declare
	
begin
	-- created+scheduled
	drop table if exists tallykhata.today_schedule_stats_help; 
	create table tallykhata.today_schedule_stats_help as
	select
		created_at::date create_date, 
		concat(schedule_date::text, ', ', schedule_dates) schedule_dates, coalesce(tbl3.schedule_time, tbl2.schedule_time) schedule_time, 
		campaign_id, created_by, bulk_notification_id, message_id,
		intended_receiver_count, total_success, 
		coalesce(tbl2.status, tbl1.status) status, 
		schedule_id, schedule_type, should_repeat, repeat_days, repeat_until,
		message, msg_type, 
		tag_id , tag_name, tag_description
	from   
		(select 
			title campaign_id, message_id, id bulk_notification_id, created_at, created_by, receiving_tag_id tag_id, 
			case 
				when status=1 then 'processing'
				when status=2 then 'ready' 
			end status
		from public.notification_bulknotificationrequest
		) tbl1
		
		left join 
		    
		(select 
			request_id bulk_notification_id, 
			coalesce(schedule_time::time, updated_at::time) schedule_time, 
			coalesce(schedule_time::date, updated_at::date) schedule_date, 
			receiver_count intended_receiver_count, total_success, 
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
		) tbl5 using(tag_id)
	
		inner join 
		
		(select 
			id message_id, 
			case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message, 
			"type" msg_type
		from public.notification_pushmessage
		) tbl4 using(message_id);
		
	-- status of today's schedules
	drop table if exists tallykhata.today_schedule_stats; 
	create table tallykhata.today_schedule_stats as
	select 
		current_date schedule_date, schedule_time, schedule_id, schedule_type, 
		create_date, created_by,
		campaign_id, bulk_notification_id,
		(case 
			when intended_receiver_count is not null then intended_receiver_count
			when tag_description is null then null
			else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=create_date and tg in(select * from unnest(string_to_array(tag_description, '_'))))
		end 
		) intended_receiver_count, total_success, 
		message_id, message, msg_type,
		tag_id, tag_name, tag_description,
		status
	from tallykhata.today_schedule_stats_help 
	where 
		schedule_dates like concat('%', current_date::text, '%')
		and campaign_id like '%-%'
		and campaign_id not ilike '%test%'; 
	
	-- drop auxiliary table(s)
	drop table if exists tallykhata.today_schedule_stats_help; 

END;
$function$
;

select tallykhata.fn_today_schedule_stats(); 

select *
from tallykhata.today_schedule_stats; 

-- version-02

-- created+scheduled
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

	left join 
		
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

-- status of today's schedules
select 
	current_date schedule_date, schedule_time, schedule_id, schedule_type, 
	created_at, created_by,
	campaign_id, bulk_notification_id,
	(case 
		when intended_receiver_count is not null then intended_receiver_count
		when tag_description is null then null
		else (select count(mobile_no) from cjm_segmentation.retained_users where report_date=created_at and tg in(select * from unnest(string_to_array(tag_description, '_'))))
	end 
	) intended_receiver_count, total_success, 
	message_id, message, message_type,
	tag_id, tag_name, tag_description,
	status
from data_vajapora.help_a 
where 
	schedule_dates like concat('%', current_date::text, '%')
	and (result_date=current_date or result_date is null)
	and campaign_id like '%-%'
	and campaign_id not ilike '%test%'; 
 






