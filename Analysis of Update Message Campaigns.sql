/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=644642686
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Request for data of App version 4.0.0 & 4.0.1 users!
- Notes (if any): 
*/

-- Version-01: inbox
do $$

declare 
	var_date date:='2022-01-03'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.update_message_analysis
		where report_date=var_date; 
	
		-- TG
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select 
			mobile_no, 
			case 
				when tg ilike 'pu%' then 'PU'
				when tg ilike '3rau%'  then '3RAU'
				when tg ilike 'ltu%' then 'LTU'
				when tg ilike 'z%' then 'Zombie'
				when tg ilike 'psu%' then 'PSU'
				when tg in('NN1', 'NN2-6', 'NB0') then 'NN'
			else 'NT' end tg_shrunk 
		from cjm_segmentation.retained_users 
		where report_date=var_date; 
		
		-- sequenced events of DAUs
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select id, mobile_no, event_name, notification_id, event_timestamp, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from tallykhata.tallykhata_sync_event_fact_final
		where created_date=var_date; 
			
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.notification_id, tbl1.mobile_no, tbl3.id
		from 
			data_vajapora.help_a tbl1
			
			inner join 
			
			data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			
			left join 
			
			(select mobile_no, min(id) id 
			from data_vajapora.help_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
			
		-- necessary statistics
		insert into data_vajapora.update_message_analysis
		select 
			var_date report_date, 
			tg, 
			notification_id, 
			merchants_received_message, 
			merchants_opened_message, 
			taps_on_message, 
			coalesce(open_through_inbox_merchants, 0) open_through_inbox_merchants, 
			coalesce(first_open_through_inbox_merchants, 0) first_open_through_inbox_merchants, 
			merchants_acted_on_message,
			merchants_acted_on_message_and_updated
		from 
			(select 
				coalesce(tg_shrunk, 'rest') tg, 
				notification_id, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) merchants_received_message, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) merchants_opened_message, 
				count(distinct case when event_name='inbox_message_action' then mobile_no else null end) merchants_acted_on_message, 
				count(distinct case when event_name='inbox_message_action' and app_version_name='4.0.2' and update_or_reg_datetime>=event_timestamp then mobile_no else null end) merchants_acted_on_message_and_updated, 
				count(case when event_name='inbox_message_open' then id else null end) taps_on_message 
			from 
				data_vajapora.help_a tbl1
				
				left join 
				
				data_vajapora.help_c tbl2 using(mobile_no)
				
				left join 
				
				(select mobile_no, app_version_name, update_or_reg_datetime
				from data_vajapora.version_wise_days
				where 
					date(update_or_reg_datetime)=var_date
					and update_or_reg_datetime!=reg_datetime
				) tbl3 using(mobile_no)
			group by 1, 2
			) tbl1 
			
			left join 
			
			(select
				coalesce(tg_shrunk, 'rest') tg,
				notification_id, 
				count(distinct mobile_no) open_through_inbox_merchants,
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
			from 
				data_vajapora.help_b
				
				left join 
				
				data_vajapora.help_c using(mobile_no)
			group by 1, 2
			) tbl2 using(notification_id, tg); 
			
		raise notice 'Data generated for: %', var_date;
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	notification_id, 
	tg, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message,
	merchants_received_message, 
	merchants_opened_message, 
	taps_on_message, 
	open_through_inbox_merchants, 
	first_open_through_inbox_merchants, 
	merchants_acted_on_message,  
	merchants_acted_on_message_and_updated
from 
	data_vajapora.update_message_analysis tbl1 
	inner join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where notification_id in(2624)
order by report_date asc, merchants_received_message desc;

-- Version-02: inapp
do $$

declare 
	var_date date:='2022-01-03'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.update_message_analysis_2
		where report_date=var_date; 
		
		-- TG
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select 
			mobile_no, 
			case 
				when tg ilike 'pu%' then 'PU'
				when tg ilike '3rau%'  then '3RAU'
				when tg ilike 'ltu%' then 'LTU'
				when tg ilike 'z%' then 'Zombie'
				when tg ilike 'psu%' then 'PSU'
				when tg in('NN1', 'NN2-6', 'NB0') then 'NN'
			else 'NT' end tg_shrunk 
		from cjm_segmentation.retained_users 
		where report_date=var_date; 
		
		-- events of DAUs
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select id, mobile_no, event_name, notification_id, event_timestamp
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			created_date=var_date
			and event_name like '%in_app%'; 
			
		-- necessary statistics
		insert into data_vajapora.update_message_analysis_2
		select 
			var_date report_date,
			coalesce(tg_shrunk, 'rest') tg, 
			notification_id, 
			count(distinct case when event_name='in_app_message_received' then mobile_no else null end) merchants_received_message, 
			count(distinct case when event_name='in_app_message_open' then mobile_no else null end) merchants_opened_message, 
			count(case when event_name='in_app_message_open' then id else null end) taps_on_message, 
			count(distinct case when event_name='in_app_message_close' then mobile_no else null end) merchants_closed_message, 
			count(distinct case when event_name='in_app_message_link_tap' then mobile_no else null end) merchants_acted_on_message,
			count(distinct case when event_name='in_app_message_link_tap' and app_version_name='4.0.2' and update_or_reg_datetime>=event_timestamp then mobile_no else null end) merchants_acted_on_message_and_updated
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			data_vajapora.help_c tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_no, app_version_name, update_or_reg_datetime
			from data_vajapora.version_wise_days
			where 
				date(update_or_reg_datetime)=var_date
				and update_or_reg_datetime!=reg_datetime
			) tbl3 using(mobile_no)
		group by 1, 2, 3; 
	
		raise notice 'Data generated for: %', var_date;
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	notification_id, 
	tg, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message,
	merchants_received_message, 
	merchants_opened_message, 
	taps_on_message, 
	merchants_closed_message,
	merchants_acted_on_message,  
	merchants_acted_on_message_and_updated
from 
	data_vajapora.update_message_analysis_2 tbl1 
	inner join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	notification_id in(2624)
	or concat(title, summary) like '%আপডেট করুন%'
order by report_date asc, merchants_received_message desc;

-- Version-03: overall/organic
select 
	report_date, 
	count(distinct tbl1.mobile_no) merchants_in_older_versions, 
	count(distinct tbl2.mobile_no) merchants_updated
from 
	(select mobile_no, report_date 
	from cjm_segmentation.retained_users  
	where 
		report_date>'2021-12-31'::date
		and app_version in('4.0.0', '4.0.1')
	) tbl1 
	
	left join 
		
	(select mobile_no, date(update_or_reg_datetime) update_date
	from data_vajapora.version_wise_days
	where 
		update_or_reg_datetime!=reg_datetime
		and app_version_name='4.0.2'
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date=tbl2.update_date)
group by 1
order by 1; 
