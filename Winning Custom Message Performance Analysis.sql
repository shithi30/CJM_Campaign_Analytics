/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=824188769
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
	var_date date:='2021-12-20'::date; 
	var_start_time timestamp; 
	var_end_time timestamp; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.winning_custom_message_analysis 
		where report_date=var_date; 
	
		-- campaign period
		var_start_time:=var_date+interval '15 hours'; 
		var_end_time:=var_date+interval '15 hours'+interval '24 hours'; 
	
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
		select id, mobile_no, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from tallykhata.tallykhata_sync_event_fact_final
		where event_timestamp>=var_start_time and event_timestamp<var_end_time; 
			
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
		insert into data_vajapora.winning_custom_message_analysis
		select 
			var_date report_date, 
			tg, 
			notification_id, 
			merchants_received_message, 
			merchants_opened_message, 
			taps_on_message, 
			coalesce(open_through_inbox_merchants, 0) open_through_inbox_merchants, 
			coalesce(first_open_through_inbox_merchants, 0) first_open_through_inbox_merchants
		from 
			(select 
				coalesce(tg_shrunk, 'rest') tg, 
				notification_id, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) merchants_received_message, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) merchants_opened_message, 
				count(case when event_name='inbox_message_open' then id else null end) taps_on_message 
			from 
				data_vajapora.help_a 
				left join 
				data_vajapora.help_c using(mobile_no)
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
		
		raise notice 'Data generated for: %, from % to %', var_date, var_start_time, var_end_time; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	notification_id, 
	tg, 
	title,
	merchants_received_message, 
	merchants_opened_message, 
	taps_on_message, 
	open_through_inbox_merchants, 
	first_open_through_inbox_merchants
from 
	data_vajapora.winning_custom_message_analysis tbl1 
	inner join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where notification_id in(2587)
order by report_date asc, merchants_received_message desc; 

