/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=470898418
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
	var_date date:=current_date-7; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_inapp_message_analysis
		where report_date=var_date;
	
		-- TG fixed date
		var_tg_fixed_date:=var_date-(case when extract(dow from var_date)=7 then 0 else extract(dow from var_date) end)::int; 
	
		-- TG
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select
			mobile_no, 
			case 
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when (tg ilike 'z%' and reg_date=var_tg_fixed_date) or tg='NB0' then 'NB0' 
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg ilike '%NN2-6%' then 'NN2-6'
				when tg ilike '%NN1%' then 'NN1'
				else 'NT' 
			end tg_shrunk
		from 
			(select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where 
				report_date=var_tg_fixed_date 
				and tg not in('NN2-6', 'NN1', 'NB0')
				and mobile_no not in 
					(select mobile_no
					from cjm_segmentation.retained_users 
					where 
						report_date=var_date  
						and tg in('NN2-6', 'NN1', 'NB0')
					) 
				
			union all 
			
			select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date  
				and tg in('NN2-6', 'NN1', 'NB0')
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile 
			) tbl2 using(mobile_no); 

		-- events of DAUs
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select id, mobile_no, event_name, notification_id
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			created_date=var_date
			and event_name like '%in_app%'; 
		
		-- necessary statistics
		insert into data_vajapora.dau_inapp_message_analysis
		select 
			var_date report_date,
			coalesce(tg_shrunk, 'rest') tg, 
			notification_id, 
			count(distinct case when event_name='in_app_message_received' then mobile_no else null end) merchants_received_message, 
			count(distinct case when event_name='in_app_message_open' then mobile_no else null end) merchants_viewed_message, 
			count(case when event_name='in_app_message_open' then id else null end) message_views, 
			count(distinct case when event_name='in_app_message_close' then mobile_no else null end) merchants_closed_message, 
			count(distinct case when event_name='in_app_message_link_tap' then mobile_no else null end) merchants_acted_on_message
		from 
			data_vajapora.help_a tbl1
			
			left join 
			
			data_vajapora.help_c tbl2 using(mobile_no)    
		group by 1, 2, 3; 
		
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select
	report_date,
	tg,
	notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g') end title, 
	merchants_received_message, 
	merchants_viewed_message, 
	message_views, 
	merchants_closed_message, 
	merchants_acted_on_message
from 
	data_vajapora.dau_inapp_message_analysis tbl1 
	left join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	merchants_viewed_message!=0
	and merchants_received_message>999
order by 1 desc, 5 desc; 
