/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=249839838
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
	var_date date:=current_date-5; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_open_through_inbox_message_analysis_2
		where report_date=var_date;
	
		-- TG fixed date
		var_tg_fixed_date:=var_date-(case when extract(dow from var_date)=7 then 0 else extract(dow from var_date) end)::int; 
	
		-- TG
		drop table if exists data_vajapora.temp_c;
		create table data_vajapora.temp_c as
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
			else 'NT' end tg_shrunk
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
		-- analyze data_vajapora.temp_c;

		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where created_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		-- analyze data_vajapora.temp_a;
		
		-- all push-open cases with message ids, with first opens of the day identified
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select tbl1.mobile_no, tbl1.notification_id, tbl3.id
		from 
			data_vajapora.temp_a tbl1
			inner join 
			data_vajapora.temp_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from data_vajapora.temp_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		-- analyze data_vajapora.temp_b;
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_open_through_inbox_message_analysis_2
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct mobile_no) open_through_inbox_merchants,
			count(distinct case when tg_shrunk='PUAll' then mobile_no else null end) open_through_inbox_puall,
			count(distinct case when tg_shrunk='3RAUAll' then mobile_no else null end) open_through_inbox_3raull,
			count(distinct case when tg_shrunk='LTUAll' then mobile_no else null end) open_through_inbox_ltuall,
			count(distinct case when tg_shrunk='ZAll' then mobile_no else null end) open_through_inbox_zall,
			count(distinct case when tg_shrunk='PSU' then mobile_no else null end) open_through_inbox_psu,
			count(distinct case when tg_shrunk='NN2-6' then mobile_no else null end) open_through_inbox_nn26,
			count(distinct case when tg_shrunk='NN1' then mobile_no else null end) open_through_inbox_nn1,
			count(distinct case when tg_shrunk='NB0' then mobile_no else null end) open_through_inbox_nb0,
			count(distinct case when tg_shrunk='NT' then mobile_no else null end) open_through_inbox_nt, 
			count(distinct case when tg_shrunk is null then mobile_no else null end) open_through_inbox_rest 
		from 
			data_vajapora.temp_b tbl1 
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
		group by 1, 2; 
		commit; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select
	report_date, 
	-- notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end title, 
	open_through_inbox_merchants, 
	open_through_inbox_puall,
	open_through_inbox_3raull,
	open_through_inbox_ltuall,
	open_through_inbox_zall,
	open_through_inbox_psu,
	open_through_inbox_nn26,
	open_through_inbox_nn1,
	open_through_inbox_nb0,
	open_through_inbox_nt, 
	open_through_inbox_rest
from 
	data_vajapora.dau_open_through_inbox_message_analysis_2 tbl1 
	left join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where open_through_inbox_merchants>99
order by 1 desc, 3 desc; 
