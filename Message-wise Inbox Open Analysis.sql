/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=148653500
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
	var_date date:=current_date-10;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_inbox_message_analysis
		where report_date=var_date;

		-- all inbox opens
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select mobile_no, notification_id
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			event_date=var_date
			and event_name='inbox_message_open'; 
			
		-- segment-wise summary metrics
		insert into data_vajapora.dau_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct mobile_no) inbox_open_merchants,
			count(distinct case when tg_shrunk='PUAll' then mobile_no else null end) inbox_open_puall,
			count(distinct case when tg_shrunk='3RAUAll' then mobile_no else null end) inbox_open_3raull,
			count(distinct case when tg_shrunk='LTUAll' then mobile_no else null end) inbox_open_ltuall,
			count(distinct case when tg_shrunk='ZAll' then mobile_no else null end) inbox_open_zall,
			count(distinct case when tg_shrunk='PSU' then mobile_no else null end) inbox_open_psu,
			count(distinct case when tg_shrunk='NN2-6' then mobile_no else null end) inbox_open_nn26,
			count(distinct case when tg_shrunk='NN1' then mobile_no else null end) inbox_open_nn1,
			count(distinct case when tg_shrunk='NT' then mobile_no else null end) inbox_open_nt
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select 
				mobile_no, 
				case 
					when tg ilike 'pu%' then 'PUAll'
					when tg ilike '3rau%'  then '3RAUAll'
					when tg ilike 'ltu%' then 'LTUAll'
					when tg ilike 'z%' then 'ZAll'
					when tg ilike 'psu%' then 'PSU'
					when tg ilike '%NN2-6%' then 'NN2-6'
					when tg ilike '%NN1%' then 'NN1'
				else 'NT' end tg_shrunk
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl3 using(mobile_no)
		group by 1, 2; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date-00 then exit;
		end if; 
	end loop; 
end $$; 

select
	report_date, 
	-- notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else title end title, 
	inbox_open_merchants, 
	inbox_open_puall,
	inbox_open_3raull,
	inbox_open_ltuall,
	inbox_open_zall,
	inbox_open_psu,
	inbox_open_nn26,
	inbox_open_nn1,
	inbox_open_nt
from 
	data_vajapora.dau_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where inbox_open_merchants>99;

-- Version-02
do $$

declare 
	var_date date:=current_date-7; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_inbox_message_analysis
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

		-- DAUs' inbox open events
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select mobile_no, notification_id
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			created_date=var_date
			and event_name in('inbox_message_open'); 
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct mobile_no) inbox_open_merchants,
			count(distinct case when tg_shrunk='PUAll' then mobile_no else null end) inbox_open_puall,
			count(distinct case when tg_shrunk='3RAUAll' then mobile_no else null end) inbox_open_3raull,
			count(distinct case when tg_shrunk='LTUAll' then mobile_no else null end) inbox_open_ltuall,
			count(distinct case when tg_shrunk='ZAll' then mobile_no else null end) inbox_open_zall,
			count(distinct case when tg_shrunk='PSU' then mobile_no else null end) inbox_open_psu,
			count(distinct case when tg_shrunk='NN2-6' then mobile_no else null end) inbox_open_nn26,
			count(distinct case when tg_shrunk='NN1' then mobile_no else null end) inbox_open_nn1,
			count(distinct case when tg_shrunk='NB0' then mobile_no else null end) inbox_open_nb0,
			count(distinct case when tg_shrunk='NT' then mobile_no else null end) inbox_open_nt, 
			count(distinct case when tg_shrunk is null then mobile_no else null end) inbox_open_rest 
		from 
			data_vajapora.temp_b tbl1 
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
		group by 1, 2; 
	
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
	inbox_open_merchants, 
	inbox_open_puall,
	inbox_open_3raull,
	inbox_open_ltuall,
	inbox_open_zall,
	inbox_open_psu,
	inbox_open_nn26,
	inbox_open_nn1,
	inbox_open_nb0,
	inbox_open_nt, 
	inbox_open_rest
from 
	data_vajapora.dau_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	inbox_open_merchants!=0
	-- and report_date>=current_date-14
	and inbox_open_merchants>99
order by 1 desc, 3 desc; 


