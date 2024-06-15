/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=2064352780
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
		delete from data_vajapora.dau_open_through_inbox_message_analysis
		where report_date=var_date;

		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		
		-- all push-open cases with message ids, with first opens of the day identified
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl1.notification_id, tbl3.id
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
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_open_through_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants,
			count(distinct case when id is not null and tg_shrunk='PUAll' then mobile_no else null end) first_open_through_inbox_puall,
			count(distinct case when id is not null and tg_shrunk='3RAUAll' then mobile_no else null end) first_open_through_inbox_3raull,
			count(distinct case when id is not null and tg_shrunk='LTUAll' then mobile_no else null end) first_open_through_inbox_ltuall,
			count(distinct case when id is not null and tg_shrunk='ZAll' then mobile_no else null end) first_open_through_inbox_zall,
			count(distinct case when id is not null and tg_shrunk='PSU' then mobile_no else null end) first_open_through_inbox_psu,
			count(distinct case when id is not null and tg_shrunk='NN2-6' then mobile_no else null end) first_open_through_inbox_nn26,
			count(distinct case when id is not null and tg_shrunk='NN1' then mobile_no else null end) first_open_through_inbox_nn1,
			count(distinct case when id is not null and tg_shrunk='NT' then mobile_no else null end) first_open_through_inbox_nt
		from 
			data_vajapora.help_b tbl1 
			
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
	first_open_through_inbox_merchants, 
	first_open_through_inbox_puall,
	first_open_through_inbox_3raull,
	first_open_through_inbox_ltuall,
	first_open_through_inbox_zall,
	first_open_through_inbox_psu,
	first_open_through_inbox_nn26,
	first_open_through_inbox_nn1,
	first_open_through_inbox_nt
from 
	data_vajapora.dau_open_through_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where first_open_through_inbox_merchants!=0;

-- Version-02
do $$

declare 
	-- var_date date:='2021-12-05'::date;
	var_date date:=current_date-3; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_open_through_inbox_message_analysis
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
				when tg ilike 'z%' and reg_date=var_tg_fixed_date then 'NN0' 
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
				and tg not in('NN2-6', 'NN1')
				and mobile_no not in 
					(select mobile_no
					from cjm_segmentation.retained_users 
					where 
						report_date=var_date  
						and tg in('NN2-6', 'NN1')
					) 
				
			union all 
			
			select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date  
				and tg in('NN2-6', 'NN1')
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile 
			) tbl2 using(mobile_no); 
		analyze data_vajapora.temp_c;

		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile_no, notification_id, 1 received_status
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date  
				and event_name='inbox_message_received'
			) tbl2 using(mobile_no, notification_id)
		where event_name in('app_opened', 'inbox_message_open'); 
		analyze data_vajapora.temp_a;
		
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
			tbl1.received_status=1
			and tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		analyze data_vajapora.temp_b;
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_open_through_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants,
			count(distinct case when id is not null and tg_shrunk='PUAll' then mobile_no else null end) first_open_through_inbox_puall,
			count(distinct case when id is not null and tg_shrunk='3RAUAll' then mobile_no else null end) first_open_through_inbox_3raull,
			count(distinct case when id is not null and tg_shrunk='LTUAll' then mobile_no else null end) first_open_through_inbox_ltuall,
			count(distinct case when id is not null and tg_shrunk='ZAll' then mobile_no else null end) first_open_through_inbox_zall,
			count(distinct case when id is not null and tg_shrunk='PSU' then mobile_no else null end) first_open_through_inbox_psu,
			count(distinct case when id is not null and tg_shrunk='NN2-6' then mobile_no else null end) first_open_through_inbox_nn26,
			count(distinct case when id is not null and tg_shrunk='NN1' then mobile_no else null end) first_open_through_inbox_nn1,
			count(distinct case when id is not null and tg_shrunk='NN0' then mobile_no else null end) first_open_through_inbox_nn0,
			count(distinct case when id is not null and tg_shrunk='NT' then mobile_no else null end) first_open_through_inbox_nt, 
			count(distinct case when id is not null and tg_shrunk is null then mobile_no else null end) first_open_through_inbox_rest 
		from 
			data_vajapora.temp_b tbl1 
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
		group by 1, 2; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date+1 then exit;
		end if; 
	end loop; 
end $$; 

select
	report_date, 
	-- notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else title end title, 
	first_open_through_inbox_merchants, 
	first_open_through_inbox_puall,
	first_open_through_inbox_3raull,
	first_open_through_inbox_ltuall,
	first_open_through_inbox_zall,
	first_open_through_inbox_psu,
	first_open_through_inbox_nn26,
	first_open_through_inbox_nn1,
	first_open_through_inbox_nn0,
	first_open_through_inbox_nt, 
	first_open_through_inbox_rest
from 
	data_vajapora.dau_open_through_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id); 

-- Version-03
do $$

declare 
	var_date date:='2021-12-12'::date;
	-- var_date date:=current_date-3; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_open_through_inbox_message_analysis
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
		analyze data_vajapora.temp_c;

		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
			
			/*left join 
			
			(select mobile_no, notification_id, 1 received_status
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date  
				and event_name='inbox_message_received'
			) tbl2 using(mobile_no, notification_id)*/
		where event_name in('app_opened', 'inbox_message_open'); 
		analyze data_vajapora.temp_a;
		
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
			1=1
			-- and tbl1.received_status=1
			and tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		analyze data_vajapora.temp_b;
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_open_through_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants,
			count(distinct case when id is not null and tg_shrunk='PUAll' then mobile_no else null end) first_open_through_inbox_puall,
			count(distinct case when id is not null and tg_shrunk='3RAUAll' then mobile_no else null end) first_open_through_inbox_3raull,
			count(distinct case when id is not null and tg_shrunk='LTUAll' then mobile_no else null end) first_open_through_inbox_ltuall,
			count(distinct case when id is not null and tg_shrunk='ZAll' then mobile_no else null end) first_open_through_inbox_zall,
			count(distinct case when id is not null and tg_shrunk='PSU' then mobile_no else null end) first_open_through_inbox_psu,
			count(distinct case when id is not null and tg_shrunk='NN2-6' then mobile_no else null end) first_open_through_inbox_nn26,
			count(distinct case when id is not null and tg_shrunk='NN1' then mobile_no else null end) first_open_through_inbox_nn1,
			count(distinct case when id is not null and tg_shrunk='NB0' then mobile_no else null end) first_open_through_inbox_nb0,
			count(distinct case when id is not null and tg_shrunk='NT' then mobile_no else null end) first_open_through_inbox_nt, 
			count(distinct case when id is not null and tg_shrunk is null then mobile_no else null end) first_open_through_inbox_rest 
		from 
			data_vajapora.temp_b tbl1 
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
		group by 1, 2; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date+1 then exit;
		end if; 
	end loop; 
end $$; 

select
	report_date, 
	-- notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else title end title, 
	first_open_through_inbox_merchants, 
	first_open_through_inbox_puall,
	first_open_through_inbox_3raull,
	first_open_through_inbox_ltuall,
	first_open_through_inbox_zall,
	first_open_through_inbox_psu,
	first_open_through_inbox_nn26,
	first_open_through_inbox_nn1,
	first_open_through_inbox_nb0,
	first_open_through_inbox_nt, 
	first_open_through_inbox_rest
from 
	data_vajapora.dau_open_through_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	report_date>='2021-12-12'::date and report_date<current_date
	and first_open_through_inbox_merchants!=0; 

-- cross-check
select *, first_open_through_inbox_merchants_broken-first_open_through_inbox_merchants_authentic merchants_synced_later
from 
	(select report_date, sum(first_open_through_inbox_merchants) first_open_through_inbox_merchants_broken
	from data_vajapora.dau_open_through_inbox_message_analysis 
	group by 1
	) tbl1 
	
	inner join 
	
	(select report_date, first_open_through_inbox_merchants first_open_through_inbox_merchants_authentic
	from data_vajapora.dau_open_through_inbox_analysis
	) tbl2 using(report_date) 
order by 1;

-- Version-04
do $$

declare 
	var_date date:=current_date-7; 
	var_tg_fixed_date date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_open_through_inbox_message_analysis
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
		analyze data_vajapora.temp_c;

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
		analyze data_vajapora.temp_a;
		
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
		analyze data_vajapora.temp_b;
		
		-- segment-wise summary metrics
		insert into data_vajapora.dau_open_through_inbox_message_analysis
		select
			var_date report_date,
			case when notification_id is null then -1 else notification_id end notification_id, 
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants,
			count(distinct case when id is not null and tg_shrunk='PUAll' then mobile_no else null end) first_open_through_inbox_puall,
			count(distinct case when id is not null and tg_shrunk='3RAUAll' then mobile_no else null end) first_open_through_inbox_3raull,
			count(distinct case when id is not null and tg_shrunk='LTUAll' then mobile_no else null end) first_open_through_inbox_ltuall,
			count(distinct case when id is not null and tg_shrunk='ZAll' then mobile_no else null end) first_open_through_inbox_zall,
			count(distinct case when id is not null and tg_shrunk='PSU' then mobile_no else null end) first_open_through_inbox_psu,
			count(distinct case when id is not null and tg_shrunk='NN2-6' then mobile_no else null end) first_open_through_inbox_nn26,
			count(distinct case when id is not null and tg_shrunk='NN1' then mobile_no else null end) first_open_through_inbox_nn1,
			count(distinct case when id is not null and tg_shrunk='NB0' then mobile_no else null end) first_open_through_inbox_nb0,
			count(distinct case when id is not null and tg_shrunk='NT' then mobile_no else null end) first_open_through_inbox_nt, 
			count(distinct case when id is not null and tg_shrunk is null then mobile_no else null end) first_open_through_inbox_rest 
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
	first_open_through_inbox_merchants, 
	first_open_through_inbox_puall,
	first_open_through_inbox_3raull,
	first_open_through_inbox_ltuall,
	first_open_through_inbox_zall,
	first_open_through_inbox_psu,
	first_open_through_inbox_nn26,
	first_open_through_inbox_nn1,
	first_open_through_inbox_nb0,
	first_open_through_inbox_nt, 
	first_open_through_inbox_rest
from 
	data_vajapora.dau_open_through_inbox_message_analysis tbl1 
	left join 
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where first_open_through_inbox_merchants!=0
order by 1 desc, 3 desc; 

-- sanity check
select *, first_open_through_inbox_merchants_broken-first_open_through_inbox_merchants_authentic merchants_synced_later
from 
	(select report_date, sum(first_open_through_inbox_merchants) first_open_through_inbox_merchants_broken
	from data_vajapora.dau_open_through_inbox_message_analysis 
	group by 1
	) tbl1 
	
	inner join 
	
	(select report_date, first_open_through_inbox_merchants first_open_through_inbox_merchants_authentic
	from data_vajapora.dau_open_through_inbox_analysis
	) tbl2 using(report_date) 
order by 1;
