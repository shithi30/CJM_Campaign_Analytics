/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=2131303900
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
	var_date date:=current_date-15;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_inbox_message_analysis_welcome
		where report_date=var_date;

		-- all inbox receives+opens on date
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select *
		from 
			(select mobile_no, notification_id
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date
				and event_name='inbox_message_received'
			) tbl1 
			
			inner join 
			
			(select mobile_no, notification_id
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date
				and event_name='inbox_message_open'
			) tbl2 using(mobile_no, notification_id); 
		
		-- welcome messages inbox receives+opens on date
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			data_vajapora.help_b tbl1
			
			inner join 
			
			(select id notification_id
			from public.notification_pushmessage
			where title like '%টালিখাতা অ্যাপে স্বাগতম%'
			) tbl2 using(notification_id); 
			
		-- segment-wise summary metrics
		insert into data_vajapora.dau_inbox_message_analysis_welcome
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
			count(distinct case when tg_shrunk='NT' then mobile_no else null end) inbox_open_nt, 
			count(distinct case when tg_shrunk is null and var_date=reg_date then mobile_no else null end) registered_on_report_date, 
			count(distinct case when tg_shrunk is null and var_date>reg_date then mobile_no else null end) reinstalled, 
			count(distinct case when tg_shrunk is null and (var_date<reg_date or reg_date is null) then mobile_no else null end) unverified
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
			) tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile
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
	inbox_open_nt, 
	registered_on_report_date, 
	reinstalled, 
	unverified
from 
	data_vajapora.dau_inbox_message_analysis_welcome tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where inbox_open_merchants>99;

-- see the cases for investigation
do $$

declare 
	var_date date:=current_date-15;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_inbox_message_analysis_welcome_inv
		where report_date=var_date;

		-- all inbox receives+opens on date
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select *
		from 
			(select mobile_no, notification_id
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date
				and event_name='inbox_message_received'
			) tbl1 
			
			inner join 
			
			(select mobile_no, notification_id
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date
				and event_name='inbox_message_open'
			) tbl2 using(mobile_no, notification_id); 
		
		-- welcome messages inbox receives+opens on date
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			data_vajapora.help_b tbl1
			
			inner join 
			
			(select id notification_id
			from public.notification_pushmessage
			where title like '%টালিখাতা অ্যাপে স্বাগতম%'
			) tbl2 using(notification_id); 
			
		-- cases where zombies were found to receive welcome msg
		insert into data_vajapora.dau_inbox_message_analysis_welcome_inv
		select distinct var_date report_date, mobile_no, reg_date
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
			) tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile
			) tbl3 using(mobile_no)
		where tg_shrunk='ZAll'; 
			
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date-00 then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.dau_inbox_message_analysis_welcome_inv
-- where report_date!=reg_date; 
