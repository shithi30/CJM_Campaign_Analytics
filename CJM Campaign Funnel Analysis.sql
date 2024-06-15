/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1dc2D-Xl0jzs2EuF6gr7F-WgWX_VVbSjHC8YE4pm6qPw/edit#gid=0
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
	var_date date:='2021-12-05'::date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.inbox_funnel_analysis
		where report_date=var_date; 
		
		-- TG
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select
			mobile_no, 
			case 
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when tg ilike 'z%' and reg_date='2021-12-05'::date then 'NN0' -- TG fixed date
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg ilike '%NN2-6%' then 'NN2-6'
				when tg ilike '%NN1%' then 'NN1'
			else 'NT' end tg
		from 
			(select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where 
				report_date='2021-12-05'::date -- TG fixed date
				and tg not in('NN2-6', 'NN1')
				
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
		analyze data_vajapora.help_a;
			
		-- sequenced events of DAUs, filtered for necessary events on same (received+open) date
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
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
		analyze data_vajapora.help_b;
		
		-- metrics
		insert into data_vajapora.inbox_funnel_analysis
		select *
		from 
			(-- funnel: part-01
			select var_date report_date, notification_id, tg, tg_size, inbox_message_received, inbox_message_opened
			from 
				(select
					notification_id, 
					tg, 
					count(distinct tbl2.mobile_no) inbox_message_received, 
					count(distinct tbl3.mobile_no) inbox_message_opened 
				from 
					data_vajapora.help_a tbl1 
					
					inner join 
					
					(select mobile_no, notification_id, 1 received_status
					from tallykhata.tallykhata_sync_event_fact_final
					where 
						event_date=var_date  
						and event_name='inbox_message_received'
					) tbl2 using(mobile_no) 
					
					left join 
					
					(select mobile_no, notification_id, 1 opened_status
					from tallykhata.tallykhata_sync_event_fact_final
					where 
						event_date=var_date  
						and event_name='inbox_message_open'
					) tbl3 using(mobile_no, notification_id)
				group by 1, 2
				) tbl1 
				
				inner join 
				
				(select tg, count(mobile_no) tg_size 
				from data_vajapora.help_a 
				group by 1
				) tbl2 using(tg)
			) tbl1 
			
			inner join 
			
			(-- funnel: part-02
			select
				var_date report_date,
				notification_id, 
				tg, 
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
			from 
				(-- all push-open cases with message ids, with first opens of the day identified
				select tbl1.mobile_no, tbl1.notification_id, tbl3.id
				from 
					data_vajapora.help_b tbl1
					inner join 
					data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
					left join 
					(select mobile_no, min(id) id 
					from data_vajapora.help_b 
					where event_name='app_opened'
					group by 1
					) tbl3 on(tbl2.id=tbl3.id)
				where 
					tbl1.received_status=1
					and tbl1.event_name='inbox_message_open'
					and tbl2.event_name='app_opened'
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a tbl2 using(mobile_no)
			group by 1, 2, 3
			) tbl2 using(report_date, notification_id, tg); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 

end $$; 

select *
from data_vajapora.inbox_funnel_analysis; 

select 
	report_date, 
	-- notification_id, 
	title, 
	tg, 
	tg_size, 
	inbox_message_received, 
	inbox_message_opened, 
	first_open_through_inbox_merchants
from 
	data_vajapora.inbox_funnel_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id); 



-- Version-02
do $$ 

declare 
	var_date date:='2021-12-05'::date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.inbox_funnel_analysis
		where report_date=var_date; 
		
		-- TG
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select
			mobile_no, 
			case 
				when tg ilike 'pu%' then 'PUAll'
				when tg ilike '3rau%'  then '3RAUAll'
				when tg ilike 'ltu%' then 'LTUAll' 
				when tg ilike 'z%' and reg_date='2021-12-05'::date then 'NN0' -- TG fixed date
				when tg ilike 'z%' then 'ZAll'
				when tg ilike 'psu%' then 'PSU'
				when tg ilike '%NN2-6%' then 'NN2-6'
				when tg ilike '%NN1%' then 'NN1'
			else 'NT' end tg
		from 
			(select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where 
				report_date='2021-12-05'::date -- TG fixed date
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
		analyze data_vajapora.help_a;
			
		-- sequenced events of DAUs, filtered for necessary events on same (received+open) date
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
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
		analyze data_vajapora.help_b;
		
		-- metrics
		insert into data_vajapora.inbox_funnel_analysis
		select *
		from 
			(-- funnel: part-01
			select var_date report_date, notification_id, tg, tg_size, inbox_message_received, inbox_message_opened
			from 
				(select
					notification_id, 
					tg, 
					count(distinct tbl2.mobile_no) inbox_message_received, 
					count(distinct tbl3.mobile_no) inbox_message_opened 
				from 
					data_vajapora.help_a tbl1 
					
					inner join 
					
					(select mobile_no, notification_id, 1 received_status
					from tallykhata.tallykhata_sync_event_fact_final
					where 
						event_date=var_date  
						and event_name='inbox_message_received'
					) tbl2 using(mobile_no) 
					
					left join 
					
					(select mobile_no, notification_id, 1 opened_status
					from tallykhata.tallykhata_sync_event_fact_final
					where 
						event_date=var_date  
						and event_name='inbox_message_open'
					) tbl3 using(mobile_no, notification_id)
				group by 1, 2
				) tbl1 
				
				inner join 
				
				(select tg, count(mobile_no) tg_size 
				from data_vajapora.help_a 
				group by 1
				) tbl2 using(tg)
			) tbl1 
			
			inner join 
			
			(-- funnel: part-02
			select
				var_date report_date,
				notification_id, 
				tg, 
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
			from 
				(-- all push-open cases with message ids, with first opens of the day identified
				select tbl1.mobile_no, tbl1.notification_id, tbl3.id
				from 
					data_vajapora.help_b tbl1
					inner join 
					data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
					left join 
					(select mobile_no, min(id) id 
					from data_vajapora.help_b 
					where event_name='app_opened'
					group by 1
					) tbl3 on(tbl2.id=tbl3.id)
				where 
					tbl1.received_status=1
					and tbl1.event_name='inbox_message_open'
					and tbl2.event_name='app_opened'
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a tbl2 using(mobile_no)
			group by 1, 2, 3
			) tbl2 using(report_date, notification_id, tg); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-12-11'::date+1 then exit; 
		end if; 
	end loop; 

end $$; 

select *
from data_vajapora.inbox_funnel_analysis; 

select 
	report_date, 
	-- notification_id, 
	title, 
	tg, 
	tg_size, 
	inbox_message_received, 
	inbox_message_opened, 
	first_open_through_inbox_merchants
from 
	data_vajapora.inbox_funnel_analysis tbl1 
	left join 
	(select id notification_id, title
	from public.notification_pushmessage
	) tbl2 using(notification_id); 


