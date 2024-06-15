/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=859112593
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

-- time spent
do $$ 

declare 
	var_date date:=(select max(report_date)-7 from data_vajapora.reg_day_time_spent); 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.reg_day_time_spent 
		where report_date=var_date; 
	
		-- sequenced terminal events on reg date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name in('app_in_background', 'app_opened', 'app_launched', 'app_closed')
				and event_date=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl2 using(mobile_no); 
		
		-- avg. mins spent on reg date
		insert into data_vajapora.reg_day_time_spent
		select 
			var_date report_date,
			
			count(tbl1.mobile_no) reg_merchants_recorded_time, 
			sum(min_spent) reg_merchant_total_min_spent, 
			avg(min_spent) reg_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) reg_txn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_total_min_spent,
			avg(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) reg_nontxn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_total_min_spent, 
			avg(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_avg_min_spent
		from 
			(select mobile_no, sum(sec_spent)/60.00 min_spent
			from 
				(select 
					tbl1.mobile_no, 
					date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
					+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
					+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					sec_spent
				from 
					data_vajapora.help_a tbl1
					inner join 
					data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				where 
					tbl1.event_name in('app_opened', 'app_launched')
					and tbl2.event_name in('app_in_background', 'app_closed')
				) tbl1 
			group by 1
			) tbl1 
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl2 using(mobile_no); 

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

-- time spent in inbox
do $$ 

declare 
	var_date date:=(select max(report_date)-7 from data_vajapora.reg_day_time_spent_inbox); 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.reg_day_time_spent_inbox 
		where report_date=var_date; 
	
		-- sequenced events on reg date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final 
			where event_date=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl2 using(mobile_no); 
		
		-- avg. mins spent in inbox on reg date
		insert into data_vajapora.reg_day_time_spent_inbox
		select 
			var_date report_date,
			avg(min_spent) reg_merchant_avg_min_spent_inbox
		from 
			(select mobile_no, sum(sec_spent)/60.00 min_spent
			from 
				(select 
					tbl1.mobile_no, 
					date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
					+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
					+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					sec_spent
				from 
					data_vajapora.help_a tbl1
					inner join 
					data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				where tbl1.event_name in('inbox_message_open', 'inbox_message_action')
				) tbl1 
			group by 1
			) tbl1; 

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

-- open through inbox
do $$

declare 
	var_date date:=(select max(report_date)-7 from data_vajapora.mom_cjm_performance_detailed); 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.mom_cjm_performance_detailed
		where report_date=var_date;

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
		
		-- all push-open cases with message ids, with first opens of the day identified
		insert into data_vajapora.mom_cjm_performance_detailed
		select var_date report_date, tbl1.mobile_no, tbl1.notification_id, tbl3.id
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
		
		commit; 
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

-- inbox open 
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no, event_date report_date
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_name='inbox_message_open'
	and event_date>='2022-01-01'; 
	
-- necessary metrics
select 
	report_date,
	merchants_registered,
	registered_merchants_transacted, 
	reg_merchant_avg_min_spent, 
	reg_merchant_avg_min_spent_inbox, 
	registered_merchants_opened_inbox, 
	open_through_inbox_on_reg_day, 
	first_open_through_inbox_on_reg_day
from 
	data_vajapora.reg_day_time_spent tbl1 
	
	inner join 

	(select date(created_at) report_date, count(mobile_number) merchants_registered 
	from public.register_usermobile
	group by 1
	) tbl2 using(report_date)
	
	inner join 
	
	(select 
		report_date, 
		count(distinct mobile_no) open_through_inbox_on_reg_day, 
		count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_on_reg_day
	from 
		data_vajapora.mom_cjm_performance_detailed tbl1 
		
		inner join 
		
		(select date(created_at) report_date, mobile_number mobile_no
		from public.register_usermobile
		) tbl2 using(mobile_no, report_date)
	group by 1
	) tbl3 using(report_date)
	
	inner join 
	
	(select 
		report_date, 
		count(tbl2.mobile_no) registered_merchants_transacted, 
		count(tbl3.mobile_no) registered_merchants_opened_inbox 
	from 
		(select date(created_at) report_date, mobile_number mobile_no
		from public.register_usermobile
		) tbl1 
		
		left join 
		
		(select mobile_no, created_datetime report_date
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		) tbl2 using(report_date, mobile_no)
		
		left join 
		
		data_vajapora.help_b tbl3 using(report_date, mobile_no)
	group by 1
	) tbl4 using(report_date)
	
	inner join  
	
	data_vajapora.reg_day_time_spent_inbox tbl5 using(report_date)
order by 1 desc; 

