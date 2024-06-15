/*
- Viz: https://docs.google.com/spreadsheets/d/1RXgKF7FmiEq-oRMBB8SqYIdqvFZZxo73pcN8-eDdFmA/edit#gid=1292233767
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
	var_date date:=current_date-30;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.zombie_winback_analysis
		where report_date=var_date;
	
		-- last DAU date after registration
		drop table if exists data_vajapora.help_e;
		create table data_vajapora.help_e as
		select mobile_no, max(event_date) max_previous_event_date_after_reg
		from 
			tallykhata.tallykhata_user_date_sequence_final tbl1 
			inner join 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile 
			) tbl3 using(mobile_no)
		where 
			event_date<var_date
			and event_date>=reg_date
		group by 1; 
	
		-- returning zombie DAUs
		drop table if exists data_vajapora.help_c;
		create table data_vajapora.help_c as
		select mobile_no 
		from 
			(select mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl1 
			
			inner join
			
			data_vajapora.help_e tbl2 using(mobile_no)
		where var_date-max_previous_event_date_after_reg>=30; 
	
		-- returning transacting zombie DAUs
		drop table if exists data_vajapora.help_d;
		create table data_vajapora.help_d as
		select mobile_no 
		from 
			(select mobile_no
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date
			) tbl1 
			
			inner join
			
			data_vajapora.help_e tbl2 using(mobile_no)
		where var_date-max_previous_event_date_after_reg>=30; 
	
		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
	
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl3.id
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
		insert into data_vajapora.zombie_winback_analysis 
		select var_date report_date, *
		from 
			(select count(mobile_no) zombie_merchants_winback 
			from data_vajapora.help_c
			) tbl0,
			
			(select count(mobile_no) zombie_transacting_merchants_winback 
			from data_vajapora.help_d
			) tbl3, 
		
			(select count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_zombies
			from 
				data_vajapora.help_b
				inner join 
				data_vajapora.help_c using(mobile_no)
			) tbl1,
		
			(select count(mobile_no) first_and_only_open_through_inbox_zombies
			from 
				(select mobile_no
				from data_vajapora.help_b
				where id is not null
				) tbl1 
				
				inner join 
				
				(select mobile_no, count(id) app_open_events 
				from data_vajapora.help_a
				where event_name='app_opened'
				group by 1
				) tbl2 using(mobile_no)
				
				inner join 
				data_vajapora.help_c using(mobile_no)
			where app_open_events=1 
			) tbl2; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date-00 then exit;
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	zombie_merchants_winback, 
	zombie_transacting_merchants_winback, 
	first_open_through_inbox_merchants, 
	first_and_only_open_through_inbox_merchants,
	first_open_through_inbox_zombies, 
	first_and_only_open_through_inbox_zombies
from 
	data_vajapora.zombie_winback_analysis tbl1
	inner join 
	(select report_date, first_open_through_inbox_merchants, first_and_only_open_through_inbox_merchants
	from data_vajapora.dau_open_through_inbox_analysis 
	) tbl2 using(report_date)
order by 1; 