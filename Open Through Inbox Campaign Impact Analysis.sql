/*
- Viz: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=0
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Go to the next sheets for sanity check. 
*/

do $$

declare 
	var_date date:='2021-12-01'::date;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_open_through_inbox_analysis
		where report_date=var_date;
	
		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where created_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
	
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select tbl1.mobile_no, tbl3.id
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
	
		-- necessary statistics
		insert into data_vajapora.dau_open_through_inbox_analysis 
		select *
		from 
			(select
				var_date report_date,
				(select count(distinct mobile_no) from data_vajapora.temp_a where event_name='inbox_message_open') inbox_opened_merchants,
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants,
				count(distinct mobile_no) open_through_inbox_merchants
			from data_vajapora.temp_b
			) tbl1,
		
			(select count(mobile_no) first_and_only_open_through_inbox_merchants
			from 
				(select mobile_no
				from data_vajapora.temp_b
				where id is not null
				) tbl1 
				
				inner join 
				
				(select mobile_no, count(id) app_open_events 
				from data_vajapora.temp_a
				where event_name='app_opened'
				group by 1
				) tbl2 using(mobile_no)
			where app_open_events=1 
			) tbl2; 
		commit; 
		
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date-00 then exit;
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	total_active_user_db_event, 
	inbox_opened_merchants, 
	first_open_through_inbox_merchants, 
	first_and_only_open_through_inbox_merchants,
	open_through_inbox_merchants
from 
	data_vajapora.dau_open_through_inbox_analysis tbl1 
	inner join
	(-- dashboard DAU
	select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event','Non Verified')
		group by d.report_date
		) as tbl_1 
	order by 1
	) tbl2 using(report_date)
order by 1;
