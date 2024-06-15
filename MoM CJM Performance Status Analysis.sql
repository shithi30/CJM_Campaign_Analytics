/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=2082953978
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Regarding MoM CJM Performance Status Analysis!
- Notes (if any): 
*/

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
			(select id, mobile_no, event_name, notification_id, bulk_notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		
		-- all push-open cases with message ids, with first opens of the day identified
		insert into data_vajapora.mom_cjm_performance_detailed
		select var_date report_date, tbl1.mobile_no, tbl1.notification_id, tbl3.id, tbl1.bulk_notification_id, tbl4.if_transacted
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
			left join 
			(select mobile_no, 1 if_transacted
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date
			) tbl4 on(tbl2.mobile_no=tbl4.mobile_no)
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

select * 
from data_vajapora.mom_cjm_performance_detailed; 

-- inbox open
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	left(event_date::text, 7) year_month, 
	count(mobile_no) inbox_open_events, 
	count(distinct mobile_no) inbox_open_merchants
from tallykhata.tallykhata_sync_event_fact_final 
where event_name='inbox_message_open' 
group by 1; 

-- time spent in inbox
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	left(report_date::text, 7) year_month, 
	sum(inbox_time_spent_in_min) minutes_spent_in_inbox, 
	count(distinct case when inbox_time_spent_in_min!=0 then mobile_no else null end) merchants_spent_time_in_inbox
from test.userwise_time_spent
group by 1; 

select *
from 
	(select 
		left(report_date::text, 7) year_month, 
		count(mobile_no) open_through_inbox_events, 
		count(distinct mobile_no) open_through_inbox_merchants, 
		count(case when id is not null then mobile_no else null end) first_open_through_inbox_events,
		count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
	from data_vajapora.mom_cjm_performance_detailed
	group by 1
	) tbl1 
	
	left join 
	
	data_vajapora.help_a tbl2 using(year_month)
	
	left join 
	
	data_vajapora.help_b tbl3 using(year_month); 

-- statistics of received
do $$ 

declare 
	var_mon text; 
	var_mon_start date; 
	var_mon_end date; 
	var_seq int:=coalesce((select max(seq)+1 from data_vajapora.monthly_inbox_rec), 1); 
	var_max_seq int; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- calendar
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		to_char(dt, 'YYYY-MM') mon, 
		min(dt) mon_start, 
		max(dt) mon_end, 
		row_number() over(order by to_char(dt, 'YYYY-MM') asc) seq
	from 
		(select generate_series(0, (current_date-'2022-01-01'))+'2022-01-01'::date dt
		) tbl1 
	group by 1 
	having to_char(dt, 'YYYY-MM')!=to_char(current_date, 'YYYY-MM')
	order by 1 desc; 
	raise notice 'Calendar is populated.'; 

	var_max_seq:=(select max(seq) from data_vajapora.help_a); 
	raise notice 'Table to contain % months of data.', var_max_seq; 
	raise notice 'Table to generate from month: %', var_seq; 

	loop 
		var_mon:=(select mon from data_vajapora.help_a where seq=var_seq); 
		var_mon_start:=(select mon_start from data_vajapora.help_a where seq=var_seq)-1; 
		var_mon_end:=(select mon_end from data_vajapora.help_a where seq=var_seq)+1; 
	
		-- monthly receivers
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date>var_mon_start and event_date<var_mon_end 
			and event_name='inbox_message_received'; 

		delete from data_vajapora.monthly_inbox_rec 
		where "month"=var_mon; 
		
		insert into data_vajapora.monthly_inbox_rec
		select var_seq seq, var_mon "month", segment, count(mobile_no) merchants_rec_msg
		from 
			data_vajapora.help_b tbl1 
			
			left join 
		
			(-- segments
			select 
				mobile_no,
				max(
					case 
						when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
						when tg in('LTUCb','LTUTa') then 'LTU'
						when tg in('NT--') then 'NT'
						when tg in('NB0','NN1','NN2-6') then 'NN'
						when tg in('PSU') then 'PSU'
						when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
						when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
						else 'rest'
					end
				) segment
			from cjm_segmentation.retained_users 
			where report_date=var_mon_end-1
			group by 1
			) tbl2 using(mobile_no)
		group by 1, 2, 3;
				
		commit; 
		raise notice 'Data generated for: month %, from % to %', var_mon, var_mon_start+1, var_mon_end-1;  
		var_seq:=var_seq+1; 
		if var_seq=var_max_seq+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.monthly_inbox_rec
order by 1, 2, 3; 

select 
	"month", 
	case when segment is null then 'rest' else segment end segment, 
	sum(merchants_rec_msg) merchants_rec_msg
from data_vajapora.monthly_inbox_rec
group by 1, 2
order by 1 desc, 2 asc; 