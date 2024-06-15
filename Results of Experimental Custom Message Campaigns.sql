/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1dc2D-Xl0jzs2EuF6gr7F-WgWX_VVbSjHC8YE4pm6qPw/edit#gid=1477873441 
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
	var_date date:='2021-12-13'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.custom_message_exp 
		where event_date=var_date; 
	
		-- sequenced events of DAUs
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select id, mobile_no, event_date, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from tallykhata.tallykhata_sync_event_fact_final
		where event_date=var_date; 
			
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select tbl1.notification_id, tbl1.mobile_no, tbl3.id
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
		insert into data_vajapora.custom_message_exp
		select 
			event_date, 
			notification_id, 
			title, 
			merchants_received_message, 
			merchants_opened_message, 
			taps_on_message, 
			coalesce(open_through_inbox_merchants, 0) open_through_inbox_merchants, 
			coalesce(first_open_through_inbox_merchants, 0) first_open_through_inbox_merchants
		from 
			(select 
				event_date, 
				notification_id, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) merchants_received_message, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) merchants_opened_message, 
				count(case when event_name='inbox_message_open' then id else null end) taps_on_message 
			from data_vajapora.temp_a 
			group by 1, 2
			) tbl1 
			
			left join 
			
			(select
				notification_id, 
				count(distinct mobile_no) open_through_inbox_merchants,
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
			from data_vajapora.temp_b
			group by 1
			) tbl2 using(notification_id)
		
			inner join 
			
			(select id notification_id, title
			from public.notification_pushmessage
			) tbl3 using(notification_id); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date+1 then exit; 
		end if; 
	end loop; 
end $$; 

select 
	event_date, 
	notification_id, 
	title,
	summary, 
	case when notification_id in(2560, 2570) then 20000 else null end merchants_sent_message, 
	merchants_received_message, 
	merchants_opened_message, 
	taps_on_message, 
	open_through_inbox_merchants, 
	first_open_through_inbox_merchants
from 
	data_vajapora.custom_message_exp tbl1 
	inner join 
	(select id notification_id, summary
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	notification_id in(2560, 2570, 2573, 2574)
	and event_date<current_date; 
