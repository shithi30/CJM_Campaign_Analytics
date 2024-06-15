/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=207209056
- Data:
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

do $$

declare
	var_date date:='2021-08-23'; -- start date of retained campaigns
	var_start_time timestamp;
	var_end_time timestamp; 
begin
	raise notice 'New OP goes below:'; 

	-- retained campaigns launched
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select campaign_id, request_id, min(start_datetime) start_datetime, max(end_datetime) end_datetime
	from 
	    (select 
	        request_id,
	        case when schedule_time is not null then schedule_time else created_at end start_datetime, 
	        case when schedule_time is not null then schedule_time else created_at end+interval '12 hours' end_datetime
	    from public.notification_bulknotificationsendrequest
	    ) tbl1 
	
	    inner join 
	
	    (select id request_id, title campaign_id
	    from public.notification_bulknotificationrequest
	    ) tbl2 using(request_id) 
	where campaign_id in
		('CM210823-01',
		'CM210824-01',
		'CM210824-02',
		'CM210824-03',
		'CM210825-01',
		'CM210825-02',
		'CM210825-03',
		'CM210826-01',
		'CM210826-02',
		'CM210826-03',
		'CM210827-01',
		'CM210827-02',
		'CM210827-03',
		'CM210828-01',
		'CM210828-02',
		'CM210828-03',
		'CM210829-01',
		'CM210829-02',
		'CM210830-01',
		'CM210830-04',
		'CM210831-01',
		'CM210831-02',
		'CM210901-01',
		'CM210901-02')
	group by 1, 2; 
	raise notice 'Campaign details generated.'; 

	-- TG: more or less fixed
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as 
	select concat('0', mobile_no) mobile_no
	from data_vajapora.retained_today;

	truncate table data_vajapora.campaign_period_retained; -- campaign period
	truncate table data_vajapora.noncampaign_period_retained; -- noncampaign period (30 days back)
	truncate table data_vajapora.help_i; -- for users who clicked inbox after open: within 1 min

	-- day-to-day comparative analysis
	loop
		raise notice 'Generating comparative data for: %', var_date;
	
		select min(start_datetime) 
		into var_start_time
		from data_vajapora.help_a
		where date(start_datetime)=var_date;
	
		select max(end_datetime) 
		into var_end_time
		from data_vajapora.help_a
		where date(start_datetime)=var_date;
	
		/*
		-- TG: specific TG sometimes unavailable 
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as 
		select distinct mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id in(select request_id from data_vajapora.help_a where date(start_datetime)=var_date); 
		*/
		
		-- campaign period
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as 
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) event_seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_name in('app_opened', 'inbox_message_open')
				and event_timestamp>=var_start_time and event_timestamp<=var_end_time
			) tbl1 
			inner join 
			data_vajapora.help_b tbl2 using(mobile_no);
	
		drop table if exists data_vajapora.help_d; 
		create table data_vajapora.help_d as
		select 
			tbl1.mobile_no, 
			date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
			+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
			+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
			seconds_from_open_to_inbox
		from 
			data_vajapora.help_c tbl1 
			inner join 
			data_vajapora.help_c tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
		where tbl1.event_name='app_opened' and tbl2.event_name='inbox_message_open';
		
		insert into data_vajapora.campaign_period_retained
		select var_date, var_start_time, var_end_time, *, inbox_after_open-inbox_after_open_within_60_seconds inbox_after_open_over_60_seconds
		from 
			(select count(*) unique_tg_day
			from data_vajapora.help_b
			) tbl1,
			
			(select count(distinct mobile_no) inbox_after_open
			from data_vajapora.help_d
			) tbl2,
			
			(select count(distinct mobile_no) inbox_after_open_within_60_seconds
			from data_vajapora.help_d
			where seconds_from_open_to_inbox<=60
			) tbl3;
		
		-- users who clicked inbox after open: within 1 min, all campaigns combined
		insert into data_vajapora.help_i
		select *
		from 
			(select distinct mobile_no
			from data_vajapora.help_d
			where seconds_from_open_to_inbox<=60
			) tbl1 
			left join 
			data_vajapora.help_i tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
		
		-- noncampaign period (same modality, TG & timeframe, 30 days back)
		drop table if exists data_vajapora.help_f; 
		create table data_vajapora.help_f as 
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) event_seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_name in('app_opened', 'inbox_message_open')
				and event_timestamp>=var_start_time-interval '28 days' and event_timestamp<=var_end_time-interval '28 days'
			) tbl1 
			inner join 
			data_vajapora.help_b tbl2 using(mobile_no);
	
		drop table if exists data_vajapora.help_g; 
		create table data_vajapora.help_g as
		select 
			tbl1.mobile_no, 
			date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
			+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
			+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
			seconds_from_open_to_inbox
		from 
			data_vajapora.help_f tbl1 
			inner join 
			data_vajapora.help_f tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
		where tbl1.event_name='app_opened' and tbl2.event_name='inbox_message_open';
		
		insert into data_vajapora.noncampaign_period_retained
		select 
			var_date-interval '28 days', 
			var_start_time-interval '28 days', 
			var_end_time-interval '28 days', 
			*, 
			inbox_after_open-inbox_after_open_within_60_seconds inbox_after_open_over_60_seconds
		from 
			(select count(*) unique_tg_day
			from data_vajapora.help_b
			) tbl1,
			
			(select count(distinct mobile_no) inbox_after_open
			from data_vajapora.help_g
			) tbl2,
			
			(select count(distinct mobile_no) inbox_after_open_within_60_seconds
			from data_vajapora.help_g
			where seconds_from_open_to_inbox<=60
			) tbl3;
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

	/*
	-- drop auxiliary tables
	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	drop table if exists data_vajapora.help_c;
	drop table if exists data_vajapora.help_d;
	drop table if exists data_vajapora.help_f;
	drop table if exists data_vajapora.help_g;
	drop table if exists data_vajapora.help_i;
	*/
		
end $$; 

select *
from 
	(select *, 'campaign' period
	from data_vajapora.campaign_period_retained
	union 
	select *, 'noncampaign' period
	from data_vajapora.noncampaign_period_retained
	) tbl1
where unique_tg_day!=0
order by 1; 

select count(mobile_no) 
from data_vajapora.help_i;

/*
-- no TG found since 26th Sep, 2021 onwards
CM210826-01	2576	2021-08-26 11:30:46	2021-08-26 23:30:46
CM210826-01	2577	2021-08-26 11:30:56	2021-08-26 23:30:56
CM210826-01	2578	2021-08-26 11:31:04	2021-08-26 23:31:04
CM210826-01	2579	2021-08-26 11:31:12	2021-08-26 23:31:12
CM210826-01	2580	2021-08-26 11:31:19	2021-08-26 23:31:19
CM210826-01	2581	2021-08-26 11:31:25	2021-08-26 23:31:25
CM210826-02	2585	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-03	2590	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-02	2583	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-02	2587	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-02	2584	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-03	2589	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-02	2586	2021-08-26 13:58:00	2021-08-27 01:58:00
CM210826-03	2591	2021-08-26 13:58:00	2021-08-27 01:58:00

select distinct mobile mobile_no
from public.notification_bulknotificationreceiver
where request_id in(select request_id from data_vajapora.help_a where date(start_datetime)='2021-08-26');
*/
