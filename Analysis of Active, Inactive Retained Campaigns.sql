/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1103597369
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	- guidelines (changeable, consult): https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit#gid=1112370411
*/

do $$ 

declare 
	var_max_seq int; 
	i int:=1;
	var_campaign_date date; 
begin 
	raise notice 'New OP goes below:'; 

	-- details of the campaigns to analyze
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select *, dense_rank() over(order by campaign_id asc) seq
	from 
		(select campaign_id, request_id, min(start_datetime) start_datetime, max(end_datetime) end_datetime
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
			(/*'CM210823-01', 'CM210824-01', 'CM210824-02', 'CM210824-03', -- main
			-- CJMs
			'C210823-06',
			'C210823-07',
			'C210823-05',
			'C210815-05',
			'C210815-06',
			'C210815-07',
			'C210814-05',
			'C210814-06',
			'C210814-07',
			'C210622-05',
			'C210622-06',
			'C210622-07',
			'C210622-08',
			'C210701-05',
			'C210701-06',
			'C210701-07'*/
			'CM210825-01', 'CM210825-02', 'CM210825-03'
			)
		group by 1, 2
		) tbl1; 

	-- count of campaigns
	select max(seq) max_seq 
	into var_max_seq
	from data_vajapora.help_a; 
	raise notice 'Campaigns to analyze: %', var_max_seq; 

	-- generate data afresh for all the campaigns
	truncate table data_vajapora.retained_campaign_results;
	loop
		raise notice 'Generating data for campaign: %', i; 
	
		-- for a single campaign: date of campaign
		select min(date(start_datetime))
		into var_campaign_date
		from data_vajapora.help_a 
		where seq=i; 
			
		-- for a single campaign: analysis 
		insert into data_vajapora.retained_campaign_results
		select *
		from 
			(-- id, start/end times
			select campaign_id, min(request_id) request_id, min(start_datetime) start_datetime, min(end_datetime) end_datetime
			from data_vajapora.help_a 
			where seq=i
			group by 1 
			) tbl1, 
			
			(-- users initially targetted
			select sum(receiver_count) receiver_count
			from public.notification_bulknotificationsendrequest
			where request_id in(select request_id from data_vajapora.help_a where seq=i)
			) tbl2,
			
			(-- users actually reached
			select count(distinct mobile) users_rec
			from public.notification_bulknotificationreceiver
			where request_id in(select request_id from data_vajapora.help_a where seq=i)
			) tbl3,
			
			(-- count of users who clicked
			select count(distinct mobile_no) users_clicked
			from 
				(select mobile mobile_no
				from public.notification_bulknotificationreceiver
				where request_id in(select request_id from data_vajapora.help_a where seq=i)
				) tbl1 
				
				inner join 
				
				(select mobile_no
				from tallykhata.tallykhata_sync_event_fact_final 
				where 
					event_timestamp>=(select min(start_datetime) from data_vajapora.help_a where seq=i)
					and event_timestamp<=(select min(end_datetime) from data_vajapora.help_a where seq=i)
					and event_name='inbox_message_open'
				) tbl2 using(mobile_no)
			) tbl4,
			
			(-- nontrt_to_trt users
			select count(mobile_no) nontrt_to_trt
			from 
				(select distinct mobile_no
				from 
					tallykhata.tallykhata_transacting_user_date_sequence_final tbl1 
					inner join
					(select mobile mobile_no
					from public.notification_bulknotificationreceiver
					where request_id in(select request_id from data_vajapora.help_a where seq=i)
					) tbl2 using(mobile_no)
				where created_datetime=var_campaign_date
				) tbl1 
				
				left join 
				
				(select distinct mobile_no
				from tallykhata.tallykhata_transacting_user_date_sequence_final 
				where created_datetime=var_campaign_date-1
				) tbl2 using(mobile_no) 
			where tbl2.mobile_no is null
			) tbl5,
			
			(-- inactive_to_active users
			select count(mobile_no) inactive_to_active
			from 
				(select distinct mobile_no
				from 
					tallykhata.tallykhata_user_date_sequence_final tbl1 
					inner join
					(select mobile mobile_no
					from public.notification_bulknotificationreceiver
					where request_id in(select request_id from data_vajapora.help_a where seq=i)
					) tbl2 using(mobile_no)
				where event_date=var_campaign_date
				) tbl1 
				
				left join 
				
				(select distinct mobile_no
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>=to_char(var_campaign_date, 'YYYY-MM-01')::date and event_date<var_campaign_date
				) tbl2 using(mobile_no) 
			where tbl2.mobile_no is null
			) tbl6,
			
			(-- inactive_to_active users of last 7 days to today
			select count(mobile_no) inactive_to_active_last_7_days
			from 
				(select distinct mobile_no
				from 
					tallykhata.tallykhata_user_date_sequence_final tbl1 
					inner join
					(select mobile mobile_no
					from public.notification_bulknotificationreceiver
					where request_id in(select request_id from data_vajapora.help_a where seq=i)
					) tbl2 using(mobile_no)
				where event_date=var_campaign_date
				) tbl1 
				
				left join 
				
				(select distinct mobile_no
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>=var_campaign_date-7 and event_date<var_campaign_date
				) tbl2 using(mobile_no) 
			where tbl2.mobile_no is null
			) tbl7; 
		
		i:=i+1;
		if i=var_max_seq+1 then exit;
		end if;
	end loop;
	
end $$; 

select *
from data_vajapora.retained_campaign_results
order by campaign_id desc; 

/*
-- test
CM210824-02	2530	2021-08-24 13:58:00	2021-08-25 01:58:00	6
CM210824-02	2531	2021-08-24 13:58:00	2021-08-25 01:58:00	6
CM210824-02	2532	2021-08-24 13:58:00	2021-08-25 01:58:00	6
CM210824-02	2533	2021-08-24 13:58:00	2021-08-25 01:58:00	6

-- inactive_to_active users
select count(tbl1.mobile_no) inactive_to_active
from 
	(select distinct mobile_no
	from 
		tallykhata.tallykhata_user_date_sequence_final tbl1 
		inner join
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id in(2530, 2531, 2532, 2533)
		) tbl2 using(mobile_no)
	where event_date='2021-08-24'
	) tbl1 
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>='2021-08-01' and event_date<'2021-08-24'
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no) 
where tbl2.mobile_no is null;   
*/
