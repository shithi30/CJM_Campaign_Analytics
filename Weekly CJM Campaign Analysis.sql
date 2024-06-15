/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit?pli=1#gid=1175269363
- Function: 
- Table:
- Instructions: 
- Format: https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit?pli=1#gid=1560807016
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Weekly CJM Impact Analysis Template!
- Notes (if any): 
*/

-- for inbox
do $$

declare 
	var_start_date date:='2021-09-04'::date;
	var_end_date date:=var_start_date+7;
begin 
	raise notice 'New OP goes below:'; 

	-- all campaigns
	drop table if exists data_vajapora.campaign_help_a; 
	create table data_vajapora.campaign_help_a as
	select request_id, campaign_id, channel, created_at, date(start_datetime) schedule_date, start_datetime, end_datetime, regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message, row_number() over(order by request_id asc) seq
	from 
	    (select distinct request_id 
	    from test.campaign_data_v2
	    ) tbl1
	
	    inner join
	
	    (select 
	        request_id,
	        case when schedule_time is not null then schedule_time else created_at end start_datetime, 
	        case when schedule_time is not null then schedule_time else created_at end+interval '12 hours' end_datetime
	    from public.notification_bulknotificationsendrequest
	    ) tbl2 using(request_id)
	
	    inner join 
	
	    (select title campaign_id, id request_id, created_at, message_id
	    from public.notification_bulknotificationrequest
	    ) tbl3 using(request_id)
	
	    inner join 
	
	    (select campaign_id_title campaign_id, channel
	    from test.campaign_info
	    ) tbl4 using(campaign_id)
	
	    left join 
	
	    (select id message_id, summary message
	    from public.notification_pushmessage
	    ) tbl5 using(message_id)
	where channel in('Portal Inbox', 'Portal Inapp');  
	
	-- generating weekly results one-by-one
	loop
		delete from data_vajapora.weekly_inbox_analysis
		where date_range=concat(var_start_date, ' to ', var_end_date-1); 
	
		-- sequenced events in the week
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select mobile_no, event_date, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date>=var_start_date and event_date<var_end_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		
		-- all metrics to show
		insert into data_vajapora.weekly_inbox_analysis
		select concat(var_start_date, ' to ', var_end_date-1) date_range, *
		from 
			(select count(distinct message) messages_sent
			from data_vajapora.campaign_help_a
			where 
				channel='Portal Inbox'
				and schedule_date>=var_start_date and schedule_date<var_end_date
			) tbl4,
		
			(select count(distinct mobile) tg_size
			from test.campaign_data_v2
			where request_id in
				(select request_id
				from data_vajapora.campaign_help_a
				where 
					channel='Portal Inbox'
					and schedule_date>=var_start_date and schedule_date<var_end_date
				) 
			) tbl0,
		
			(select 
				count(distinct tbl1.mobile_no) opened_app_through_inbox,
				count(distinct tbl1.mobile_no)-count(distinct tbl3.mobile_no) opened_app_through_inbox_and_no_txn,
				count(distinct tbl3.mobile_no) opened_app_through_inbox_and_did_txn
			from 
				data_vajapora.help_a tbl1
				inner join 
				data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				left join 
				(select mobile_no, created_datetime 
				from tallykhata.tallykhata_transacting_user_date_sequence_final
				where created_datetime>=var_start_date and created_datetime<var_end_date
				) tbl3 on(tbl2.mobile_no=tbl3.mobile_no and tbl2.event_date=tbl3.created_datetime)
			where 
				tbl1.event_name='inbox_message_open'
				and tbl2.event_name='app_opened'
			) tbl1,
			
			(select count(distinct mobile_no) merchants_opened_inbox_message, count(*) total_inbox_opens
			from data_vajapora.help_a
			where event_name='inbox_message_open'
			) tbl2; 
		
		raise notice 'Data generated from % to %', var_start_date, var_end_date-1;
		var_start_date:=var_start_date+7;
		var_end_date:=var_end_date+7;
		if var_end_date>current_date then exit;
		end if; 
	end loop; 
	
end $$; 

select *
from data_vajapora.weekly_inbox_analysis;

-- for inapp
do $$

declare 
	var_start_date date:='2021-09-04'::date;
	var_end_date date:=var_start_date+7;
begin 
	raise notice 'New OP goes below:'; 

	-- all campaigns
	drop table if exists data_vajapora.campaign_help_a; 
	create table data_vajapora.campaign_help_a as
	select request_id, campaign_id, channel, created_at, date(start_datetime) schedule_date, start_datetime, end_datetime, regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message, row_number() over(order by request_id asc) seq
	from 
	    (select distinct request_id 
	    from test.campaign_data_v2
	    ) tbl1
	
	    inner join
	
	    (select 
	        request_id,
	        case when schedule_time is not null then schedule_time else created_at end start_datetime, 
	        case when schedule_time is not null then schedule_time else created_at end+interval '12 hours' end_datetime
	    from public.notification_bulknotificationsendrequest
	    ) tbl2 using(request_id)
	
	    inner join 
	
	    (select title campaign_id, id request_id, created_at, message_id
	    from public.notification_bulknotificationrequest
	    ) tbl3 using(request_id)
	
	    inner join 
	
	    (select campaign_id_title campaign_id, channel
	    from test.campaign_info
	    ) tbl4 using(campaign_id)
	
	    left join 
	
	    (select id message_id, summary message
	    from public.notification_pushmessage
	    ) tbl5 using(message_id)
	where channel in('Portal Inbox', 'Portal Inapp');  
	
	-- generating weekly results one-by-one
	loop
		delete from data_vajapora.weekly_inapp_analysis
		where date_range=concat(var_start_date, ' to ', var_end_date-1); 
	
		-- events in the week
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select mobile_no, event_date, event_name
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			event_date>=var_start_date and event_date<var_end_date
			and event_name in('in_app_message_close', 'in_app_message_link_tap'); 
			
		-- all metrics to show
		insert into data_vajapora.weekly_inapp_analysis
		select concat(var_start_date, ' to ', var_end_date-1) date_range, *
		from 
			(select count(distinct message) messages_sent
			from data_vajapora.campaign_help_a
			where 
				channel='Portal Inapp'
				and schedule_date>=var_start_date and schedule_date<var_end_date
			) tbl1,
		
			(select count(distinct mobile) tg_size
			from test.campaign_data_v2
			where request_id in
				(select request_id
				from data_vajapora.campaign_help_a
				where 
					channel='Portal Inapp'
					and schedule_date>=var_start_date and schedule_date<var_end_date
				) 
			) tbl2,
		
			(select count(distinct mobile_no) merchants_dismissed_cta
			from data_vajapora.help_a
			where event_name='in_app_message_close'
			) tbl3,
			
			(select count(distinct mobile_no) merchants_clicked_cta, count(*) total_cta_clicks
			from data_vajapora.help_a
			where event_name='in_app_message_link_tap'
			) tbl4; 
		
		raise notice 'Data generated from % to %', var_start_date, var_end_date-1;
		var_start_date:=var_start_date+7;
		var_end_date:=var_end_date+7;
		if var_end_date>current_date then exit;
		end if; 
	end loop; 
	
end $$; 

select *
from data_vajapora.weekly_inapp_analysis;
