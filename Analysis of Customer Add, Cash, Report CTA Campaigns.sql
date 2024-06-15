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
*/


/* for customer add

C210325-04 (customer ad)
C210604-04 (customer ad)
C210814-04 (customer ad)
BC210706-01
BC210707-01
BC210708-01
BC210706-02
BC210707-02
BC210708-02
*/

do $$ 

declare 
	var_campaign_id varchar:='BC210708-02';
	var_start_datetime timestamp;
	var_end_datetime timestamp;
	var_request_id int;

	var_event_name varchar:='in_app_message_link_tap';
	var_txn_type varchar:='Add Customer'; 

	var_receiver_count int;
	var_users_rec int;
	var_users_clicked int;
	var_users_clicked_pct float;
	var_users_did_activity int; 
	var_users_did_activity_pct float; 

	var_inc int:=0; 
begin 
	raise notice 'New OP goes below:';

	-- extract start/end times, request ids of campaigns
	select campaign_id, min(request_id) request_id, min(start_datetime) start_datetime, max(end_datetime) end_datetime 
	into var_campaign_id, var_request_id, var_start_datetime, var_end_datetime
	from 
	    (select 
	        request_id,
	        schedule_time start_datetime, 
	        schedule_time+interval '24 hours' end_datetime,
	        date(schedule_time) start_date
	    from public.notification_bulknotificationsendrequest
	    ) tbl1 
	
	    inner join 
	
	    (select id request_id, title campaign_id
	    from public.notification_bulknotificationrequest
	    ) tbl2 using(request_id) 
	where campaign_id=var_campaign_id
	group by 1; 
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users initially targetted
	select request_id, receiver_count
	into var_request_id, var_receiver_count
	from public.notification_bulknotificationsendrequest
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users who were actually shot the message
	select count(distinct mobile) users_rec
	into var_users_rec
	from public.notification_bulknotificationreceiver
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 
	
	-- get count of users who clicked
	select count(distinct mobile_no) users_clicked
	into var_users_clicked
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		(select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_timestamp>=var_start_datetime and event_timestamp<=var_end_datetime
			and event_name=var_event_name
		) tbl2 using(mobile_no);
	var_users_clicked_pct=var_users_clicked*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- get count of users who did the activity
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where 
		created_timestamp>=var_start_datetime and created_timestamp<=var_end_datetime
		and txn_type=var_txn_type;
	var_inc:=var_inc+1; raise notice '%', var_inc; 
		
	select count(distinct mobile_no) users_did_activity
	into var_users_did_activity
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no);
	var_users_did_activity_pct=var_users_did_activity*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- print out, save values
	raise notice '% % % % % % % % % %', 
	var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked, var_users_clicked_pct,
	var_users_did_activity, var_users_did_activity_pct;

	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b
		(campaign_id varchar,
		start_datetime timestamp, 
		end_datetime timestamp, 
		request_id int, 
		receiver_count int, 
		users_rec int, 
		users_clicked int, 
		users_clicked_pct float,
		users_did_activity int,
		users_did_activity_pct float
		);
	
	insert into data_vajapora.help_b values
	(var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked, var_users_clicked_pct,
	var_users_did_activity, var_users_did_activity_pct
	);
	
end $$; 

select *
from data_vajapora.help_b; 




/* for cash sale

-- C210605-04
*/

do $$ 

declare 
	var_campaign_id varchar:='BC210708-02';
	var_start_datetime timestamp;
	var_end_datetime timestamp;
	var_request_id int;

	var_event_name varchar:='cash';
	var_txn_type varchar:='CASH_SALE'; 

	var_receiver_count int;
	var_users_rec int;
	var_users_clicked int;
	var_users_clicked_pct float;
	var_users_did_activity int; 
	var_users_did_activity_pct float; 

	var_inc int:=0; 
begin 
	raise notice 'New OP goes below:';

	-- extract start/end times, request ids of campaigns
	select campaign_id, min(request_id) request_id, min(start_datetime) start_datetime, max(end_datetime) end_datetime 
	into var_campaign_id, var_request_id, var_start_datetime, var_end_datetime
	from 
	    (select 
	        request_id,
	        schedule_time start_datetime, 
	        schedule_time+interval '24 hours' end_datetime,
	        date(schedule_time) start_date
	    from public.notification_bulknotificationsendrequest
	    ) tbl1 
	
	    inner join 
	
	    (select id request_id, title campaign_id
	    from public.notification_bulknotificationrequest
	    ) tbl2 using(request_id) 
	where campaign_id=var_campaign_id
	group by 1; 
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users initially targetted
	select request_id, receiver_count
	into var_request_id, var_receiver_count
	from public.notification_bulknotificationsendrequest
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users who were actually shot the message
	select count(distinct mobile) users_rec
	into var_users_rec
	from public.notification_bulknotificationreceiver
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 
	
	-- get count of users who clicked
	select count(distinct mobile_no) users_clicked
	into var_users_clicked
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		(select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_timestamp>=var_start_datetime and event_timestamp<=var_end_datetime
			and event_name=var_event_name
		) tbl2 using(mobile_no);
	var_users_clicked_pct=var_users_clicked*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- get count of users who did the activity
	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where 
		created_timestamp>=var_start_datetime and created_timestamp<=var_end_datetime
		and txn_type=var_txn_type;
	var_inc:=var_inc+1; raise notice '%', var_inc; 
		
	select count(distinct mobile_no) users_did_activity
	into var_users_did_activity
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no);
	var_users_did_activity_pct=var_users_did_activity*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- print out, save values
	raise notice '% % % % % % % % % %', 
	var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked, var_users_clicked_pct,
	var_users_did_activity, var_users_did_activity_pct;

	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b
		(campaign_id varchar,
		start_datetime timestamp, 
		end_datetime timestamp, 
		request_id int, 
		receiver_count int, 
		users_rec int, 
		users_clicked int, 
		users_clicked_pct float,
		users_did_activity int,
		users_did_activity_pct float
		);
	
	insert into data_vajapora.help_b values
	(var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked, var_users_clicked_pct,
	var_users_did_activity, var_users_did_activity_pct
	);
	
end $$; 

select *
from data_vajapora.help_b; 




/* for report 

C210701-08 (Report)
C210703-07 (Report)
*/

do $$ 

declare 
	var_campaign_id varchar:='C210703-07';
	var_start_datetime timestamp;
	var_end_datetime timestamp;
	var_request_id int;

	var_event_name_1 varchar:='report_download_customer_list';
	var_event_name_2 varchar:='report_download_customer_detail'; 

	var_receiver_count int;
	var_users_rec int;

	var_users_clicked_event_1 int;
	var_users_clicked_event_1_pct float;
	var_users_clicked_event_2 int;
	var_users_clicked_event_2_pct float;

	var_inc int:=0; 
begin 
	raise notice 'New OP goes below:';

	-- extract start/end times, request ids of campaigns
	select campaign_id, min(request_id) request_id, min(start_datetime) start_datetime, max(end_datetime) end_datetime 
	into var_campaign_id, var_request_id, var_start_datetime, var_end_datetime
	from 
	    (select 
	        request_id,
	        schedule_time start_datetime, 
	        schedule_time+interval '24 hours' end_datetime,
	        date(schedule_time) start_date
	    from public.notification_bulknotificationsendrequest
	    ) tbl1 
	
	    inner join 
	
	    (select id request_id, title campaign_id
	    from public.notification_bulknotificationrequest
	    ) tbl2 using(request_id) 
	where campaign_id=var_campaign_id
	group by 1; 
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users initially targetted
	select request_id, receiver_count
	into var_request_id, var_receiver_count
	from public.notification_bulknotificationsendrequest
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- users who were actually shot the message
	select count(distinct mobile) users_rec
	into var_users_rec
	from public.notification_bulknotificationreceiver
	where request_id=var_request_id;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- get count of users who clicked: first event
	select count(distinct mobile_no) users_clicked
	into var_users_clicked_event_1
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		(select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_timestamp>=var_start_datetime and event_timestamp<=var_end_datetime
			and event_name=var_event_name_1
		) tbl2 using(mobile_no);
	var_users_clicked_event_1_pct=var_users_clicked_event_1*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- get count of users who clicked: second event
	select count(distinct mobile_no) users_clicked
	into var_users_clicked_event_2
	from 
		(select mobile mobile_no
		from public.notification_bulknotificationreceiver
		where request_id=var_request_id
		) tbl1 
		
		inner join 
		
		(select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_timestamp>=var_start_datetime and event_timestamp<=var_end_datetime
			and event_name=var_event_name_2
		) tbl2 using(mobile_no);
	var_users_clicked_event_2_pct=var_users_clicked_event_2*1.00/var_users_rec;
	var_inc:=var_inc+1; raise notice '%', var_inc; 

	-- print out, save values
	raise notice '% % % % % % % % % %', 
	var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked_event_1, var_users_clicked_event_1_pct,
	var_users_clicked_event_2, var_users_clicked_event_2_pct; 

	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b
		(campaign_id varchar,
		start_datetime timestamp, 
		end_datetime timestamp, 
		request_id int, 
		receiver_count int, 
		users_rec int, 
		users_clicked_event_1 int, 
		users_clicked_event_1_pct float,
		users_clicked_event_2 int, 
		users_clicked_event_2_pct float
		);
	
	insert into data_vajapora.help_b values
	(var_campaign_id, var_start_datetime, var_end_datetime, var_request_id, 
	var_receiver_count, var_users_rec, 
	var_users_clicked_event_1, var_users_clicked_event_1_pct,
	var_users_clicked_event_2, var_users_clicked_event_2_pct
	);
	
end $$; 

select *
from data_vajapora.help_b; 


