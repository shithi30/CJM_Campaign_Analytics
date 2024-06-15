/*
- Viz: 304.png
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=0
- Table:
- File: 
- Email thread: Referral Campaign Report
- Notes (if any): 
*/

-- successful referrals 
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select distinct 
	refer_datetime reg_datetime, 
	referrer_user_id, referrer_mobile_no,
	referred_user_id, referred_mobile_no
from 
	(select created_at refer_datetime, referrer_mobile_id referrer_user_id, user_mobile_id referred_user_id
	from public.register_referral
	) tbl1 
	
	inner join 
	
	(select mobile referrer_mobile_no, tallykhata_user_id referrer_user_id
	from public.registered_users
	) tbl2 using(referrer_user_id)
	
	inner join 
	
	(select mobile referred_mobile_no, tallykhata_user_id referred_user_id
	from public.registered_users
	) tbl3 using(referred_user_id); 

-- campaign details 
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select request_id, campaign_id, start_date, end_date, tg_reached
from 
    (select request_id, min(date(schedule_time)) start_date, max(date(schedule_time)) end_date
    from public.notification_bulknotificationsendrequest
    group by 1
    ) tbl1 

    inner join 

    (select id request_id, title campaign_id
    from public.notification_bulknotificationrequest
    where title like '%ORC%'
    ) tbl2 using(request_id)
    
    inner join 

	(select request_id, count(mobile) tg_reached
	from public.notification_bulknotificationreceiver
	group by 1 
	) tbl3 using(request_id); 

-- campaign results
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	tbl2.*, 
	count(distinct referrer_mobile_no) referrers, 
	count(distinct referred_mobile_no) referred_regs, 
	count(distinct case when event_date>=start_date then pk_id else null end) refer_clicks,
	count(distinct case when event_date>=start_date then tbl4.mobile_no else null end) refer_clickers
from 
	(select request_id, mobile mobile_no
	from public.notification_bulknotificationreceiver
	) tbl1
	
	inner join 
	
	data_vajapora.help_c tbl2 using(request_id)
	
	left join 
	
	(select referrer_mobile_no, referred_mobile_no 
	from data_vajapora.help_a
	) tbl3 on(tbl1.mobile_no=tbl3.referrer_mobile_no)
	
	left join 
	
	(select mobile_no, event_date, pk_id
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='refer'
	) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
group by 1, 2, 3, 4, 5;
select *
from data_vajapora.help_b;
