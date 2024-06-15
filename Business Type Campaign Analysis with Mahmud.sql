/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1706788210
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 

	- extract from live who updated public.register_tallykhatauser within the last 2 days
	- push that as data_vajapora.gave_bi_type
	- rest was done by Mahmud, he has notes
	
	Business Category Update Campaign IDs:
	Portal Inapp	BCU210913-01
	Portal Inbox	BCU210913-02
	Date: Sep 13 & 14
	
	BCU210913-01	13 Sep 2021	2:00 PM		14 Sep 2021	6:00 AM
	BCU210913-02	13 Sep 2021	2:00 PM		13 Sep 2021	11:59 PM
	BCU210913-01	14 Sep 2021	9:00 AM		15 Sep 2021	6:00 AM
	BCU210913-02	14 Sep 2021	9:00 AM		14 Sep 2021	11:59 PM
	
	- for inapp, event: in_app_message_link_tap
	- for inbox, event: inbox_message_open
*/

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
	-- campaign IDs
	('BCU210913-01',
	'BCU210913-02')
group by 1, 2;

select distinct mobile mobile_no
from public.notification_bulknotificationreceiver
where request_id in(2950); -- put request_id to see if TG exists

-- from live
select mobile_no
from public.register_tallykhatauser
where 
	date(updated_at)>='2021-09-13'
	and mobile_no is not null
	and business_type is not null 
	and business_type!='';  

/* useful from here */

select count(distinct mobile_no) merchants_gave_bi_type
from 
	(-- Mahmud's given data
	select mobile_no
	from (
			select 
				distinct mobile_no
			from tallykhata.test.last_30_days_pu_no_business_type
			union
			select 
				distinct tk_mobile as mobile_no
			from tallykhata.test.potential_tallycredit_but_not_pu
		) as t1
	) tbl1 
	
	inner join 
		
	(-- see who gave BI-type
	select mobile_no
	from data_vajapora.gave_bi_type -- pushed from live
	) tbl2 using(mobile_no);

