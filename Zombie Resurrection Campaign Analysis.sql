/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=426718350
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: Data analysis: Zombie Resurrection Campaign
- Notes (if any): 
	- Firebase Cloud	ZRC210616-14
	- Portal Inapp		ZRC210616-16
*/

-- daily 
select event_date, campaign_id, request_id, tg_reached, merchants_app_opened, merchants_added_customer, merchants_added_txn
from 
	(select 
		event_date, 
		campaign_id, 
		request_id, 
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn
	from 
		(select distinct title campaign_id, id request_id
		from public.notification_bulknotificationrequest
		where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16') -- change
		) tbl1 
		
		inner join 
		
		(select request_id, mobile mobile_no
		from public.notification_bulknotificationreceiver
		) tbl2 using(request_id)
		
		inner join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-06-16' and event_date<=current_date
		) tbl3 using(mobile_no)
	group by 1, 2, 3
	) tbl1 
	
	inner join 
	
	(select 
		campaign_id, 
		request_id, 
		count(distinct tbl2.mobile_no) tg_reached
	from 
		(select distinct title campaign_id, id request_id
		from public.notification_bulknotificationrequest
		where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16') -- change
		) tbl1 
		
		inner join 
		
		(select request_id, mobile mobile_no
		from public.notification_bulknotificationreceiver
		) tbl2 using(request_id)
	group by 1, 2
	) tbl2 using(campaign_id, request_id)
where event_date is not null
order by 2, 1;  

-- cumulative
select campaign_id, request_id, tg_reached, merchants_app_opened, merchants_added_customer, merchants_added_txn
from 
	(select 
		campaign_id, 
		request_id, 
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn
	from 
		(select distinct title campaign_id, id request_id
		from public.notification_bulknotificationrequest
		where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16') -- change
		) tbl1 
		
		inner join 
		
		(select request_id, mobile mobile_no
		from public.notification_bulknotificationreceiver
		) tbl2 using(request_id)
		
		inner join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-06-16' and event_date<=current_date
		) tbl3 using(mobile_no)
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select 
		campaign_id, 
		request_id, 
		count(distinct tbl2.mobile_no) tg_reached
	from 
		(select distinct title campaign_id, id request_id
		from public.notification_bulknotificationrequest
		where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16') -- change
		) tbl1 
		
		inner join 
		
		(select request_id, mobile mobile_no
		from public.notification_bulknotificationreceiver
		) tbl2 using(request_id)
	group by 1, 2
	) tbl2 using(campaign_id, request_id)
order by 1; 
