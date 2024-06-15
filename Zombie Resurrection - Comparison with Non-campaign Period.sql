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

/* data to compare with */

-- registered more than 2 months back, present: 2021-04-14
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_number mobile_no, date(created_at) reg_date 
from public.register_usermobile 
where date(created_at)<='2021-04-14'::date-60; 

-- has activity in the last 2 months, present: 2021-04-14
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no 
from tallykhata.event_transacting_fact 
where event_date>'2021-04-14'::date-60 and event_date<='2021-04-14'::date; 

-- registered more than 2 months back, but has no activity in the last 2 months, present: 2021-04-14
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as 
select mobile_no
from 
	data_vajapora.help_a tbl1 
	left join 
	data_vajapora.help_b tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

-- merchants with their last active date and total days of activity, present: 2021-04-14
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as 
select mobile_no, max(event_date) last_active_date, count(distinct event_date) total_active_days 
from tallykhata.event_transacting_fact 
where event_date<='2021-04-14'
group by 1; 

-- registered more than 2 months back, has no activity in the last 2 months and has been active at least 3 days prior to that, present: 2021-04-14
select mobile_no
from 
	data_vajapora.help_c tbl1
	inner join 
	data_vajapora.help_d tbl2 using(mobile_no)
where total_active_days>=3; 

/* the comparison */

-- campaign period
select 
	count(distinct tbl2.mobile_no) tg_reached,
	count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
	count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
	count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,
	'campaign' period
from 
	(select distinct title campaign_id, id request_id
	from public.notification_bulknotificationrequest
	where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16')
	) tbl1 
	
	inner join 
	
	(select request_id, mobile mobile_no
	from public.notification_bulknotificationreceiver
	) tbl2 using(request_id)
	
	left join 
	
	(select mobile_no, entry_type, event_name, event_date
	from tallykhata.event_transacting_fact 
	where 
		(event_name='app_opened' or entry_type=1) 
		and event_date>='2021-06-16' and event_date<=current_date-1 -- change
	) tbl3 using(mobile_no)

union all

-- non-campaign period
select 
	count(distinct tbl2.mobile_no) tg_reached,
	count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
	count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
	count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,
	'non-campaign' period
from 
	(select mobile_no
	from 
		data_vajapora.help_c tbl1
		inner join 
		data_vajapora.help_d tbl2 using(mobile_no)
	where total_active_days>=3
	) tbl2
	
	left join 
	
	(select mobile_no, entry_type, event_name, event_date
	from tallykhata.event_transacting_fact 
	where 
		(event_name='app_opened' or entry_type=1) 
		and event_date>='2021-04-16' and event_date<='2021-04-20' -- change
	) tbl3 using(mobile_no); 
