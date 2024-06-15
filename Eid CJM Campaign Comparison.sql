/*
- Viz: https://docs.google.com/spreadsheets/d/1kgO7_kkg5CFwRy4GNHLTnBT7ssTyhdygsFi5gH3Cduc/edit#gid=1054424251
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 

The campaigns for which results are absent, 
- Either, 3 days have not passed after they ended
- Or, they gave 0 results

*/

Kurbani Eid - July 2021: 
E210717-04
E210717-05
E210718-04
E210719-04
E210719-05
E210719-06
E210720-03
E210720-04
E210720-05
E210721-03
E210722-05

Ramadan Eid - May 2021: 
L210510-05
L210510-06
L210511-09
L210511-10
L210511-11
L210511-12
L210512-09
L210513-03
L210514-05
L210514-06
L210515-09
L210515-10
L210515-11
L210515-12
L210515-13
L210516-09
L210516-10
L210516-11
L210516-12
L210516-13

Require data:
1. Reach count
2. Open count
3. Activity user count
4. Others that you messure

select *
from data_vajapora.analyzable_inbox_campaigns; 

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00) ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	other_hrs_trt/(other_hrs_users*120.00) ibx_open_plus_120_mins_trt_rate,
	(ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00))/(other_hrs_trt/(other_hrs_users*120.00)) growth_rate,
	message, 
	request_id
from 
	(select 
		request_id, 
		ibx_open_users, avg_view_time_sec, 
		ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
		other_hrs_trt, other_hrs_users
	from tallykhata.inbox_5_10_20_60_mins_analysis
	) tbl1 
	
	inner join 
	
	(select request_id, campaign_id, tg_set, channel, message, min(start_date) start_date
	from data_vajapora.analyzable_inbox_campaigns
	group by 1, 2, 3, 4, 5
	) tbl2 using(request_id)
	
	inner join 
	
	(select request_id, count(mobile) tg_size
	from public.notification_bulknotificationreceiver
	group by 1 
	) tbl3 using(request_id)
where 	
	ibx_open_plus_20_mins_trt_users!=0
	and other_hrs_users!=0;

-- all mature campaigns validly (excluding div by 0 cases) analyzed
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	ibx_open_plus_120_mins_trt_rate,
	growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from data_vajapora.help_a tbl1
order by start_date asc;

-- Where did these go?
2251	E210717-05
2252	E210717-05
2265	E210718-04
2266	E210718-04

-- 0s are obtained, so no results could be shown
select 
	request_id, 
	ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	other_hrs_trt, other_hrs_users
from tallykhata.inbox_5_10_20_60_mins_analysis
order by 1 asc;

-- Where did 20, 21, 22 go? Only those that ended within 22th are shown.
 
-- Then where did 20 go?
2308	E210720-05
2309	E210720-05

-- 0s are obtained, so no results could be shown
select 
	request_id, 
	ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	other_hrs_trt, other_hrs_users
from tallykhata.inbox_5_10_20_60_mins_analysis
order by 1 asc;
