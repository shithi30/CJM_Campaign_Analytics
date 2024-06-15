/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.cjm_inbox_analyses, data_vajapora.cjm_inapp_analyses
- File: 
- Path: http://localhost:8888/tree/CJM%20for%20Automation
- Document/Presentation: 
- Email thread: CJM Python Scripts
- Notes (if any): 
*/

/* for Inbox */
drop table if exists data_vajapora.cjm_inbox_analyses;
create table data_vajapora.cjm_inbox_analyses as
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00) ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	other_hrs_trt/(other_hrs_users*120.00) ibx_open_plus_120_mins_trt_rate,
	(ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00))/(other_hrs_trt/(other_hrs_users*120.00)) growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
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
	and other_hrs_users!=0
order by 14 desc; 


/* for inapp */
drop table if exists data_vajapora.cjm_inapp_analyses;
create table data_vajapora.cjm_inapp_analyses as
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, iap_open_users, avg_view_time_sec, 
	iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
	iap_open_plus_20_mins_trt/(iap_open_plus_20_mins_trt_users*20.00) iap_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	other_hrs_trt/(other_hrs_users*120.00) iap_open_plus_120_mins_trt_rate,
	(iap_open_plus_20_mins_trt/(iap_open_plus_20_mins_trt_users*20.00))/(other_hrs_trt/(other_hrs_users*120.00)) growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from 
	(select 
		request_id, 
		iap_open_users, avg_view_time_sec, 
		iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
		other_hrs_trt, other_hrs_users
	from tallykhata.inapp_5_10_20_60_mins_analysis
	) tbl1 
	
	inner join 
	
	(select request_id, campaign_id, tg_set, channel, message, min(start_date) start_date
	from data_vajapora.analyzable_inapp_campaigns
	group by 1, 2, 3, 4, 5
	) tbl2 using(request_id)
	
	inner join 
	
	(select request_id, count(mobile) tg_size
	from public.notification_bulknotificationreceiver
	group by 1 
	) tbl3 using(request_id)
where 	
	iap_open_plus_20_mins_trt_users!=0
	and other_hrs_users!=0
order by 14 desc; 

/*
select *
from data_vajapora.cjm_inapp_analyses;

select *
from data_vajapora.cjm_inbox_analyses;
*/

