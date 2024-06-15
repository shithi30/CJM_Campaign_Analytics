/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=506390613
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): second table
*/

-- install/uninstall info
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no, device_id, 
	app_status, app_status_created_at, 
	device_status, device_status_created_at
from 
	(select device_id, created_at app_status_created_at, app_status
	from public.notification_fcmtoken 
	) tbl1 
	
	inner join 
	
	(select id, device_id, mobile mobile_no, created_at device_status_created_at, device_status
	from public.registered_users 
	) tbl2 using(device_id)
	
	inner join 

	(select mobile mobile_no, max(id) id
	from public.registered_users 
	group by 1
	) tbl3 using(mobile_no, id); 

-- campaigns with targeted merchants
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select campaign_id, request_id, mobile_no
from 
	(select distinct title campaign_id, id request_id
	from public.notification_bulknotificationrequest
	where title in('RTG210624-10', 'RTG210624-01')
	) tbl1 
	
	inner join 
	
	(select request_id, mobile mobile_no
	from public.notification_bulknotificationreceiver
	) tbl2 using(request_id);
		
-- install/uninstall info of TG
select current_date app_status_snapshot_date, *, tg_reached-uninstalled-installed_and_using_on_device-installed_but_not_using_on_device untraced
from 
	(select 
		campaign_id,
		count(distinct tbl1.mobile_no) tg_reached,
		count(distinct case when app_status='UNINSTALLED' then mobile_no else null end) uninstalled,
		count(distinct case when app_status='ACTIVE' and device_status='active' then mobile_no else null end) installed_and_using_on_device,
		count(distinct case when app_status='ACTIVE' and device_status='inactive' then mobile_no else null end) installed_but_not_using_on_device
	from 
		data_vajapora.help_b tbl1
		left join 
		data_vajapora.help_a tbl2 using(mobile_no)
	group by 1
	) tbl1; 