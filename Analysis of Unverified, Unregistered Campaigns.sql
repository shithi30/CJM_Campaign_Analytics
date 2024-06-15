/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1479832088
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=635434491
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): The campaigns have overall caused a ~40% growth in registration compared to noncampaign period. 
*/

select 
	device_id_reg_date, 
	count(tbl1.device_id) devices_registered_with_app_installed, 
	count(case when tbl3.device_id is not null then tbl1.device_id else null end) devices_registered_with_app_installed_unverified,
	count(case when tbl3.device_id is null then tbl1.device_id else null end) devices_registered_with_app_installed_unregistered
from 
	(select device_id, date(created_at) device_id_create_date
	from public.notification_fcmtoken 
	where app_status='ACTIVE'
	) tbl1 
	
	left join 
	
	(select device_id, min(date(created_at)) device_id_reg_date 
	from public.registered_users
	group by 1
	) tbl2 using(device_id)
	
	left join 
		
	(select device_id, date(created_at) device_id_unv_date
	from public.register_unverifieduserapp
	) tbl3 using(device_id)
where 
	device_id_create_date<device_id_reg_date
	and device_id_reg_date>'2022-02-24'::date
group by 1
order by 1; 
