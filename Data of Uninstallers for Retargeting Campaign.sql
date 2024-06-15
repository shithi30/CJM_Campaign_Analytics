/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.uninstalled_users_11_Aug
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Re: Request for User Data
- Notes (if any): 
*/

-- in live 
select mobile_no
from 
	(select 
		mobile_no,
		row_number() over(order by mobile_no) seq
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
		) tbl3 using(mobile_no, id)
	where app_status='UNINSTALLED'
	) tbl1
where 
	seq>0 and seq<=700000	-- alter here
	-- seq>700000
	
-- import 2 .csv files to DWH via Python
	
-- in DWH 
-- drop table if exists data_vajapora.uninstalled_users_11_Aug;
create table data_vajapora.uninstalled_users_11_Aug as
select concat('0', mobile_no::varchar) mobile_no
from data_vajapora."uninstalled_users_11_Aug_1"
union all
select concat('0', mobile_no::varchar) mobile_no
from data_vajapora."uninstalled_users_11_Aug_2"; 
select *
from data_vajapora.uninstalled_users_11_Aug;
