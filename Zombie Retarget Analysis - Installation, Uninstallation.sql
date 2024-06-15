/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=506390613
- Data: 
- Function: data_vajapora.fn_zombie_install_tracking_results()
- Table: data_vajapora.zombie_install_tracking_results
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_zombie_install_tracking_results()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Tracking of zombies' installations/uninstallations
Auxiliary data table(s) : none
Target data table(s)    : data_vajapora.zombie_install_tracking_results
*/

declare

begin
	
	-- delete today's data if already generated
	delete from data_vajapora.zombie_install_tracking_results
	where app_status_snapshot_date=current_date; 

	-- generate and insert today's data
	insert into data_vajapora.zombie_install_tracking_results
	select current_date app_status_snapshot_date, *, tg_reached-uninstalled-installed_and_using_on_device-installed_but_not_using_on_device untraced
	from 
		(select 
			request_id, campaign_id,
			count(distinct tbl1.mobile_no) tg_reached,
			count(distinct case when app_status='UNINSTALLED' then mobile_no else null end) uninstalled,
			count(distinct case when app_status='ACTIVE' and device_status='active' then mobile_no else null end) installed_and_using_on_device,
			count(distinct case when app_status='ACTIVE' and device_status='inactive' then mobile_no else null end) installed_but_not_using_on_device
		from 
			(-- campaigns with targeted merchants
			select campaign_id, request_id, mobile_no
			from 
				(select distinct title campaign_id, id request_id
				from public.notification_bulknotificationrequest
				where title in('ZRC210616-13', 'ZRC210616-14', 'ZRC210616-15', 'ZRC210616-16')
				) tbl1 
				
				inner join 
				
				(select request_id, mobile mobile_no
				from public.notification_bulknotificationreceiver
				) tbl2 using(request_id)
			) tbl1
			
			left join 
			
			(-- install/uninstall info
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
				) tbl3 using(mobile_no, id)
			) tbl2 using(mobile_no)
		group by 1, 2
		) tbl1; 
	
	raise notice 'Results of today are successfully inserted'; 

END;
$function$
;

/*
truncate table data_vajapora.zombie_install_tracking_results;

select data_vajapora.fn_zombie_install_tracking_results(); 

select *
from data_vajapora.zombie_install_tracking_results; 
*/
