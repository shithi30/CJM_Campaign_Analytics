/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=81372478
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Regarding PU Analysis!
- Notes (if any): 

- Lifetime PUs: 511,901
	- Active last 30 days: 348,228
		- Uninstalled: 9,892 
		- Installed: 338,336
			- 3RAU: 16,221
			- LTU: 61,817 
			- NT: 47,337
			- NN: 15
			- PSU: 12,052
			- PU: 200,894
	- Inactive last 30 days: 163,673
		- Retained: 81,678
		- Uninstalled: 81,995 
			- Number changed: 2,530
				- Number changed (active): 1,752
				- Number changed (inactive): 778
			- Number unchanged (uninstalled forever): 79,465
*/

-- lifetime PUs
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct mobile_no 
from tallykhata.tk_power_users_10; 

-- statistics
select 
	count(mobile_no) lft_pus, 

	count(case when active_last_30_days=1 then mobile_no else null end) active_last_30_days,
	count(case when active_last_30_days=1 and if_retained is null then mobile_no else null end) active_last_30_days_uninstalled,
	count(case when active_last_30_days=1 and segment is not null then mobile_no else null end) active_last_30_days_installed,
	count(case when active_last_30_days=1 and segment='3RAU' then mobile_no else null end) active_last_30_days_3rau,
	count(case when active_last_30_days=1 and segment='LTU' then mobile_no else null end) active_last_30_days_ltu,
	count(case when active_last_30_days=1 and segment='NT' then mobile_no else null end) active_last_30_days_nt,
	count(case when active_last_30_days=1 and segment='NN' then mobile_no else null end) active_last_30_days_nn,
	count(case when active_last_30_days=1 and segment='PSU' then mobile_no else null end) active_last_30_days_psu,
	count(case when active_last_30_days=1 and segment='PU' then mobile_no else null end) active_last_30_days_pu,
	count(case when active_last_30_days=1 and segment='Zombie' then mobile_no else null end) active_last_30_days_zombie,
	count(case when active_last_30_days=1 and segment='rest' then mobile_no else null end) active_last_30_days_rest,
	
	count(case when active_last_30_days is null then mobile_no else null end) inactive_last_30_days, 
	count(case when active_last_30_days is null and if_retained=1 then mobile_no else null end) inactive_last_30_days_retained, 
	count(case when active_last_30_days is null and if_retained is null then mobile_no else null end) inactive_last_30_days_uninstalled, 
	
	count(case when active_last_30_days is null and if_retained is null and numbers_against_active_ad_id>1 then mobile_no else null end) inactive_last_30_days_uninstalled_number_changed, 
	count(case when active_last_30_days is null and if_retained is null and numbers_against_active_ad_id>1 and active_numbers_against_active_ad_id>0 then mobile_no else null end) inactive_last_30_days_uninstalled_number_changed_active, 
	count(case when active_last_30_days is null and if_retained is null and numbers_against_active_ad_id>1 and (active_numbers_against_active_ad_id=0 or active_numbers_against_active_ad_id is null) then mobile_no else null end) inactive_last_30_days_uninstalled_number_changed_inactive, 
	count(case when active_last_30_days is null and if_retained is null and (numbers_against_active_ad_id=1 or numbers_against_active_ad_id is null) then mobile_no else null end) inactive_last_30_days_uninstalled_number_unchanged                   
from 
	-- lifetime PUs
	data_vajapora.help_a tbl1 
	
	left join 
	
	(-- active last 30 days	
	select distinct mobile_no, 1 active_last_30_days 
	from tallykhata.tallykhata_user_date_sequence_final  
	where event_date>=current_date-30 and event_date<current_date
	) tbl2 using(mobile_no)
	
	left join 
	
	(-- if in retained base
	select 
		mobile_no, 
		1 if_retained, 
		case 
			when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
			when tg in('LTUCb','LTUTa') then 'LTU'
			when tg in('NT--') then 'NT'
			when tg in('NB0','NN1','NN2-6') then 'NN'
			when tg in('PSU') then 'PSU'
			when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
			when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
			else 'rest'
		end segment
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl3 using(mobile_no)
	
	left join 
	
	(-- numbers in same device
	select mobile_no, numbers_against_active_ad_id, active_numbers_against_active_ad_id
	from 
		(select mobile mobile_no, max(id) id
		from data_vajapora.registered_users_temp
		where device_status='active'
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, advertise_id 
		from data_vajapora.registered_users_temp
		) tbl2 using(id)
		
		inner join 
		
		(select 
			advertise_id, 
			count(distinct mobile) numbers_against_active_ad_id,
			count(distinct case when if_active=1 then mobile else null end) active_numbers_against_active_ad_id
		from 
			data_vajapora.registered_users_temp tbl1 
			
			left join 
			
			(select mobile_no mobile, 1 if_active
			from cjm_segmentation.retained_users 
			where report_date=current_date
			) tbl2 using(mobile)
		where device_status='active' 
		group by 1
		) tbl3 using(advertise_id)
	) tbl4 using(mobile_no); 













