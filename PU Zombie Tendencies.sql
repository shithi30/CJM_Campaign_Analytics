/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Regarding summary analysis reports on TG: PU to Zombie on lifetime!
- Notes (if any): 
*/

-- zombies
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select * 
from 
	(select distinct mobile_no
	from tallykhata.tk_power_users_10 
	) tbl1 
	
	left join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>=current_date-30
	) tbl2 using(mobile_no) 
where tbl2.mobile_no is null; 

-- 159153 have become zombies from PU in lft. 
select 
	count(distinct case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) zombie_pus_retained_today, 
	count(distinct case when tbl2.mobile_no is null then tbl1.mobile_no else null end) zombie_pus_uninstalled
from 
	data_vajapora.help_a tbl1 
	
	left join 

	(select mobile_no
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl2 using(mobile_no); 

-- 60,966 have changed devices
select mobile_no, count(distinct device_info) devices
from 
	(select mobile mobile_no, concat(device_brand, ' ', device_name, ' ', device_model) device_info
	from public.register_historicalregistereduser
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1
having count(distinct device_info)>1; 
