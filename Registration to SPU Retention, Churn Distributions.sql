/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=106990001
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

select 
	reg_month, 
	count(tbl1.mobile_no) registered_merchants, 
	count(tbl2.mobile_no) entered_spu, 
	count(tbl3.mobile_no) current_spu,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null then tbl1.mobile_no else null end) churned_spu,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='PU' then tbl1.mobile_no else null end) churned_spu_in_PU,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='3RAU' then tbl1.mobile_no else null end) churned_spu_in_3RAU,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='Zombie' then tbl1.mobile_no else null end) churned_spu_in_Zombie,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment is null then tbl1.mobile_no else null end) churned_spu_in_uninstalled, 
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='LTU' then tbl1.mobile_no else null end) churned_spu_in_LTU,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='NT' then tbl1.mobile_no else null end) churned_spu_in_NT,
	count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null and segment='PSU' then tbl1.mobile_no else null end) churned_spu_in_PSU
from 
	(select left(date(created_at)::text, 7) reg_month, mobile_number mobile_no
	from public.register_usermobile  
	) tbl1 
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tk_spu_aspu_data 
	where pu_type='SPU'
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU'
		and report_date=current_date-1
	) tbl3 using(mobile_no)
	
	left join 
	
	(select 
		mobile_no, 
		case 
			when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
			when tg in('LTUCb','LTUTa') then 'LTU'
			when tg in('NT--') then 'NT'
			when tg in('NB0','NN1','NN2-6') then 'NN'
			when tg in('PSU') then 'PSU'
			when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
			when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
		end segment
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl4 using(mobile_no)
group by 1 
order by 1; 

