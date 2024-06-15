/*
- Viz: https://docs.google.com/spreadsheets/d/1Wp9Kxu3bmSLGNSf6MYtsmyolTciaOq3r8WjnuzGCu0E/edit#gid=91064467
- Data: 
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

-- lft PUs
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct mobile_no 
from tallykhata.tk_power_users_10; 

-- lft SPUs
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no 
from tallykhata.tk_spu_aspu_data
where pu_type='SPU'; 

-- merchants added 3+ baki customers
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as 
select mobile_no, count(id) added_baki_custs
from public.account 
where 
	type=2
	and start_balance>0
group by 1
having count(id)>2; 

-- necessary metrics
select
	new_bi_business_type, 
	count(tbl1.mobile_no) merchants,
	count(case when tbl8.mobile_no is not null then tbl1.mobile_no else null end) txn_min_1, 
	count(case when tbl9.mobile_no is not null then tbl1.mobile_no else null end) added_baki_custs_min_3,
	count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) txn_jan, 
	count(case when tbl6.mobile_no is not null then tbl1.mobile_no else null end) pu_lft, 
	count(case when tbl3.mobile_no is not null and tbl4.mobile_no is not null then tbl1.mobile_no else null end) pu_now_ret, 
	count(case when tbl7.mobile_no is not null then tbl1.mobile_no else null end) spu_lft, 
	count(case when tbl5.mobile_no is not null and tbl4.mobile_no is not null then tbl1.mobile_no else null end) spu_now_ret
from 
	(select mobile mobile_no, new_bi_business_type 
	from tallykhata.tallykhata_user_personal_info 
	) tbl1 
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tallykhata_transacting_user_date_sequence_final
	where 
		date_part('year', created_datetime)=2022
		and date_part('month', created_datetime)=1
	) tbl2 using(mobile_no)
	
	left join 
		
	(select distinct mobile_no 
	from tallykhata.tk_power_users_10 
	where report_date=current_date-1
	) tbl3 using(mobile_no)
	
	left join 
	
	(select distinct mobile_no 
	from cjm_segmentation.retained_users  
	where report_date=current_date
	) tbl4 using(mobile_no)
	
	left join 
	
	(select mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='SPU' 
		and report_date=current_date-1
	) tbl5 using(mobile_no)
	
	left join 
	
	-- lft PUs
	data_vajapora.help_a tbl6 using(mobile_no)
	
	left join 
	
	-- lft SPUs
	data_vajapora.help_b tbl7 using(mobile_no)
	
	left join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_transacting_user_date_sequence_final 
	) tbl8 using(mobile_no)
	
	left join 
	
	-- merchants added 3+ baki customers
	data_vajapora.help_c tbl9 using(mobile_no)
group by 1 
order by 2 desc; 
	