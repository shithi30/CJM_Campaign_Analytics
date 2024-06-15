/*
- Viz: https://docs.google.com/spreadsheets/d/1kQJ6u29GoIuFNFIIHQGvos0tOTVUT_LMl7Kdy9KyshU/edit#gid=1156777090
- Data: 
- Function: 
- Table: data_vajapora.continued_su_details, data_vajapora.discontinued_su_details
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Pivot help: 
	- https://docs.google.com/spreadsheets/d/1jcFLdV3N__t8kFGVWc4-hyVQDMLZ35wK07rZlDLlw5Q/edit#gid=521865406
	- https://docs.google.com/spreadsheets/d/1jcFLdV3N__t8kFGVWc4-hyVQDMLZ35wK07rZlDLlw5Q/edit#gid=2028034734
*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct mobile_no, to_char(report_date, 'YYYY-MM') spu_year_mon
from tallykhata.tk_spu_aspu_data 
where 
	pu_type in('SPU')
	and report_date<current_date; 	

-- rigorous retention cohort in number and pct
with 
	temp_table as
	(select 
		min_spu_year_mon, 
		spu_year_mon, 
		count(mobile_no) sus, 
		row_number() over(partition by min_spu_year_mon order by spu_year_mon)-1 seq
	from 
		(select 
			mobile_no, 
			min_spu_year_mon, 
			spu_year_mon, 
			(date_part('year', concat(spu_year_mon, '-01')::date)-date_part('year', concat(min_spu_year_mon, '-01')::date))*12+(date_part('month', concat(spu_year_mon, '-01')::date)-date_part('month', concat(min_spu_year_mon, '-01')::date)) diff_months,                                      
			row_number() over(partition by mobile_no order by spu_year_mon)-1 diff_seq
		from 
			(select mobile_no, min(spu_year_mon) min_spu_year_mon 
			from data_vajapora.help_a 
			group by 1
			) tbl1 
			
			inner join 
			
			data_vajapora.help_a tbl2 using(mobile_no)
		) tbl1 
	where diff_months=diff_seq
	group by 1, 2
	)

select min_spu_year_mon, spu_year_mon, seq, sus, sus_init, sus*1.00/sus_init sus_init_pct
from 
	temp_table tbl1 
	
	inner join 
	
	(select min_spu_year_mon, sus sus_init 
	from temp_table 
	where seq=0
	) tbl2 using(min_spu_year_mon)
where min_spu_year_mon>='2020-07'
order by 1, 2; 

-- continued monthly SUs from Jul-2020 to Jul-2022
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile_no, min_spu_year_mon, spu_year_mon
from 
	(select 
		mobile_no, 
		min_spu_year_mon, 
		spu_year_mon, 
		(date_part('year', concat(spu_year_mon, '-01')::date)-date_part('year', concat(min_spu_year_mon, '-01')::date))*12+(date_part('month', concat(spu_year_mon, '-01')::date)-date_part('month', concat(min_spu_year_mon, '-01')::date)) diff_months,                                      
		row_number() over(partition by mobile_no order by spu_year_mon)-1 diff_seq
	from 
		(select mobile_no, min(spu_year_mon) min_spu_year_mon 
		from data_vajapora.help_a 
		group by 1
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no)
	) tbl1 
where 
	diff_months=diff_seq 
	and spu_year_mon='2022-07'; 

-- biz type by Mahmud
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select 
	mobile_no, 
	case 
		when business_type in('BAKERY_AND_CONFECTIONERY') then 'SWEETS AND CONFECTIONARY'
		when business_type in('ELECTRONICS') then 'ELECTRONICS STORE'
		when business_type in('MFS_AGENT','MFS_MOBILE_RECHARGE') then 'MFS-MOBILE RECHARGE STORE'
		when business_type in('GROCERY') then 'GROCERY'
		when business_type in('DISTRIBUTOR_OR_WHOLESALE','WHOLESALER','DEALER') then 'OTHER WHOLESELLER'
		when business_type in('HOUSEHOLD_AND_FURNITURE') then 'FURNITURE SHOP'
		when business_type in('STATIONERY') then 'STATIONARY BUSINESS'
		when business_type in('TAILORS') then 'TAILERS'
		when business_type in('PHARMACY') then 'PHARMACY'
		when business_type in('SHOE_STORE') then 'SHOE STORE'
		when business_type in('MOTOR_REPAIR') then 'VEHICLE-CAR SERVICING'
		when business_type in('COSMETICS') then 'COSMETICS AND PERLOUR'
		when business_type in('ROD_CEMENT') then 'CONSTRUCTION RAW MATERIAL'
		when business_type='' then upper(case when new_bi_business_type!='Other Business' then new_bi_business_type else null end) 
		else null 
	end biz_type
from 
	(select id, business_type 
	from public.register_tallykhatauser 
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(id) id
	from public.register_tallykhatauser 
	group by 1
	) tbl2 using(id)
	
	inner join 
	
	(select 
		mobile mobile_no, 
		max(new_bi_business_type) new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl3 using(mobile_no); 

-- all info of continued
drop table if exists data_vajapora.continued_su_details; 
create table data_vajapora.continued_su_details as
select 
	mobile_no, 
	min_spu_year_mon, spu_year_mon, 
	reg_date, shop_name, merchant_name, 
	coalesce(district_name, 'Unknown') district_name, coalesce(union_name, 'Unknown') union_name, 
	coalesce(biz_type, 'OTHER BUSINESS') biz_type, 
	su_months, 
	tg_name, 
	coalesce(added_customers, 0) added_customers, coalesce(added_suppliers, 0) added_suppliers
from 
	data_vajapora.help_b tbl1
	
	left join 

	(select 
		mobile mobile_no, 
		max(registration_date) reg_date, 
		max(coalesce(shop_name, business_name, "name")) shop_name, 
		max(merchant_name) merchant_name 
	from tallykhata.tallykhata_user_personal_info
	group by 1
	) tbl2 using(mobile_no)

	left join 

	(select 
		mobile mobile_no, 
		max(district_name) district_name, 
		max(union_name) union_name
	from tallykhata.tallykhata_clients_location_info
	group by 1
	) tbl3 using(mobile_no)
	
	left join 
	
	(select mobile_no, biz_type 
	from data_vajapora.help_c
	) tbl4 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(spu_year_mon) su_months
	from data_vajapora.help_a
	where spu_year_mon<='2022-07'
	group by 1
	) tbl5 using(mobile_no)
	
	left join 

	(select mobile_no, max(tg) tg_name
	from cjm_segmentation.retained_users
	where report_date=current_date-1
	group by 1
	) tbl6 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(distinct contact) added_customers
	from public.account 
	where 
		"type"=2
		and is_active is true
	group by 1
	) tbl7 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(distinct contact) added_suppliers
	from public.account 
	where 
		"type"=3
		and is_active is true
	group by 1
	) tbl8 using(mobile_no); 

select * 
from data_vajapora.continued_su_details; 

-- all info of discontinued
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select mobile_no
from 
	(select distinct mobile_no
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU')
	) tbl1 
	
	left join 
	
	(select distinct mobile_no
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type in('SPU')
		and report_date=current_date-1
	) tbl2 using(mobile_no)
	
	left join 
		
	(select mobile_no 
	from data_vajapora.continued_su_details
	) tbl3 using(mobile_no) 
where 
	tbl2.mobile_no is null 
	and tbl3. mobile_no is null; 

drop table if exists data_vajapora.discontinued_su_details; 
create table data_vajapora.discontinued_su_details as
select 
	mobile_no, 
	min_spu_year_mon, churn_month, 
	reg_date, shop_name, merchant_name, 
	coalesce(district_name, 'Unknown') district_name, coalesce(union_name, 'Unknown') union_name, 
	coalesce(biz_type, 'OTHER BUSINESS') biz_type, 
	su_months, 
	tg_name, 
	coalesce(added_customers, 0) added_customers, coalesce(added_suppliers, 0) added_suppliers
from 
	data_vajapora.help_d tbl1
	
	left join 

	(select 
		mobile mobile_no, 
		max(registration_date) reg_date, 
		max(coalesce(shop_name, business_name, "name")) shop_name, 
		max(merchant_name) merchant_name 
	from tallykhata.tallykhata_user_personal_info
	group by 1
	) tbl2 using(mobile_no)

	left join 

	(select 
		mobile mobile_no, 
		max(district_name) district_name, 
		max(union_name) union_name
	from tallykhata.tallykhata_clients_location_info
	group by 1
	) tbl3 using(mobile_no)
	
	left join 
	
	(select mobile_no, biz_type 
	from data_vajapora.help_c
	) tbl4 using(mobile_no)
	
	left join 
	
	(select 
		mobile_no, 
		count(spu_year_mon) su_months, 
		to_char(concat(max(spu_year_mon), '-01')::date+45, 'YYYY-MM') churn_month, 
		min(spu_year_mon) min_spu_year_mon
	from data_vajapora.help_a
	where spu_year_mon<='2022-07'
	group by 1
	) tbl5 using(mobile_no)
	
	left join 

	(select mobile_no, max(tg) tg_name
	from cjm_segmentation.retained_users
	where report_date=current_date-1
	group by 1
	) tbl6 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(distinct contact) added_customers
	from public.account 
	where 
		"type"=2
		and is_active is true
	group by 1
	) tbl7 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(distinct contact) added_suppliers
	from public.account 
	where 
		"type"=3
		and is_active is true
	group by 1
	) tbl8 using(mobile_no); 

select * 
from data_vajapora.discontinued_su_details;  