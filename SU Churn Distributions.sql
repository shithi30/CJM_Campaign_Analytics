/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1600909475
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

-- SPUs churned
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select 
	tbl1.report_date+1 report_date, 
	count(distinct tbl1.mobile_no) spu_churned, 
	count(distinct case when tbl3.max_spu_date=tbl1.report_date then tbl1.mobile_no else null end) spu_churned_forever, 
	count(distinct case when tbl3.max_spu_date!=tbl1.report_date then tbl1.mobile_no else null end) spu_churned_temporarily
from 
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU') 
	) tbl1  
	
	left join 
	
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU') 
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.report_date=tbl1.report_date+1)
	
	left join 
	
	(select mobile_no, max(report_date) max_spu_date 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU')
	group by 1
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
where tbl2.mobile_no is null
group by 1; 

-- Saturday, April 2, 2022
select * 
from data_vajapora.help_a
where report_date>='2022-04-02' and report_date<current_date; 

-- Tuesday, April 13 2021
select * 
from data_vajapora.help_a
where report_date>='2021-04-13' and report_date<'2021-04-13'::date+(current_date-'2022-04-02'::date); 

-- last 90 days' trend
select * 
from data_vajapora.help_a
where report_date>=current_date-90; 

-- SPUs permanently churned
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select tbl1.report_date+1 report_date, tbl1.mobile_no
from 
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU') 
	) tbl1  
	
	left join 
	
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU') 
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.report_date=tbl1.report_date+1)
	
	left join 
	
	(select mobile_no, max(report_date) max_spu_date 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU', 'Sticky SPU')
	group by 1
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
where 
	tbl2.mobile_no is null
	and tbl3.max_spu_date=tbl1.report_date; 

-- biz-types (Mahmud)
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	tbl1.mobile_no, 
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
	(select id, mobile_no, business_type 
	from public.register_tallykhatauser 
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(id) id
	from public.register_tallykhatauser 
	group by 1
	) tbl2 on(tbl1.id=tbl2.id)

	inner join 

	(select owner_id, mobile mobile_no, new_bi_business_type 
	from tallykhata.tallykhata_user_personal_info
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_no)
	
	inner join 
	
	(select mobile mobile_no, max(owner_id) owner_id
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl4 on(tbl3.owner_id=tbl4.owner_id); 

-- distributions
select 
	case when biz_type is null then 'OTHERS' else biz_type end biz_type, 
	count(mobile_no) spus_permanently_churned 
from 
	(select mobile_no 
	from data_vajapora.help_a
	where report_date>=current_date-90 and report_date<current_date-14
	) tbl1 
	
	left join 
	
	data_vajapora.help_b tbl2 using(mobile_no)
group by 1
order by 2 desc; 

select 
	case when tg is null then 'uninstalled' else tg end segment, 
	count(mobile_no) spus_permanently_churned 
from 
	(select mobile_no 
	from data_vajapora.help_a
	where report_date>=current_date-90 and report_date<current_date-14
	) tbl1 
	
	left join 
	
	(select 
		mobile_no,
		case 
			when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
			when tg in('LTUCb','LTUTa') then 'LTU'
			when tg in('NB0','NN1','NN2-6') then 'NN'
			when tg in('NT--') then 'NT'
			when tg in('PSU') then 'PSU'
			when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
			when tg in('SPU') then 'SPU'
			when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
			else 'others'
		end tg
	from cjm_segmentation.retained_users
	where report_date=current_date
	) tbl2 using(mobile_no)
group by 1
order by 2 desc; 

select 
	case when district_name is null then 'uninstalled' else district_name end district_name, 
	count(mobile_no) spus_permanently_churned 
from 
	(select mobile_no 
	from data_vajapora.help_a
	where report_date>=current_date-90 and report_date<current_date-14
	) tbl1 
	
	left join 
	
	(select mobile mobile_no, district_name
	from tallykhata.tallykhata.tallykhata_clients_location_info
	) tbl2 using(mobile_no)
group by 1
order by 2 desc; 

select 
	case
		when reg_date>=report_date-42 and reg_date<report_date then 'reg in 6 weeks'
		when reg_date>=report_date-84 and reg_date<report_date-42 then 'reg in 7 to 12 weeks' 
		when reg_date>=report_date-168 and reg_date<report_date-84 then 'reg in 13 to 24 weeks'
		when reg_date>=report_date-336 and reg_date<report_date-168 then 'reg in 25 to 48 weeks'
		when reg_date>=report_date-672 and reg_date<report_date-336 then 'reg in 49 to 96 weeks'
		when reg_date<report_date-672 then 'reg in more than 96 weeks'
		else null 
	end reg_week,
	count(mobile_no) spus_permanently_churned 
from 
	(select mobile_no, report_date
	from data_vajapora.help_a
	where report_date>=current_date-90 and report_date<current_date-14
	) tbl1 
	
	left join 
	
	(select date(created_at) reg_date, mobile_number mobile_no
	from public.register_usermobile  
	) tbl2 using(mobile_no)
group by 1
order by 2 desc; 
