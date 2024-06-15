/*
- Viz: https://docs.google.com/spreadsheets/d/1pvO4I2UgFkeINGyXwZ9d60Zk94-U4GyyHPaKNaEtrTA/edit#gid=100650543
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

do $$ 
	
declare 
	var_year_month text:='2021-10'; 
	var_year int:=split_part(var_year_month, '-', 1)::int; 
	var_month int:=split_part(var_year_month, '-', 2)::int; 
begin 
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		mobile_no, 
		case 
			when mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in ('SPU','Sticky SPU') and report_date=(date_trunc('month', concat(var_year_month, '-01')::date) + interval '1 month - 1 day')::date) then '1. SPU'
			when tg ilike '%pu%' then '2. PU' 
			when mobile_no in(select mobile_no from tallykhata.tallykhata_transacting_user_date_sequence_final where date_part('month', created_datetime)=var_month and date_part('year', created_datetime)=var_year) then '3. txn MAU'                                              
			else '4. other seg'
		end derived_tg
	from cjm_segmentation.retained_users 
	where report_date=(date_trunc('month', concat(var_year_month, '-01')::date) + interval '1 month - 1 day')::date; 
end $$; 

-- 
select derived_tg, count(mobile_no) merchants 
from data_vajapora.help_a
group by 1; 

--
select derived_tg, count(*) valid_custs
from 
	(select mobile_no, contact
	from public.account
	where 
		type=2
		and left(contact, 3) not in('010', '011', '012')
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1; 

select count(distinct contact) total_valid_custs
from 
	(select mobile_no, contact
	from public.account
	where 
		type=2
		and left(contact, 3) not in('010', '011', '012')
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no);

select derived_tg, count(*) custs
from 
	(select mobile_no, contact
	from public.account
	where type=2
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1; 

select derived_tg, count(distinct contact) custs_month
from 
	(select mobile_no, contact 
	from tallykhata.tallykhata_fact_info_final 
	where 
		left(created_datetime::text, 7)='2021-10'
		and txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
	) tbl1 
	
	inner join 
		
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1;

-- 
select 
	derived_tg,
	count(auto_id) baki_trt, 
	sum(input_amount) baki_trv
from 
	(select mobile_no, input_amount, auto_id 
	from tallykhata.tallykhata_fact_info_final 
	where 
		is_suspicious_txn=0 
		and txn_type='CREDIT_SALE'
		and left(created_datetime::text, 7)='2021-10'          
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1; 

select 
	derived_tg,
	count(auto_id) aday_trt, 
	sum(input_amount) aday_trv
from 
	(select mobile_no, input_amount, auto_id 
	from tallykhata.tallykhata_fact_info_final 
	where 
		is_suspicious_txn=0 
		and txn_type='CREDIT_SALE_RETURN'
		and left(created_datetime::text, 7)='2021-10'           
	) tbl1 
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1; 
	