/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1164061439
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): (answer to first query) 
	Nazrul, do we have any analysis about below two points?
	-Baki purchase and payment behavior of a customer. For example, one customer takes 5 times baki in a month. and pays 2 times in a month. And lead time for purchase is 6 days, payment is 15 days.
	-Number of baki customer txn activity in a week by a PU. For example, average 15 activities in 4 days out of last 7 days by one PU. Activity includes baki sale-collection-customer add-tap on customer bistarito
	Hypothesis
	Number of baki customers, and sum of customers' activities are the key factor for PU retention.
*/

-- PUs
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select distinct mobile_no
from tallykhata.tk_power_users_10 
where report_date=current_date-1;

-- 3RAUs
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select distinct mobile_no
from tallykhata.regular_active_user_event
where 
	rau_category=3
	and report_date::date=current_date-1; 

-- neither PUs nor 3RAUs, but DAUs
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select tbl1.mobile_no
from 		
	(select distinct mobile_no 
	from tallykhata.event_transacting_fact
	where event_date=current_date-1
	) tbl1 
	left join 
	data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
	left join 
	data_vajapora.help_c tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
where 
	tbl2.mobile_no is null
	and tbl3.mobile_no is null; 

-- metrics to show
select *
from 
	(select count(auto_id)*1.00/count(distinct mobile_no) avg_add_customer
	from 
		(select mobile_no, auto_id
		from tallykhata.tallykhata_fact_info_final 
		where txn_type in('Add Customer')
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no) -- replace with right group
	) tbl1,
	
	(select count(auto_id)*1.00/count(distinct mobile_no) avg_add_supplier
	from 
		(select mobile_no, auto_id
		from tallykhata.tallykhata_fact_info_final 
		where txn_type in('Add Supplier')
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no) -- replace with right group
	) tbl2,
	
	(select 
		count(auto_id)*1.00/count(distinct mobile_no) avg_credit_sale_trt,
		sum(cleaned_amount)*1.00/count(distinct mobile_no) avg_credit_sale_amount
	from 
		(select mobile_no, auto_id, cleaned_amount
		from tallykhata.tallykhata_fact_info_final 
		where txn_type in('CREDIT_SALE')
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no) -- replace with right group
	) tbl3,
		
	(select 
		count(auto_id)*1.00/count(distinct mobile_no) avg_credit_sale_return_trt,
		sum(cleaned_amount)*1.00/count(distinct mobile_no) avg_credit_sale_return_amount
	from 
		(select mobile_no, auto_id, cleaned_amount
		from tallykhata.tallykhata_fact_info_final 
		where txn_type in('CREDIT_SALE_RETURN')
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no) -- replace with right group
	) tbl4; 
	