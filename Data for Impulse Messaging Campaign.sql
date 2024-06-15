-- existing baki from customers, for merchants retained today
drop table if exists data_vajapora.baki_from_customers;
create table data_vajapora.baki_from_customers as
select
	mobile_no, 
	account_id, 
	baki+start_balance baki
from 
	(select 
		mobile_no, 
		account_id,
		sum(case when txn_type=3 and coalesce(amount, 0)>0 then amount else 0 end)
		-
		sum(case when txn_type=3 and coalesce(amount_received, 0)>0 then amount_received else 0 end)
		baki
	from public.journal 
	where 
		is_active is true
		and txn_mode=1
		and date(create_date)<=current_date
	group by 1, 2
	) tbl1 
	
	inner join 
		
	(select mobile_no, id account_id, start_balance
	from public.account
	) tbl2 using(mobile_no, account_id)
	
	inner join 
	
	(-- retained today
	select mobile_no
	from cjm_segmentation.retained_users 
	where report_date=current_date
	) tbl3 using(mobile_no); 
	
-- baki of merchants who did not apprear after 25-Nov-21
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, sum(baki) existing_baki
	from data_vajapora.baki_from_customers 
	group by 1
	having sum(baki)>0
	) tbl1
	
	left join 
	
	(-- DAUs after 25-Nov-21
	select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>'2021-11-25'::date
	
	union 
	
	select mobile_no 
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		date(created_date)>'2021-11-25'::date
		and event_name not in ('in_app_message_received','inbox_message_received')
		
	union 
		
	select ss.mobile_number mobile_no
	from 
		public.user_summary as ss 
		left join 
		public.register_usermobile as i on ss.mobile_number = i.mobile_number
	where 
		i.mobile_number is null 
		and ss.created_at::date>'2021-11-25'::date
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

select mobile_no, translate(trim(to_char(existing_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') existing_baki
from data_vajapora.help_a; 
		
