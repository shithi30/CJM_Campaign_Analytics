/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1226931309
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
	After Noboborsho: 
	- In 40% cases, baki from customers decreased. 
	- Among them, in 10% cases, baki became 0. 
*/

drop table if exists data_vajapora.baki_from_customers_before;
create table data_vajapora.baki_from_customers_before as
select
	mobile_no, 
	account_id, 
	baki+start_balance baki
from 
	(select 
		mobile_no, 
		account_id,
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount, 0)>0 then amount else 0 end)
		-
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 then amount_received else 0 end)
		baki
	from public.journal 
	where 
		is_active is true
		and date(create_date)<'2022-04-14'::date
	group by 1, 2
	) tbl1 
	
	inner join 
		
	(select mobile_no, id account_id, start_balance
	from public.account
	) tbl2 using(mobile_no, account_id); 

drop table if exists data_vajapora.baki_from_customers_after;
create table data_vajapora.baki_from_customers_after as
select
	mobile_no, 
	account_id, 
	baki+start_balance baki
from 
	(select 
		mobile_no, 
		account_id,
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount, 0)>0 then amount else 0 end)
		-
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 then amount_received else 0 end)
		baki
	from public.journal 
	where 
		is_active is true
		and date(create_date)<'2022-04-17'::date
	group by 1, 2
	) tbl1 
	
	inner join 
		
	(select mobile_no, id account_id, start_balance
	from public.account
	) tbl2 using(mobile_no, account_id); 

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when baki_before>baki_after and baki_after=0 then 'baki decreased to zero'
		when baki_before>baki_after and baki_after!=0 then 'baki decreased' 
		when baki_before<baki_after then 'baki increased'
		when baki_before=baki_after then 'baki unchanged'
		else null 
	end baki_status
from 
	(select mobile_no, account_id, baki baki_before
	from data_vajapora.baki_from_customers_before
	where baki>0
	) tbl1 
	
	inner join 
	
	(select mobile_no 
	from cjm_segmentation.retained_users
	where report_date=current_date-1
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile_no, account_id, baki baki_after
	from data_vajapora.baki_from_customers_after
	) tbl3 using(mobile_no, account_id); 

select *
from data_vajapora.help_a
limit 5000; 

select 
	baki_status, 
	count(distinct mobile_no) merchants, 
	count(distinct account_id) customers, 
	count(*) cases
from data_vajapora.help_a
group by 1
order by 1; 
