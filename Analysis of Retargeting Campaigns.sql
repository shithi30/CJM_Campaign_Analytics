/*
- Viz: https://docs.google.com/spreadsheets/d/1PV9WqZx81g46n0X2Xk8CIx2tQlFwFND4kZNRCAcjTJ0/edit#gid=1189642695
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1109025240
- Function: data_vajapora.fn_retarget_results()
- Table: data_vajapora.retarget_results
- File: 
- Path: 
- Presentation: 
- Email thread: 
- Notes (if any): Portal Inbox - RTG210624-10, FB camping ID - RTG210624-01
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_retarget_results()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis how retargeting campaigns are doing, in comparison with previous months
Auxiliary data table(s) : data_vajapora.compare_mar, data_vajapora.compare_apr, data_vajapora.compare_may
Target data table       : data_vajapora.retarget_results
*/

declare

begin

	/* generation of month-wise data to show comparison with */
	
	-- merchants who were inactive from 2021-05-01 to 2021-05-23, but had registered before
	drop table if exists data_vajapora.compare_may; 	
	create table data_vajapora.compare_may as
	select *
	from 
		(select distinct mobile_number mobile_no 
		from public.register_usermobile
		where date(created_at)<='2021-05-01'
		) tbl1
		
		left join 
		
		(select distinct mobile_no 
		from tallykhata.event_transacting_fact
		where event_date>='2021-05-01' and event_date<='2021-05-23'
		) tbl2 using(mobile_no) 
	where tbl2.mobile_no is null; 
	
	-- merchants who were inactive from 2021-04-01 to 2021-04-23, but had registered before
	drop table if exists data_vajapora.compare_apr; 	
	create table data_vajapora.compare_apr as
	select *
	from 
		(select distinct mobile_number mobile_no 
		from public.register_usermobile
		where date(created_at)<='2021-04-01'
		) tbl1
		
		left join 
		
		(select distinct mobile_no 
		from tallykhata.event_transacting_fact
		where event_date>='2021-04-01' and event_date<='2021-04-23'
		) tbl2 using(mobile_no) 
	where tbl2.mobile_no is null; 
	
	-- merchants who were inactive from 2021-03-01 to 2021-03-23, but had registered before
	drop table if exists data_vajapora.compare_mar; 	
	create table data_vajapora.compare_mar as
	select *
	from 
		(select distinct mobile_number mobile_no 
		from public.register_usermobile
		where date(created_at)<='2021-03-01'
		) tbl1
		
		left join 
		
		(select distinct mobile_no 
		from tallykhata.event_transacting_fact
		where event_date>='2021-03-01' and event_date<='2021-03-23'
		) tbl2 using(mobile_no) 
	where tbl2.mobile_no is null; 

	raise notice 'Monthly data to perform comparison with are generated.'; 
	
	/* the comparison */
	
	drop table if exists data_vajapora.retarget_results; 
	create table data_vajapora.retarget_results as
	
	-- campaign period
	select 
		'campaign' modality,
		concat(min(event_date), ' to ', max(event_date)) timeframe, 
		
		count(distinct tbl2.mobile_no) tg_size,

		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,                       
	
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_app_opened_pct, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_customer_pct,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_txn_pct                       
	from 
		(select distinct title campaign_id, id request_id
		from public.notification_bulknotificationrequest
		where title in('RTG210624-10', 'RTG210624-01')
		) tbl1 
		
		inner join 
		
		(select request_id, mobile mobile_no
		from public.notification_bulknotificationreceiver
		) tbl2 using(request_id)
		
		left join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-06-24' and event_date<=current_date -- change
		) tbl3 using(mobile_no)
		
	union all
	
	-- non-campaign period: May
	select 
		'non-campaign/organic' modality,
		concat(min(event_date), ' to ', max(event_date)) timeframe, 
		
		count(distinct tbl2.mobile_no) tg_size,
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,
		
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_app_opened_pct, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_customer_pct,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_txn_pct                       
	from 
		data_vajapora.compare_may tbl2
		
		left join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-05-24' and event_date<='2021-05-24'::date+(current_date-'2021-06-24') -- change (observe for same count of days after campaign)
		) tbl3 using(mobile_no)
	
	union all
	
	-- non-campaign period: Apr
	select 
		'non-campaign/organic' modality,
		concat(min(event_date), ' to ', max(event_date)) timeframe, 
		
		count(distinct tbl2.mobile_no) tg_size,
		
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,
	
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_app_opened_pct, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_customer_pct,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_txn_pct                       
	from 
		data_vajapora.compare_apr tbl2
		
		left join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-04-24' and event_date<='2021-04-24'::date+(current_date-'2021-06-24') -- change (observe for same count of days after campaign)
		) tbl3 using(mobile_no)
		
	union all
	
	-- non-campaign period: Mar
	select 
		'non-campaign/organic' modality,
		concat(min(event_date), ' to ', max(event_date)) timeframe, 
		
		count(distinct tbl2.mobile_no) tg_size,

		count(distinct case when entry_type=2 then tbl3.mobile_no else null end) merchants_app_opened, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end) merchants_added_customer,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end) merchants_added_txn,
		
		count(distinct case when entry_type=2 then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_app_opened_pct, 
		count(distinct case when event_name='Add Customer' then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_customer_pct,
		count(distinct case when event_name in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'EXPENSE', 'CASH_PURCHASE', 'CREDIT_SALE_RETURN', 'CASH_SALE', 'CREDIT_PURCHASE') then tbl3.mobile_no else null end)*1.00/count(distinct tbl2.mobile_no) merchants_added_txn_pct                       
	from 
		data_vajapora.compare_mar tbl2
		
		left join 
		
		(select mobile_no, entry_type, event_name, event_date
		from tallykhata.event_transacting_fact 
		where 
			(event_name='app_opened' or entry_type=1) 
			and event_date>='2021-03-24' and event_date<='2021-03-24'::date+(current_date-'2021-06-24') -- change (observe for same count of days after campaign)
		) tbl3 using(mobile_no); 
	
	raise notice 'Comparative metrics are generated.'; 

	-- drop auxiliary tables
	drop table if exists data_vajapora.help_mar; 
	drop table if exists data_vajapora.help_apr; 
	drop table if exists data_vajapora.help_may; 

END;
$function$
;
	
/*	
select data_vajapora.fn_retarget_results(); 
select * from data_vajapora.retarget_results; 
*/
		