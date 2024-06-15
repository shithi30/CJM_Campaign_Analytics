/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=227295123
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

-- for daily monthly stats
CREATE OR REPLACE FUNCTION data_vajapora.fn_daily_monthly_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	-- last date of last month
	var_start_date date:=concat(left(current_date::text, 7), '-01')::date-1; 
	var_end_date date:=current_date; 
begin 
	raise notice 'New OP goes below:'; 

	-- for 1st day of month, show full history of last month
	if right(current_date::text, 2)='01' then var_start_date:=date_trunc('month', current_date - interval '1' month)::date-1; 
	end if; 
	
	-- entries from journal
	drop table if exists data_vajapora.daily_monthly_stats_help_b; 
	create table data_vajapora.daily_monthly_stats_help_b as
	select id jr_id, amount, amount_received
	from public.journal 
	where 
		is_active is true	
		and date(create_date)>var_start_date and date(create_date)<var_end_date;
	raise notice 'Entries brought from journal.'; 
	
	-- corresponding txn types
	drop table if exists data_vajapora.daily_monthly_stats_help_c; 
	create table data_vajapora.daily_monthly_stats_help_c as
	select mobile_no, account_id, txn_type, journal_tbl_id jr_id
	from tallykhata.tallykhata_user_transaction_info
	where date(created_datetime)>var_start_date and date(created_datetime)<var_end_date;
	raise notice 'Transaction types identified.'; 
	
	-- populate metrics
	drop table if exists data_vajapora.daily_monthly_stats_help_a; 
	create table data_vajapora.daily_monthly_stats_help_a as
	select 
		mobile_no, 
		
		coalesce(sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) ei_masher_baki_becha,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end), 0) ei_masher_baki_becha_customers,
		coalesce(sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end), 0) ei_masher_baki_aday,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end), 0) ei_masher_baki_aday_customers,
	
		-- coalesce(sum(case when txn_type in('CREDIT_SALE') then amount else 0 end)-sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end), 0) ei_masher_pabo,                                      
		-- coalesce(sum(case when txn_type in('CREDIT_PURCHASE') then amount_received else 0 end)-sum(case when txn_type in('CREDIT_PURCHASE_RETURN') then amount else 0 end), 0) ei_masher_debo,                                      
		
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end), 0) ei_masher_cash_becha,
		coalesce(sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end), 0) ei_masher_cash_kena,
		coalesce(sum(case when txn_type in('EXPENSE') then amount else 0 end), 0) ei_masher_khoroch,
		
		coalesce(sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0) ei_masher_malik_dilo,
		coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end), 0) ei_masher_malik_nilo, 
		
		abs(coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end)-sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0)) ei_masher_maliker_balance, 
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end)+sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) ei_masher_mot_becha
	from 
		(-- TG: PU, 3RAU (including personal)
		/*select mobile_no 
		from tallykhata.tk_power_users_10
		where report_date=current_date-1
		
		union
		
		select mobile_no
		from tallykhata.regular_active_user_event
		where 
			rau_category=3 
			and report_date::date=current_date-1*/
		
		select mobile_no
		from cjm_segmentation.retained_users 
		where 
			report_date=current_date
			and tg ilike 'pu%'
		) tbl0 
		
		left join
	
		data_vajapora.daily_monthly_stats_help_c tbl1 using(mobile_no)
		
		left join 
			
		data_vajapora.daily_monthly_stats_help_b tbl2 using(jr_id)
	group by 1; 
	raise notice 'Metrics populated.'; 

	-- translate into Bangla, put seperators, bring shop-names
	drop table if exists data_vajapora.daily_monthly_stats;
	create table data_vajapora.daily_monthly_stats as
	select 
		mobile_no,  
		shop_name, 
		translate(trim(to_char(ei_masher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha, 
		translate(trim(to_char(ei_masher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha_customers, 
		translate(trim(to_char(ei_masher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday, 
		translate(trim(to_char(ei_masher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday_customers, 
		-- translate(trim(to_char(ei_masher_pabo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_pabo, 
		-- translate(trim(to_char(ei_masher_debo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_debo, 
		translate(trim(to_char(ei_masher_cash_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_cash_becha, 
		translate(trim(to_char(ei_masher_cash_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_cash_kena, 
		translate(trim(to_char(ei_masher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_khoroch, 
		translate(trim(to_char(ei_masher_malik_dilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_malik_dilo, 
		translate(trim(to_char(ei_masher_malik_nilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_malik_nilo, 
		translate(trim(to_char(ei_masher_maliker_balance, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_maliker_balance, 
		translate(trim(to_char(ei_masher_mot_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_mot_becha
	from
		data_vajapora.daily_monthly_stats_help_a tbl1 
		
		left join 
			
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 using(mobile_no); 
	raise notice 'Data prepared.'; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.daily_monthly_stats_help_a; 
	drop table if exists data_vajapora.daily_monthly_stats_help_b; 
	drop table if exists data_vajapora.daily_monthly_stats_help_c; 
	raise notice 'Auxiliary tables dropped.';

END;
$function$
;

select data_vajapora.fn_daily_monthly_stats(); 



-- for yesterday stats
CREATE OR REPLACE FUNCTION data_vajapora.fn_daily_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 

begin 
	raise notice 'New OP goes below:'; 
	
	-- entries from journal
	drop table if exists data_vajapora.goto_diner_stats_help_b; 
	create table data_vajapora.goto_diner_stats_help_b as
	select id jr_id, amount, amount_received
	from public.journal 
	where 
		is_active is true	
		and date(create_date)=current_date-1;
	raise notice 'Entries brought from journal.'; 
	
	-- corresponding txn types
	drop table if exists data_vajapora.goto_diner_stats_help_c; 
	create table data_vajapora.goto_diner_stats_help_c as
	select mobile_no, account_id, txn_type, journal_tbl_id jr_id
	from tallykhata.tallykhata_user_transaction_info
	where date(created_datetime)=current_date-1;
	raise notice 'Transaction types identified.'; 
	
	-- populate metrics
	drop table if exists data_vajapora.goto_diner_stats_help_a; 
	create table data_vajapora.goto_diner_stats_help_a as
	select 
		mobile_no, 
		
		coalesce(sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) goto_diner_baki_becha,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end), 0) goto_diner_baki_becha_customers,
		coalesce(sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end), 0) goto_diner_baki_aday,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end), 0) goto_diner_baki_aday_customers,
	
		-- coalesce(sum(case when txn_type in('CREDIT_SALE') then amount else 0 end)-sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end), 0) goto_diner_pabo,                                      
		-- coalesce(sum(case when txn_type in('CREDIT_PURCHASE') then amount_received else 0 end)-sum(case when txn_type in('CREDIT_PURCHASE_RETURN') then amount else 0 end), 0) goto_diner_debo,                                      
		
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end), 0) goto_diner_cash_becha,
		coalesce(sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end), 0) goto_diner_cash_kena,
		coalesce(sum(case when txn_type in('EXPENSE') then amount else 0 end), 0) goto_diner_khoroch,
		
		coalesce(sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0) goto_diner_malik_dilo,
		coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end), 0) goto_diner_malik_nilo, 
		
		abs(coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end)-sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0)) goto_diner_maliker_balance, 
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end)+sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) goto_diner_mot_becha
	from 
		(-- TG: PU, 3RAU (including personal)
		/*select mobile_no 
		from tallykhata.tk_power_users_10
		where report_date=current_date-1
		
		union
		
		select mobile_no
		from tallykhata.regular_active_user_event
		where 
			rau_category=3 
			and report_date::date=current_date-1*/
		
		select mobile_no
		from cjm_segmentation.retained_users 
		where 
			report_date=current_date
			and tg ilike 'pu%'
		) tbl0 
		
		left join
	
		data_vajapora.goto_diner_stats_help_c tbl1 using(mobile_no)
		
		left join 
			
		data_vajapora.goto_diner_stats_help_b tbl2 using(jr_id)
	group by 1; 
	raise notice 'Metrics populated.'; 

	-- translate into Bangla, put seperators, bring shop-names
	drop table if exists data_vajapora.daily_stats;
	create table data_vajapora.daily_stats as
	select 
		mobile_no,  
		shop_name, 
		translate(trim(to_char(goto_diner_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_baki_becha, 
		translate(trim(to_char(goto_diner_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_baki_becha_customers, 
		translate(trim(to_char(goto_diner_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_baki_aday, 
		translate(trim(to_char(goto_diner_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_baki_aday_customers, 
		-- translate(trim(to_char(goto_diner_pabo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_pabo, 
		-- translate(trim(to_char(goto_diner_debo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_debo, 
		translate(trim(to_char(goto_diner_cash_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_cash_becha, 
		translate(trim(to_char(goto_diner_cash_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_cash_kena, 
		translate(trim(to_char(goto_diner_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_khoroch, 
		translate(trim(to_char(goto_diner_malik_dilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_malik_dilo, 
		translate(trim(to_char(goto_diner_malik_nilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_malik_nilo, 
		translate(trim(to_char(goto_diner_maliker_balance, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_maliker_balance, 
		translate(trim(to_char(goto_diner_mot_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_diner_mot_becha
	from
		data_vajapora.goto_diner_stats_help_a tbl1 
		
		left join 
			
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 using(mobile_no); 
	raise notice 'Data prepared.'; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.goto_diner_stats_help_a; 
	drop table if exists data_vajapora.goto_diner_stats_help_b; 
	drop table if exists data_vajapora.goto_diner_stats_help_c; 
	raise notice 'Auxiliary tables dropped.';

END;
$function$
;

select data_vajapora.fn_daily_stats(); 

-- QA: in('01980001564', '01686154127', '01684311672')



-- PUs of the day
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select mobile_no, row_number() over(order by random()) seq
from cjm_segmentation.retained_users 
where 
	report_date=current_date
	and tg ilike 'pu%'; 
	
-- daily monthly: 20k PUs
drop table if exists data_vajapora.experimental_custom_msg_daily_monthly; 
create table data_vajapora.experimental_custom_msg_daily_monthly as
select tbl1.*
from 
	data_vajapora.daily_monthly_stats tbl1
	inner join 
	data_vajapora.help_a tbl2 using(mobile_no)
where seq>=1 and seq<=20000; 

select *
from data_vajapora.experimental_custom_msg_daily_monthly; 

-- daily: 20k PUs
drop table if exists data_vajapora.experimental_custom_msg_yesterday; 
create table data_vajapora.experimental_custom_msg_yesterday as
select tbl1.*
from 
	data_vajapora.daily_stats tbl1
	inner join 
	data_vajapora.help_a tbl2 using(mobile_no)
where seq>=20001 and seq<=40000; 

select *
from data_vajapora.experimental_custom_msg_yesterday; 



-- calling function 

CREATE OR REPLACE FUNCTION data_vajapora.fn_experimental_custom_message()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare
	
begin
	-- functions to generate data
	execute 'select * from data_vajapora.fn_daily_monthly_stats()'; 
	execute 'select * from data_vajapora.fn_daily_stats()'; 

	-- PUs of the day
	drop table if exists data_vajapora.custom_msg_help; 
	create table data_vajapora.custom_msg_help as 
	select mobile_no, row_number() over(order by random()) seq
	from cjm_segmentation.retained_users 
	where 
		report_date=current_date
		and tg ilike 'pu%'; 
		
	-- daily monthly: 20k PUs
	drop table if exists data_vajapora.experimental_custom_msg_daily_monthly; 
	create table data_vajapora.experimental_custom_msg_daily_monthly as
	select tbl1.*
	from 
		data_vajapora.daily_monthly_stats tbl1
		inner join 
		data_vajapora.custom_msg_help tbl2 using(mobile_no)
	where seq>=1 and seq<=20000;
	
	-- daily: 20k PUs
	drop table if exists data_vajapora.experimental_custom_msg_yesterday; 
	create table data_vajapora.experimental_custom_msg_yesterday as
	select tbl1.*
	from 
		data_vajapora.daily_stats tbl1
		inner join 
		data_vajapora.custom_msg_help tbl2 using(mobile_no)
	where seq>=20001 and seq<=40000; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.custom_msg_help; 

END;
$function$
;

select data_vajapora.fn_experimental_custom_message(); 

select *
from data_vajapora.experimental_custom_msg_daily_monthly; 

select *
from data_vajapora.experimental_custom_msg_yesterday; 




