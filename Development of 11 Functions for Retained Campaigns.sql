/*
- Viz: 
- Data: https://drive.google.com/drive/folders/18xYAyWk_1cj8z5sGkQROqQTuT7ttWKV8
- Function: 6 functions in mentioned note
- Table:
- File: 
- Path: http://localhost:8888/notebooks/Import%20from%20csv%20to%20DB/28-Aug-21.ipynb
- Cal: https://docs.google.com/presentation/d/1QZDxiefsRldk-Ha9N8bOgbzJNASUXTvihLdXIcvpJHc/edit#slide=id.ge88bb8e400_0_0
- Document/Presentation/Dashboard: https://docs.google.com/presentation/d/1QZDxiefsRldk-Ha9N8bOgbzJNASUXTvihLdXIcvpJHc/edit#slide=id.ge8fbd8c5a7_0_0
- Email thread: CJM Data For Custom Messaging.
- Notes (if any): 

Shahnewaj Bhai's messages and corresponding functions:
- 1: data_vajapora.fn_yesterday_transaction_stats()
- 2: data_vajapora.fn_credit_sales_previous_month_stats()
- 3: data_vajapora.fn_more_than_30_days_credit_stats()
- 4: data_vajapora.fn_top_baki_customer_stats()
- 5: data_vajapora.fn_credit_sales_this_month_stats()
- 6: data_vajapora.fn_weekly_transaction_stats()
- 7: data_vajapora.fn_last_3_months_transaction_stats()
- 8: data_vajapora.fn_this_or_previous_month_transaction_stats()
- 9: data_vajapora.fn_usage_last_7_days_stats()
- 10: data_vajapora.fn_inspire_credit_return_monthly_stats()
- 11: data_vajapora.fn_inspire_credit_return_weekly_stats()

- sanity check with Shahnewaj Bhai's no.: mobile_no in('01684311672')

-- get rid of edited entries:
-- in live
select distinct mobile_no
from public.journal 
where 
	is_updated is true
	and date(create_date)>='2021-06-01' and date(create_date)<='2021-08-31';
-- import to DWH's data_vajapora.edited_entry using Python 
-- get statistics of users who did not edit entries
select * 
from 
    data_vajapora.last_3_months_transaction_stats tbl1 
    left join 
    (select concat('0', mobile_no) mobile_no
    from data_vajapora.edited_entry
    ) tbl2 using(mobile_no)
where tbl2.mobile_no is null;

-- break the csvs
    """
    df_fetched=df_fetched.drop_duplicates()
    size=200000
    k=math.ceil(df_fetched.shape[0]/size)
    for i in range(k):
        df = df_fetched[size*i:size*(i+1)]
        df.to_csv(now.strftime("%d%m%Y")+f'_top_baki_customer_stats_{i+1}.csv', index=False)
    """
-- gef files to upload here: C:\Users\progoti\Import from csv to DB

*/

/*
1.
গতকালের বাকি আদায় ৳ ১,৯০০; বাকি বেচা ৳ ৩,৫৫০; 💸⚡️
বাকি বেচা ৫ জনের কাছে; বাকি আদায় ২ জন থেকে।
মোট বেচা ৳ ১২,৩৭৫; কেনা ৳ ২,৫৪০; খরচ ৳ ৬৫০; কোন এন্ট্রি মিস হলে এখনই টালিখাতা অ্যাপে লিখে নিন!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_yesterday_transaction_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	-- generate yesterday's statistics
	drop table if exists data_vajapora.yesterday_transaction_stats_help; 
	create table data_vajapora.yesterday_transaction_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when gotokaler_becha is not null then gotokaler_becha else 0 end gotokaler_becha,
		case when gotokaler_baki_becha is not null then gotokaler_baki_becha else 0 end gotokaler_baki_becha,
		case when gotokaler_baki_becha_customers is not null then gotokaler_baki_becha_customers else 0 end gotokaler_baki_becha_customers,
		case when gotokaler_kena is not null then gotokaler_kena else 0 end gotokaler_kena,
		case when gotokaler_khoroch is not null then gotokaler_khoroch else 0 end gotokaler_khoroch,
		case when gotokaler_baki_aday is not null then gotokaler_baki_aday else 0 end gotokaler_baki_aday,
		case when gotokaler_baki_aday_customers is not null then gotokaler_baki_aday_customers else 0 end gotokaler_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- yesterday's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) gotokaler_becha,
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) gotokaler_baki_becha,
			count(distinct case when txn_type in('CREDIT_SALE') then contact else null end) gotokaler_baki_becha_customers,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) gotokaler_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) gotokaler_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) gotokaler_baki_aday,
			count(distinct case when txn_type in('CREDIT_SALE_RETURN') then contact else null end) gotokaler_baki_aday_customers
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=current_date-1
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	-- translate statistics into Bangla
	drop table if exists data_vajapora.yesterday_transaction_stats;
	create table data_vajapora.yesterday_transaction_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(gotokaler_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_becha,
		translate(trim(to_char(gotokaler_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_baki_becha,
		translate(trim(to_char(gotokaler_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_baki_becha_customers,
		translate(trim(to_char(gotokaler_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_kena,
		translate(trim(to_char(gotokaler_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_khoroch,
		translate(trim(to_char(gotokaler_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_baki_aday,
		translate(trim(to_char(gotokaler_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') gotokaler_baki_aday_customers
	from data_vajapora.yesterday_transaction_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.yesterday_transaction_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_yesterday_transaction_stats(); 

select *
from data_vajapora.yesterday_transaction_stats; 
*/

/*
3.
৩০ দিনের বেশি বাকি ৳ ২৫,৮৭৫; 😟💰
১৭ জন কাস্টমারের কাছে এই বাকি আছে। বাকি আদায় দ্রুত করতে আজই তাগাদা মেসেজ পাঠান।
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_more_than_30_days_credit_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	-- retained merchants of today
	drop table if exists data_vajapora.retained_today_help;
	create table data_vajapora.retained_today_help as 
	select concat('0', mobile_no) mobile_no
	from data_vajapora.retained_today; 
	
	-- retained merchants' last txns with their customers
	drop table if exists data_vajapora.last_txn_with_customer;
	create table data_vajapora.last_txn_with_customer as
	select mobile_no, contact, max(jr_ac_id) last_txn_id 
	from 
		(select mobile_no, contact, jr_ac_id 
		from tallykhata.tallykhata_fact_info_final
		) tbl1
		inner join 
		data_vajapora.retained_today_help tbl2 using(mobile_no)
	group by 1, 2; 
	
	-- retained merchants' last txns with their customers (cases in which credits were not returned in >=30 days)
	drop table if exists data_vajapora.more_than_30_days_credit_stats_help;
	create table data_vajapora.more_than_30_days_credit_stats_help as
	select *
	from 
		(select mobile_no, sum(input_amount) baki_30_or_more_days, count(contact) baki_30_or_more_days_customers
		from 
			data_vajapora.last_txn_with_customer tbl1 
			
			inner join 
			
			(select jr_ac_id last_txn_id, input_amount
			from tallykhata.tallykhata_fact_info_final 
			where 
				current_date-created_datetime>=30
				and txn_type='CREDIT_SALE'
			) tbl2 using(last_txn_id)
		group by 1
		) tbl1 
	
		left join 
				
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 using(mobile_no); 
	
	-- translate into Bangla
	drop table if exists data_vajapora.more_than_30_days_credit_stats;
	create table data_vajapora.more_than_30_days_credit_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(baki_30_or_more_days, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_30_or_more_days,
		translate(trim(to_char(baki_30_or_more_days_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_30_or_more_days_customers
	from data_vajapora.more_than_30_days_credit_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.retained_today_help;
	drop table if exists data_vajapora.last_txn_with_customer;
	drop table if exists data_vajapora.more_than_30_days_credit_stats_help;

END;
$function$
;

/*
select data_vajapora.fn_more_than_30_days_credit_stats(); 

select *
from data_vajapora.more_than_30_days_credit_stats; 
*/

/*
5.
এই মাসে বাকি বেচা ৳ ১২,৯০০ (১৫ কাস্টমার) 📅
বাকি আদায় ৳ ৩৬,৪৫২ (৭ কাস্টমার)💸
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_credit_sales_this_month_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	-- generate this month's statistics
	drop table if exists data_vajapora.credit_sales_this_month_stats_help; 
	create table data_vajapora.credit_sales_this_month_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ei_masher_becha is not null then ei_masher_becha else 0 end ei_masher_becha,
		case when ei_masher_baki_becha is not null then ei_masher_baki_becha else 0 end ei_masher_baki_becha,
		case when ei_masher_baki_becha_customers is not null then ei_masher_baki_becha_customers else 0 end ei_masher_baki_becha_customers,
		case when ei_masher_kena is not null then ei_masher_kena else 0 end ei_masher_kena,
		case when ei_masher_khoroch is not null then ei_masher_khoroch else 0 end ei_masher_khoroch,
		case when ei_masher_baki_aday is not null then ei_masher_baki_aday else 0 end ei_masher_baki_aday,
		case when ei_masher_baki_aday_customers is not null then ei_masher_baki_aday_customers else 0 end ei_masher_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- this month's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) ei_masher_becha,
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) ei_masher_baki_becha,
			count(distinct case when txn_type in('CREDIT_SALE') then contact else null end) ei_masher_baki_becha_customers,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) ei_masher_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) ei_masher_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) ei_masher_baki_aday,
			count(distinct case when txn_type in('CREDIT_SALE_RETURN') then contact else null end) ei_masher_baki_aday_customers
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=to_char(current_date, 'YYYY-MM')
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	-- translate statistics into Bangla
	drop table if exists data_vajapora.credit_sales_this_month_stats;
	create table data_vajapora.credit_sales_this_month_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(ei_masher_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_becha,
		translate(trim(to_char(ei_masher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha,
		translate(trim(to_char(ei_masher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha_customers,
		translate(trim(to_char(ei_masher_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_kena,
		translate(trim(to_char(ei_masher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_khoroch,
		translate(trim(to_char(ei_masher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday,
		translate(trim(to_char(ei_masher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday_customers
	from data_vajapora.credit_sales_this_month_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.credit_sales_this_month_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_credit_sales_this_month_stats();

select *
from data_vajapora.credit_sales_this_month_stats; 
*/

/*
6. 
এই সপ্তাহে বাকি আদায় ৳ ১,৯০০; বাকি বেচা ৳ ৩,৫৫০; 💸🔽
বাকি বেচা ৫ জনের কাছে; বাকি আদায় ২ জন থেকে।
মোট বেচা ৳ ১২,৩৭৫; কেনা ৳ ২,৫৪০; খরচ ৳ ৬৫০; কোন এন্ট্রি মিস হলে এখনই টালিখাতা অ্যাপে লিখে নিন!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_weekly_transaction_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	-- summarize this week's activities 
	drop table if exists data_vajapora.weekly_activity_stats_help;
	create table data_vajapora.weekly_activity_stats_help as
	select 
		mobile_no, 
		sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then amount else 0 end) ei_soptaher_becha,
		sum(case when txn_type in('CREDIT_SALE') then amount else 0 end) ei_soptaher_baki_becha,
		count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end) ei_soptaher_baki_becha_customers,
		sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end)+sum(case when txn_type in('CREDIT_PURCHASE') then amount_received else 0 end) ei_soptaher_kena,                                            
		sum(case when txn_type in('EXPENSE') then amount else 0 end) ei_soptaher_khoroch,
		sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end) ei_soptaher_baki_aday,
		count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end) ei_soptaher_baki_aday_customers
	from 
		(select mobile_no, account_id, txn_type, journal_tbl_id jr_id
		from tallykhata.tallykhata_user_transaction_info
		where date(created_datetime)>=current_date-7 and date(created_datetime)<current_date
		) tbl1
		
		inner join 
			
		(select id jr_id, mobile_no, amount, amount_received
		from public.journal 
		where 
			is_active is true
			and date(create_date)>=current_date-7 and date(create_date)<current_date
		) tbl2 using(jr_id, mobile_no)
	group by 1; 
	
	-- generate this week's statistics
	drop table if exists data_vajapora.weekly_transaction_stats_help; 
	create table data_vajapora.weekly_transaction_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ei_soptaher_becha is not null then ei_soptaher_becha else 0 end ei_soptaher_becha,
		case when ei_soptaher_baki_becha is not null then ei_soptaher_baki_becha else 0 end ei_soptaher_baki_becha,
		case when ei_soptaher_baki_becha_customers is not null then ei_soptaher_baki_becha_customers else 0 end ei_soptaher_baki_becha_customers,
		case when ei_soptaher_kena is not null then ei_soptaher_kena else 0 end ei_soptaher_kena,
		case when ei_soptaher_khoroch is not null then ei_soptaher_khoroch else 0 end ei_soptaher_khoroch,
		case when ei_soptaher_baki_aday is not null then ei_soptaher_baki_aday else 0 end ei_soptaher_baki_aday,
		case when ei_soptaher_baki_aday_customers is not null then ei_soptaher_baki_aday_customers else 0 end ei_soptaher_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		-- this week's activities
		data_vajapora.weekly_activity_stats_help tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	-- translate statistics into Bangla
	drop table if exists data_vajapora.weekly_transaction_stats;
	create table data_vajapora.weekly_transaction_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(ei_soptaher_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_becha,
		translate(trim(to_char(ei_soptaher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_becha,
		translate(trim(to_char(ei_soptaher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_becha_customers,
		translate(trim(to_char(ei_soptaher_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_kena,
		translate(trim(to_char(ei_soptaher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_khoroch,
		translate(trim(to_char(ei_soptaher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_aday,
		translate(trim(to_char(ei_soptaher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_soptaher_baki_aday_customers
	from data_vajapora.weekly_transaction_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.weekly_activity_stats_help;
	drop table if exists data_vajapora.weekly_transaction_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_weekly_transaction_stats(); 

select *
from data_vajapora.weekly_transaction_stats;
*/

/*
7.
প্রতিমাসে গড় বাকি বেচা ৳ ৬৫,০০০ (৭০ কাস্টমার); 📈💡
প্রতিমাসে গড় বাকি আদায় ৬০,০০০ (৬৫ কাস্টমার)। 
বাকি বেচা আগস্ট ৳ ১৩২৩১, জুলাই ২৩৪২৩; জুন ২৩৪২৩।
বাকি আদায় আগস্ট ৳ ১৩২৩১, জুলাই ২৩৪২৩; জুন ২৩৪২৩।
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_last_3_months_transaction_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare
	-- last 3 months' terminal dates
	var_start_date date:=to_char(to_char(current_date, 'YYYY-MM-01')::date-75, 'YYYY-MM-01')::date;
	var_end_date date:=to_char(current_date, 'YYYY-MM-01')::date;
begin
	
	-- generate last 3 months' statistics
	drop table if exists data_vajapora.last_3_months_transaction_stats_help; 
	create table data_vajapora.last_3_months_transaction_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when previous_month_1_baki_becha is not null then previous_month_1_baki_becha else 0 end previous_month_1_baki_becha,
		case when previous_month_1_baki_aday is not null then previous_month_1_baki_aday else 0 end previous_month_1_baki_aday,
		case when previous_month_2_baki_becha is not null then previous_month_2_baki_becha else 0 end previous_month_2_baki_becha,
		case when previous_month_2_baki_aday is not null then previous_month_2_baki_aday else 0 end previous_month_2_baki_aday,
		case when previous_month_3_baki_becha is not null then previous_month_3_baki_becha else 0 end previous_month_3_baki_becha,
		case when previous_month_3_baki_aday is not null then previous_month_3_baki_aday else 0 end previous_month_3_baki_aday,
		case when previous_month_3_baki_becha_customers is not null then previous_month_3_baki_becha_customers else 0 end previous_month_3_baki_becha_customers,
		case when previous_month_3_baki_aday_customers is not null then previous_month_3_baki_aday_customers else 0 end previous_month_3_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- this month-1's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) previous_month_1_baki_becha,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) previous_month_1_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-15, 'YYYY-MM')
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
		
		left join 
		
		(-- this month-2's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) previous_month_2_baki_becha,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) previous_month_2_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-45, 'YYYY-MM')
		group by 1
		) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
		
		left join 
		
		(-- this month-3's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) previous_month_3_baki_becha,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) previous_month_3_baki_aday
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-75, 'YYYY-MM')
		group by 1
		) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
	
		left join 
		
		(-- last 3 months' customers
		select 
			mobile_no, 
			count(distinct case when txn_type in('CREDIT_SALE') then contact else null end) previous_month_3_baki_becha_customers,
			count(distinct case when txn_type in('CREDIT_SALE_RETURN') then contact else null end) previous_month_3_baki_aday_customers
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>=var_start_date and created_datetime<var_end_date
		group by 1
		) tbl6 on(tbl1.mobile_no=tbl6.mobile_no); 
	
	-- translate statistics into Bangla
	drop table if exists data_vajapora.last_3_months_transaction_stats;
	create table data_vajapora.last_3_months_transaction_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(previous_month_1_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_1_baki_becha,
		translate(trim(to_char(previous_month_1_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_1_baki_aday,
		translate(trim(to_char(previous_month_2_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_2_baki_becha,
		translate(trim(to_char(previous_month_2_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_2_baki_aday,
		translate(trim(to_char(previous_month_3_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_3_baki_becha,
		translate(trim(to_char(previous_month_3_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_3_baki_aday,
		translate(trim(to_char(((previous_month_1_baki_becha+previous_month_2_baki_becha+previous_month_3_baki_becha)/3.00), '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_3_months_avg_baki_becha,        
		translate(trim(to_char(((previous_month_1_baki_aday+previous_month_2_baki_aday+previous_month_3_baki_aday)/3.00), '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_3_months_avg_baki_aday,
		translate(trim(to_char(previous_month_3_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_3_baki_becha_customers,
		translate(trim(to_char(previous_month_3_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') previous_month_3_baki_aday_customers
	from data_vajapora.last_3_months_transaction_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.last_3_months_transaction_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_last_3_months_transaction_stats(); 

select *
from data_vajapora.last_3_months_transaction_stats;
*/

/*
4. 
টপ ৫ বাকি কাস্টমার। পাবেন ৳ ৩৭,২৯০✨👇
কাস্টমার-১ ৳ ১০,১৫০; কাস্টমার-২ ৳ ৮,৫৪০; কাস্টমার-৩ ৳ ৭,৩৫০; কাস্টমার-৪ ৳ ৬,১৫০; কাস্টমার-৫ ৳ ৫,১০০।
নিয়মিত আপডেট পেতে প্রতিদিন টালিখাতা অ্যাপে হিসাব রাখুন।
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_top_baki_customer_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	-- retained merchants of today
	drop table if exists data_vajapora.retained_today_help;
	create table data_vajapora.retained_today_help as 
	select concat('0', mobile_no) mobile_no
	from data_vajapora.retained_today; 
	
	-- merchants' customers' names
	drop table if exists data_vajapora.merchant_customer_names;
	create table data_vajapora.merchant_customer_names as
	select *
	from 
		(select mobile_no, contact, max(id) max_id
		from public.account 
		where type=2
		group by 1, 2
		) tbl1 
		
		inner join 
		
		(select id max_id, name
		from public.account  
		) tbl2 using(max_id); 
	
	-- retained merchants' baki customers
	drop table if exists data_vajapora.baki_from_customers;
	create table data_vajapora.baki_from_customers as
	select 
		mobile_no, 
		contact, 
		sum(case when txn_type='CREDIT_SALE' then input_amount else 0 end)-sum(case when txn_type='CREDIT_SALE_RETURN' then input_amount else 0 end) baki 
	from 
		(select mobile_no, contact, input_amount, txn_type
		from tallykhata.tallykhata_fact_info_final
		where txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
		) tbl1
		inner join 
		data_vajapora.retained_today_help tbl2 using(mobile_no)
	group by 1, 2; 
	
	-- retained merchants' sequenced baki customers
	drop table if exists data_vajapora.baki_from_customers_seq;
	create table data_vajapora.baki_from_customers_seq as
	select *
	from 
		(select *, row_number() over(partition by mobile_no order by baki desc) baki_customer_seq
		from data_vajapora.baki_from_customers
		where baki>0
		) tbl1
		left join 
		data_vajapora.merchant_customer_names tbl2 using(mobile_no, contact); 
	
	-- top-05 baki customers
	drop table if exists data_vajapora.top_baki_customer_stats_help;
	create table data_vajapora.top_baki_customer_stats_help as
	select 
		mobile_no, shop_name,
		top_baki_customers, 
		top_1_baki_customer, case when top_1_baki is null then 0 else top_1_baki end top_1_baki, 
		top_2_baki_customer, case when top_2_baki is null then 0 else top_2_baki end top_2_baki, 
		top_3_baki_customer, case when top_3_baki is null then 0 else top_3_baki end top_3_baki, 
		top_4_baki_customer, case when top_4_baki is null then 0 else top_4_baki end top_4_baki, 
		top_5_baki_customer, case when top_5_baki is null then 0 else top_5_baki end top_5_baki
	from 
		(select mobile_no, max(baki_customer_seq) top_baki_customers
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq<=5
		group by 1
		) tbl0 
		
		left join 
	
		(select mobile_no, name top_1_baki_customer, baki top_1_baki
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq=1
		) tbl1 using(mobile_no)
		
		left join 
		
		(select mobile_no, name top_2_baki_customer, baki top_2_baki
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq=2
		) tbl2 using(mobile_no)
		
		left join 
		
		(select mobile_no, name top_3_baki_customer, baki top_3_baki
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq=3
		) tbl3 using(mobile_no)
		
		left join 
		
		(select mobile_no, name top_4_baki_customer, baki top_4_baki
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq=4
		) tbl4 using(mobile_no)
		
		left join 
		
		(select mobile_no, name top_5_baki_customer, baki top_5_baki
		from data_vajapora.baki_from_customers_seq
		where baki_customer_seq=5
		) tbl5 using(mobile_no)
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl6 using(mobile_no); 
	
	-- translate into Bangla
	drop table if exists data_vajapora.top_baki_customer_stats;
	create table data_vajapora.top_baki_customer_stats as
	select 
		mobile_no,
		shop_name,
		translate(top_baki_customers::text, '0123456789', '০১২৩৪৫৬৭৮৯') top_baki_customers, 
		top_1_baki_customer,
		translate(trim(to_char(top_1_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_1_baki,
		top_2_baki_customer,
		translate(trim(to_char(top_2_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_2_baki,
		top_3_baki_customer,
		translate(trim(to_char(top_3_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_3_baki,
		top_4_baki_customer,
		translate(trim(to_char(top_4_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_4_baki,
		top_5_baki_customer,
		translate(trim(to_char(top_5_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_5_baki,
		translate(trim(to_char(top_1_baki+top_2_baki+top_3_baki+top_4_baki+top_5_baki, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') top_total_baki
	from data_vajapora.top_baki_customer_stats_help;
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.retained_today_help;
	drop table if exists data_vajapora.merchant_customer_names;
	drop table if exists data_vajapora.baki_from_customers;
	drop table if exists data_vajapora.baki_from_customers_seq;
	drop table if exists data_vajapora.top_baki_customer_stats_help;

END;
$function$
;

/*
select data_vajapora.fn_top_baki_customer_stats(); 

select *
from data_vajapora.top_baki_customer_stats;
*/

/*
2. 
আগস্টে বাকি আদায় ৳ ১,৯০০ (৭ জন) 📊👇
বাকি বেচা ৳ ৩,৫৫০ (২ জন)।
মোট বেচা ৳ ১২,৩৭৫; কেনা ৳ ২,৫৪০; খরচ ৳ ৬৫০; কোন এন্ট্রি মিস হলে এখনই টালিখাতা অ্যাপে লিখে নিন!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_credit_sales_previous_month_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	-- generate previous month's statistics
	drop table if exists data_vajapora.credit_sales_previous_month_stats_help; 
	create table data_vajapora.credit_sales_previous_month_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ei_masher_becha is not null then ei_masher_becha else 0 end ei_masher_becha,
		case when ei_masher_baki_becha is not null then ei_masher_baki_becha else 0 end ei_masher_baki_becha,
		case when ei_masher_baki_becha_customers is not null then ei_masher_baki_becha_customers else 0 end ei_masher_baki_becha_customers,
		case when ei_masher_kena is not null then ei_masher_kena else 0 end ei_masher_kena,
		case when ei_masher_khoroch is not null then ei_masher_khoroch else 0 end ei_masher_khoroch,
		case when ei_masher_baki_aday is not null then ei_masher_baki_aday else 0 end ei_masher_baki_aday,
		case when ei_masher_baki_aday_customers is not null then ei_masher_baki_aday_customers else 0 end ei_masher_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		(-- this month's activities
		select 
			mobile_no, 
			sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then input_amount else 0 end) ei_masher_becha,
			sum(case when txn_type in('CREDIT_SALE') then input_amount else 0 end) ei_masher_baki_becha,
			count(distinct case when txn_type in('CREDIT_SALE') then contact else null end) ei_masher_baki_becha_customers,
			sum(case when txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE') then input_amount else 0 end) ei_masher_kena,
			sum(case when txn_type in('EXPENSE') then input_amount else 0 end) ei_masher_khoroch,
			sum(case when txn_type in('CREDIT_SALE_RETURN') then input_amount else 0 end) ei_masher_baki_aday,
			count(distinct case when txn_type in('CREDIT_SALE_RETURN') then contact else null end) ei_masher_baki_aday_customers
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-15, 'YYYY-MM')
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	-- translate statistics into Bangla
	drop table if exists data_vajapora.credit_sales_previous_month_stats;
	create table data_vajapora.credit_sales_previous_month_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(ei_masher_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_becha,
		translate(trim(to_char(ei_masher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha,
		translate(trim(to_char(ei_masher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha_customers,
		translate(trim(to_char(ei_masher_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_kena,
		translate(trim(to_char(ei_masher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_khoroch,
		translate(trim(to_char(ei_masher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday,
		translate(trim(to_char(ei_masher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday_customers
	from data_vajapora.credit_sales_previous_month_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.credit_sales_previous_month_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_credit_sales_previous_month_stats();

select *
from data_vajapora.credit_sales_previous_month_stats; 
*/

/*
9.
গত ৭ দিনে ৫ দিন হিসাব এন্ট্রি করেছেন।📅📲
ব্যবসার পুরো হিসাব কন্ট্রোলে রাখতে প্রতিদিন হিসাব রাখুন টালিখাতা অ্যাপে।🤹🏻‍♂️
বিঃ দ্রঃ রিপোর্টটি ১ সেপ্টেম্বর থেকে ৭ সেপ্টেম্বর, ২০২১ - রাত ১২:০০ টা পর্যন্ত!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_usage_last_7_days_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	
	-- last 7 days' usage summary
	drop table if exists data_vajapora.usage_last_7_days_help;
	create table data_vajapora.usage_last_7_days_help as
	select mobile_no, count(distinct date(create_date)) entry_last_7_days, count(distinct account_id) entry_last_7_days_cs
	from public.journal 
	where date(create_date)>=current_date-7 and date(create_date)<current_date
	group by 1; 
	
	-- generate last 7 days' usage statistics (holistic)
	drop table if exists data_vajapora.usage_last_7_days_help_2; 
	create table data_vajapora.usage_last_7_days_help_2 as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when entry_last_7_days is not null then entry_last_7_days else 0 end entry_last_7_days,
		case when entry_last_7_days_cs is not null then entry_last_7_days_cs else 0 end entry_last_7_days_cs
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		-- last 7 days' usage
		data_vajapora.usage_last_7_days_help tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
	
	-- translate statistics into Bangla: 0 activities
	drop table if exists data_vajapora.usage_last_7_days_stats_zero;
	create table data_vajapora.usage_last_7_days_stats_zero as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(entry_last_7_days, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') entry_last_7_days,
		translate(trim(to_char(entry_last_7_days_cs, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') entry_last_7_days_cs
	from data_vajapora.usage_last_7_days_help_2
	where entry_last_7_days=0; 
	
	-- translate statistics into Bangla: >0 activities
	drop table if exists data_vajapora.usage_last_7_days_stats_nonzero;
	create table data_vajapora.usage_last_7_days_stats_nonzero as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(entry_last_7_days, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') entry_last_7_days,
		translate(trim(to_char(entry_last_7_days_cs, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') entry_last_7_days_cs
	from data_vajapora.usage_last_7_days_help_2
	where entry_last_7_days!=0; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.usage_last_7_days_help;
	drop table if exists data_vajapora.usage_last_7_days_help_2; 

END;
$function$
;

/*
select data_vajapora.fn_usage_last_7_days_stats();

select *
from data_vajapora.usage_last_7_days_stats_zero;

select *
from data_vajapora.usage_last_7_days_stats_nonzero;
*/

/*
8.
Active in September
সেপ্টেম্বর মাসে মোট বাকি আদায় ৳ ১,৯০০ (৭ জন) 📊👇

বাকি বেচা ৳ ৩,৫৫০ (২ জন)।
মোট বেচা ৳ ১২,৩৭৫; কেনা ৳ ২,৫৪০; খরচ ৳ ৬৫০; কোন এন্ট্রি মিস হলে এখনই টালিখাতা অ্যাপে লিখে নিন!

বিঃ দ্রঃ ১ সেপ্টেম্বর থেকে ৮ সেপ্টেম্বর, ২০২১ - রাত ১২:০০ টা পর্যন্ত!

Active in August but inactive in September
আগস্ট মাসে মোট বাকি আদায় ৳ ১,৯০০ (৭ জন) 📊👇

বাকি বেচা ৳ ৩,৫৫০ (২ জন)।
মোট বেচা ৳ ১২,৩৭৫; কেনা ৳ ২,৫৪০; খরচ ৳ ৬৫০; সেপ্টেম্বর মাসের হিসাব গুলো এন্ট্রি করে ফেলুন!

বিঃ দ্রঃ রিপোর্টটি ১ আগস্ট থেকে ৩১ আগস্ট, ২০২১ - রাত ১২:০০ টা পর্যন্ত!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_this_or_previous_month_transaction_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare
	-- first date of months
	var_start_date date:=to_char(current_date, 'YYYY-MM-01')::date; 
	var_start_date_prev_month date:=to_char(to_char(current_date, 'YYYY-MM-01')::date-15, 'YYYY-MM-01')::date; 
begin
	-- summarize this month's activities 
	drop table if exists data_vajapora.credit_sales_this_month_activities_help;
	create table data_vajapora.credit_sales_this_month_activities_help as
	select 
		mobile_no, 
		sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then amount else 0 end) ei_masher_becha,
		sum(case when txn_type in('CREDIT_SALE') then amount else 0 end) ei_masher_baki_becha,
		count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end) ei_masher_baki_becha_customers,
		sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end)+sum(case when txn_type in('CREDIT_PURCHASE') then amount_received else 0 end) ei_masher_kena,                                            
		sum(case when txn_type in('EXPENSE') then amount else 0 end) ei_masher_khoroch,
		sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end) ei_masher_baki_aday,
		count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end) ei_masher_baki_aday_customers
	from 
		(select mobile_no, account_id, txn_type, journal_tbl_id jr_id
		from tallykhata.tallykhata_user_transaction_info
		where date(created_datetime)>=var_start_date and date(created_datetime)<current_date
		) tbl1
		
		inner join 
			
		(select id jr_id, mobile_no, amount, amount_received
		from public.journal 
		where 
			is_active is true
			and date(create_date)>=var_start_date and date(create_date)<current_date
		) tbl2 using(jr_id, mobile_no)
	group by 1; 
	
	-- generate this month's statistics
	drop table if exists data_vajapora.credit_sales_this_month_stats_help; 
	create table data_vajapora.credit_sales_this_month_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when ei_masher_becha is not null then ei_masher_becha else 0 end ei_masher_becha,
		case when ei_masher_baki_becha is not null then ei_masher_baki_becha else 0 end ei_masher_baki_becha,
		case when ei_masher_baki_becha_customers is not null then ei_masher_baki_becha_customers else 0 end ei_masher_baki_becha_customers,
		case when ei_masher_kena is not null then ei_masher_kena else 0 end ei_masher_kena,
		case when ei_masher_khoroch is not null then ei_masher_khoroch else 0 end ei_masher_khoroch,
		case when ei_masher_baki_aday is not null then ei_masher_baki_aday else 0 end ei_masher_baki_aday,
		case when ei_masher_baki_aday_customers is not null then ei_masher_baki_aday_customers else 0 end ei_masher_baki_aday_customers
	from 
		-- this month's activities
		data_vajapora.credit_sales_this_month_activities_help tbl1
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no);
		
	-- translate this month's statistics into Bangla
	drop table if exists data_vajapora.credit_sales_this_month_stats;
	create table data_vajapora.credit_sales_this_month_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(ei_masher_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_becha,
		translate(trim(to_char(ei_masher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha,
		translate(trim(to_char(ei_masher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_becha_customers,
		translate(trim(to_char(ei_masher_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_kena,
		translate(trim(to_char(ei_masher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_khoroch,
		translate(trim(to_char(ei_masher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday,
		translate(trim(to_char(ei_masher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_masher_baki_aday_customers
	from data_vajapora.credit_sales_this_month_stats_help; 




	-- summarize previous month's activities 
	drop table if exists data_vajapora.credit_sales_previous_month_activities_help;
	create table data_vajapora.credit_sales_previous_month_activities_help as
	select 
		mobile_no, 
		sum(case when txn_type in('CASH_SALE', 'CREDIT_SALE') then amount else 0 end) goto_masher_becha,
		sum(case when txn_type in('CREDIT_SALE') then amount else 0 end) goto_masher_baki_becha,
		count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end) goto_masher_baki_becha_customers,
		sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end)+sum(case when txn_type in('CREDIT_PURCHASE') then amount_received else 0 end) goto_masher_kena,                                            
		sum(case when txn_type in('EXPENSE') then amount else 0 end) goto_masher_khoroch,
		sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end) goto_masher_baki_aday,
		count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end) goto_masher_baki_aday_customers
	from 
		(select mobile_no, account_id, txn_type, journal_tbl_id jr_id
		from tallykhata.tallykhata_user_transaction_info
		where date(created_datetime)>=var_start_date_prev_month and date(created_datetime)<var_start_date
		) tbl1
		
		inner join 
			
		(select id jr_id, mobile_no, amount, amount_received
		from public.journal 
		where 
			is_active is true
			and date(create_date)>=var_start_date_prev_month and date(create_date)<var_start_date
		) tbl2 using(jr_id, mobile_no)
	group by 1;  
	
	-- generate previous month's statistics
	drop table if exists data_vajapora.credit_sales_previous_month_stats_help; 
	create table data_vajapora.credit_sales_previous_month_stats_help as
	select 
		tbl1.mobile_no,
		shop_name, 
		case when goto_masher_becha is not null then goto_masher_becha else 0 end goto_masher_becha,
		case when goto_masher_baki_becha is not null then goto_masher_baki_becha else 0 end goto_masher_baki_becha,
		case when goto_masher_baki_becha_customers is not null then goto_masher_baki_becha_customers else 0 end goto_masher_baki_becha_customers,
		case when goto_masher_kena is not null then goto_masher_kena else 0 end goto_masher_kena,
		case when goto_masher_khoroch is not null then goto_masher_khoroch else 0 end goto_masher_khoroch,
		case when goto_masher_baki_aday is not null then goto_masher_baki_aday else 0 end goto_masher_baki_aday,
		case when goto_masher_baki_aday_customers is not null then goto_masher_baki_aday_customers else 0 end goto_masher_baki_aday_customers
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		left join 
		
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
		
		left join 
		
		-- previous month's activities
		data_vajapora.credit_sales_previous_month_activities_help tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
		
		left join 
	
		(-- excluding users having activity this month 
		select mobile_no
		from data_vajapora.credit_sales_this_month_stats_help
		) tbl4 on(tbl1.mobile_no=tbl4.mobile_no) 
	where tbl4.mobile_no is null; 
	
	-- translate previous month's statistics into Bangla
	drop table if exists data_vajapora.credit_sales_previous_month_stats;
	create table data_vajapora.credit_sales_previous_month_stats as
	select 
		mobile_no,
		shop_name, 
		translate(trim(to_char(goto_masher_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_becha,
		translate(trim(to_char(goto_masher_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_baki_becha,
		translate(trim(to_char(goto_masher_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_baki_becha_customers,
		translate(trim(to_char(goto_masher_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_kena,
		translate(trim(to_char(goto_masher_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_khoroch,
		translate(trim(to_char(goto_masher_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_baki_aday,
		translate(trim(to_char(goto_masher_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') goto_masher_baki_aday_customers
	from data_vajapora.credit_sales_previous_month_stats_help; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.credit_sales_this_month_activities_help;
	drop table if exists data_vajapora.credit_sales_this_month_stats_help; 
	drop table if exists data_vajapora.credit_sales_previous_month_activities_help;
	drop table if exists data_vajapora.credit_sales_previous_month_stats_help; 

END;
$function$
;

/*
select data_vajapora.fn_this_or_previous_month_transaction_stats(); 

select *
from data_vajapora.credit_sales_this_month_stats; 

select *
from data_vajapora.credit_sales_previous_month_stats;
*/

/*
10.

If increase credit collection

অভিনন্দন! আগস্ট মাসে ৳ ২৫,৫৯০ বেশি আদায় হয়েছে।🥳👏

জুলাই মাসের থেকে আগস্ট মাসে ৳ ২৫,৫৯০ বাকি আদায় বেশি হয়েছে।  
আগস্ট মাসে বাকি আদায় ৳ ৮৫,৫৬৫
জুলাই মাসে বাকি আদায় ৳ ৫৯,৯৭৫ 

বাকি আদায় বাড়াতে নিয়মিত হিসাব রাখুন টালিখাতা অ্যাপে!

বিঃ দ্রঃ রিপোর্টটি ১ জুলাই থেকে ৩১ আগস্ট, ২০২১ - রাত ১২:০০ টা পর্যন্ত!


If reduce credit collection

জানেন কি❓ আপনার বাকি আদায় কম হচ্ছে❗😟🔥

জুলাই মাসের থেকে আগস্ট মাসে ৳ ২৫,৫৯০ বাকি আদায় কম হয়েছে❗  
আগস্ট মাসে বাকি আদায় ৳ ৫৯,৯৭৫
জুলাই মাসে বাকি আদায় ৳ ৮৫,৫৬৫

বাকি আদায় কম হলে আপনার ব্যবসায় পুঁজি কমে যাবে! বাকি আদায় বাড়াতে কাস্টমারদের তাগাদা মেসেজ পাঠান!

বিঃ দ্রঃ রিপোর্টটি ১ জুলাই থেকে ৩১ আগস্ট, ২০২১ - রাত ১২:০০ টা পর্যন্ত!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_inspire_credit_return_monthly_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	-- baki aday 'this month'-1
	drop table if exists data_vajapora.baki_aday_month_1_help;
	create table data_vajapora.baki_aday_month_1_help as
	select 
		mobile_no, 
		sum(amount_received) baki_aday_month_1
	from public.journal 
	where 
		is_active is true
		and to_char(create_date::date, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-15, 'YYYY-MM')
		and txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 
	group by 1; 
	
	-- baki aday 'this month'-2
	drop table if exists data_vajapora.baki_aday_month_2_help;
	create table data_vajapora.baki_aday_month_2_help as
	select 
		mobile_no, 
		sum(amount_received) baki_aday_month_2
	from public.journal 
	where 
		is_active is true
		and to_char(create_date::date, 'YYYY-MM')=to_char(to_char(current_date, 'YYYY-MM-01')::date-45, 'YYYY-MM')
		and txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 
	group by 1; 
	
	-- combined monthly baki aday statistics
	drop table if exists data_vajapora.inspire_monthly_baki_aday_stats_help;
	create table data_vajapora.inspire_monthly_baki_aday_stats_help as
	select 
		tbl0.mobile_no, shop_name,
		case when baki_aday_month_1 is null then 0 else baki_aday_month_1 end baki_aday_month_1, 
		case when baki_aday_month_2 is null then 0 else baki_aday_month_2 end baki_aday_month_2, 
		case when baki_aday_month_1 is null then 0 else baki_aday_month_1 end-case when baki_aday_month_2 is null then 0 else baki_aday_month_2 end baki_aday_month_1_diff
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl0
		
		left join 
	
		data_vajapora.baki_aday_month_1_help tbl1 on(tbl0.mobile_no=tbl1.mobile_no)
		
		left join 
		
		data_vajapora.baki_aday_month_2_help tbl2 on(tbl0.mobile_no=tbl2.mobile_no)
		
		left join 
						
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl3 on(tbl0.mobile_no=tbl3.mobile_no); 
	
	-- translate into Bangla: beshi cases 
	drop table if exists data_vajapora.inspire_monthly_baki_aday_beshi_stats;
	create table data_vajapora.inspire_monthly_baki_aday_beshi_stats as
	select
		mobile_no,
		shop_name, 
		translate(trim(to_char(baki_aday_month_1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_1,
		translate(trim(to_char(baki_aday_month_2, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_2,
		translate(trim(to_char(baki_aday_month_1_diff, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_1_beshi
	from data_vajapora.inspire_monthly_baki_aday_stats_help
	where baki_aday_month_1_diff>0; 
	
	-- translate into Bangla: kom cases 
	drop table if exists data_vajapora.inspire_monthly_baki_aday_kom_stats;
	create table data_vajapora.inspire_monthly_baki_aday_kom_stats as
	select
		mobile_no,
		shop_name, 
		translate(trim(to_char(baki_aday_month_1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_1,
		translate(trim(to_char(baki_aday_month_2, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_2,
		translate(trim(to_char(baki_aday_month_1_diff*-1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_month_1_kom
	from data_vajapora.inspire_monthly_baki_aday_stats_help
	where baki_aday_month_1_diff<0; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.baki_aday_month_1_help;
	drop table if exists data_vajapora.baki_aday_month_2_help;
	drop table if exists data_vajapora.inspire_monthly_baki_aday_stats_help;
	
END;
$function$
;

/*
select data_vajapora.fn_inspire_credit_return_monthly_stats();

select count(*)
from data_vajapora.inspire_monthly_baki_aday_beshi_stats;

select count(*)
from data_vajapora.inspire_monthly_baki_aday_kom_stats;
*/

/*
11.

If increase credit collection

অভিনন্দন! এই সপ্তাহে ৳ ২৫,৫৯০ বেশি আদায় হয়েছে।🥳👏

গত সপ্তাহের থেকে এই সপ্তাহে ৳ ২৫,৫৯০ বাকি আদায় বেশি হয়েছে।  

এই সপ্তাহে বাকি আদায় ৳ ৮৫,৫৬৫ (১০ সেপ্টেম্বর থেকে ১৬ সেপ্টেম্বর)
গত সপ্তাহে বাকি আদায় ৳ ৫৯,৯৭৫ (৩ সেপ্টেম্বর থেকে ৯ সেপ্টেম্বর)

বাকি আদায় বাড়াতে নিয়মিত হিসাব রাখুন টালিখাতা অ্যাপে!


বিঃ দ্রঃ রিপোর্টটি ৩ সেপ্টেম্বর থেকে ১৬ সেপ্টেম্বর, ২০২১ - রাত ১২:০০ টা পর্যন্ত!


If reduce credit collection

জানেন কি❓ আপনার বাকি আদায় কম হচ্ছে❗😟🔥

গত সপ্তাহের থেকে এই সপ্তাহে ৳ ২৫,৫৯০ বাকি আদায় কম হয়েছে❗  

এই সপ্তাহে বাকি আদায় ৳ ৫৯,৯৭৫ (১০ সেপ্টেম্বর থেকে ১৬ সেপ্টেম্বর)
গত সপ্তাহে বাকি আদায় ৳ ৮৫,৫৬৫ (৩ সেপ্টেম্বর থেকে ৯ সেপ্টেম্বর)

বাকি আদায় বাড়াতে কাস্টমারদের তাগাদা মেসেজ পাঠান!


বিঃ দ্রঃ রিপোর্টটি ৩ সেপ্টেম্বর থেকে ১৬ সেপ্টেম্বর, ২০২১ - রাত ১২:০০ টা পর্যন্ত!
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_inspire_credit_return_weekly_stats()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare

begin
	-- baki aday 'this week'-1
	drop table if exists data_vajapora.baki_aday_week_1_help;
	create table data_vajapora.baki_aday_week_1_help as
	select 
		mobile_no, 
		sum(amount_received) baki_aday_week_1
	from public.journal 
	where 
		is_active is true
		and date(create_date)>=current_date-7 and date(create_date)<current_date
		and txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 
	group by 1; 
	
	-- baki aday 'this week'-2
	drop table if exists data_vajapora.baki_aday_week_2_help;
	create table data_vajapora.baki_aday_week_2_help as
	select 
		mobile_no, 
		sum(amount_received) baki_aday_week_2
	from public.journal 
	where 
		is_active is true
		and date(create_date)>=current_date-14 and date(create_date)<current_date-7
		and txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 
	group by 1; 
	
	-- combined weekly baki aday statistics
	drop table if exists data_vajapora.inspire_weekly_baki_aday_stats_help;
	create table data_vajapora.inspire_weekly_baki_aday_stats_help as
	select 
		tbl0.mobile_no, shop_name,
		case when baki_aday_week_1 is null then 0 else baki_aday_week_1 end baki_aday_week_1, 
		case when baki_aday_week_2 is null then 0 else baki_aday_week_2 end baki_aday_week_2, 
		case when baki_aday_week_1 is null then 0 else baki_aday_week_1 end-case when baki_aday_week_2 is null then 0 else baki_aday_week_2 end baki_aday_week_1_diff      
	from 
		(-- retained today
		select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl0
		
		left join 
	
		data_vajapora.baki_aday_week_1_help tbl1 on(tbl0.mobile_no=tbl1.mobile_no)
		
		left join 
		
		data_vajapora.baki_aday_week_2_help tbl2 on(tbl0.mobile_no=tbl2.mobile_no)
		
		left join 
						
		(-- shop names
		select mobile mobile_no, case when shop_name is null then merchant_name else shop_name end shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl3 on(tbl0.mobile_no=tbl3.mobile_no); 
	
	-- translate into Bangla: beshi cases 
	drop table if exists data_vajapora.inspire_weekly_baki_aday_beshi_stats;
	create table data_vajapora.inspire_weekly_baki_aday_beshi_stats as
	select
		mobile_no,
		shop_name, 
		translate(trim(to_char(baki_aday_week_1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_1,
		translate(trim(to_char(baki_aday_week_2, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_2,
		translate(trim(to_char(baki_aday_week_1_diff, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_1_beshi
	from data_vajapora.inspire_weekly_baki_aday_stats_help
	where baki_aday_week_1_diff>0; 
	
	-- translate into Bangla: kom cases 
	drop table if exists data_vajapora.inspire_weekly_baki_aday_kom_stats;
	create table data_vajapora.inspire_weekly_baki_aday_kom_stats as
	select
		mobile_no,
		shop_name, 
		translate(trim(to_char(baki_aday_week_1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_1,
		translate(trim(to_char(baki_aday_week_2, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_2,
		translate(trim(to_char(baki_aday_week_1_diff*-1, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') baki_aday_week_1_kom
	from data_vajapora.inspire_weekly_baki_aday_stats_help
	where baki_aday_week_1_diff<0; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.baki_aday_week_1_help;
	drop table if exists data_vajapora.baki_aday_week_2_help;
	drop table if exists data_vajapora.inspire_weekly_baki_aday_stats_help;

END;
$function$
;
	
/*
select data_vajapora.fn_inspire_credit_return_weekly_stats();

select *
from data_vajapora.inspire_weekly_baki_aday_beshi_stats;
	
select *
from data_vajapora.inspire_weekly_baki_aday_kom_stats;
*/
