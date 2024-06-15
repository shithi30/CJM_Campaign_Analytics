/*
- Viz: 
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

As discussed, we want to send year closing reports(January 01 to December 31, 2021) to our merchants on January 01, 2022. 
Please provide us the data according to our personalized cumulative message data format!

*/

do $$ 

declare 

begin 
	-- TG
	drop table if exists data_vajapora.yearly_stats_help_e; 
	create table data_vajapora.yearly_stats_help_e as
	select mobile_no
	from cjm_segmentation.retained_users 
	where report_date=current_date; 
	raise notice 'TG generated.'; 
	
	-- entries from journal
	drop table if exists data_vajapora.yearly_stats_help_b; 
	create table data_vajapora.yearly_stats_help_b as
	select id jr_id, amount, amount_received
	from public.journal 
	where 
		is_active is true	
		and date_part('year', create_date::date)=2021; 
	raise notice 'Entries brought from journal.'; 
	
	-- corresponding txn types
	drop table if exists data_vajapora.yearly_stats_help_c; 
	create table data_vajapora.yearly_stats_help_c as
	select mobile_no, account_id, txn_type, journal_tbl_id jr_id
	from tallykhata.tallykhata_user_transaction_info
	where date_part('year', created_datetime)=2021; 
	raise notice 'Transaction types identified.';
	
	-- populate metrics
	drop table if exists data_vajapora.yearly_stats_help_a; 
	create table data_vajapora.yearly_stats_help_a as
	select 
		mobile_no, 
		
		coalesce(sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) ei_bochhorer_baki_becha,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE') then account_id else null end), 0) ei_bochhorer_baki_becha_customers,
		coalesce(sum(case when txn_type in('CREDIT_SALE_RETURN') then amount_received else 0 end), 0) ei_bochhorer_baki_aday,
		coalesce(count(distinct case when txn_type in('CREDIT_SALE_RETURN') then account_id else null end), 0) ei_bochhorer_baki_aday_customers,
	
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end), 0) ei_bochhorer_cash_becha,
		coalesce(sum(case when txn_type in('CASH_PURCHASE') then amount else 0 end), 0) ei_bochhorer_cash_kena,
		coalesce(sum(case when txn_type in('EXPENSE') then amount else 0 end), 0) ei_bochhorer_khoroch,
		
		coalesce(sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0) ei_bochhorer_malik_dilo,
		coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end), 0) ei_bochhorer_malik_nilo, 
		
		abs(coalesce(sum(case when txn_type in('MALIK_NILO') then amount else 0 end)-sum(case when txn_type in('MALIK_DILO') then amount else 0 end), 0)) ei_bochhorer_maliker_balance, 
		coalesce(sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then amount else 0 end)+sum(case when txn_type in('CREDIT_SALE') then amount else 0 end), 0) ei_bochhorer_mot_becha
	from 
		data_vajapora.yearly_stats_help_e tbl0 
		
		left join
	
		data_vajapora.yearly_stats_help_c tbl1 using(mobile_no)
		
		left join 
			
		data_vajapora.yearly_stats_help_b tbl2 using(jr_id)
	group by 1; 
	raise notice 'Metrics populated.'; 
	
	-- translate into Bangla, put seperators, bring shop-names
	drop table if exists data_vajapora.yearly_stats;
	create table data_vajapora.yearly_stats as
	select 
		mobile_no,  
		shop_name, 
		translate(trim(to_char(ei_bochhorer_baki_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_baki_becha, 
		translate(trim(to_char(ei_bochhorer_baki_becha_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_baki_becha_customers, 
		translate(trim(to_char(ei_bochhorer_baki_aday, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_baki_aday, 
		translate(trim(to_char(ei_bochhorer_baki_aday_customers, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_baki_aday_customers, 
		translate(trim(to_char(ei_bochhorer_cash_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_cash_becha, 
		translate(trim(to_char(ei_bochhorer_cash_kena, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_cash_kena, 
		translate(trim(to_char(ei_bochhorer_khoroch, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_khoroch, 
		translate(trim(to_char(ei_bochhorer_malik_dilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_malik_dilo, 
		translate(trim(to_char(ei_bochhorer_malik_nilo, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_malik_nilo, 
		translate(trim(to_char(ei_bochhorer_maliker_balance, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_maliker_balance, 
		translate(trim(to_char(ei_bochhorer_mot_becha, '999G999G999G999G999G999G999')), '0123456789', '০১২৩৪৫৬৭৮৯') ei_bochhorer_mot_becha, 
		row_number() over(order by mobile_no) seq
	from
		data_vajapora.yearly_stats_help_a tbl1 
		
		left join 
		
		(-- shop names
		select 
			mobile mobile_no, 
			coalesce(shop_name, merchant_name, 'প্রিয় ব্যবসায়ী') as shop_name
		from tallykhata.tallykhata_user_personal_info 
		) tbl2 using(mobile_no); 
	raise notice 'Data prepared.'; 
	
	-- split data
	drop table if exists data_vajapora.yearly_stats_1; 
	create table data_vajapora.yearly_stats_1 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq<=200000; 
	
	drop table if exists data_vajapora.yearly_stats_2; 
	create table data_vajapora.yearly_stats_2 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>200000 and seq<=400000; 

	drop table if exists data_vajapora.yearly_stats_3; 
	create table data_vajapora.yearly_stats_3 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>400000 and seq<=600000; 

	drop table if exists data_vajapora.yearly_stats_4; 
	create table data_vajapora.yearly_stats_4 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>600000 and seq<=800000; 

	drop table if exists data_vajapora.yearly_stats_5; 
	create table data_vajapora.yearly_stats_5 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>800000 and seq<=1000000; 
	
	drop table if exists data_vajapora.yearly_stats_6; 
	create table data_vajapora.yearly_stats_6 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>1000000 and seq<=1200000; 

	drop table if exists data_vajapora.yearly_stats_7; 
	create table data_vajapora.yearly_stats_7 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>1200000 and seq<=1400000; 

	drop table if exists data_vajapora.yearly_stats_8; 
	create table data_vajapora.yearly_stats_8 as
	select mobile_no, shop_name, ei_bochhorer_baki_becha, ei_bochhorer_baki_becha_customers, ei_bochhorer_baki_aday, ei_bochhorer_baki_aday_customers, ei_bochhorer_cash_becha, ei_bochhorer_cash_kena, ei_bochhorer_khoroch, ei_bochhorer_malik_dilo, ei_bochhorer_malik_nilo, ei_bochhorer_maliker_balance, ei_bochhorer_mot_becha
	from data_vajapora.yearly_stats
	where seq>1400000 and seq<=1600000; 
	
	raise notice 'Data split.'; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.yearly_stats_help_a; 
	drop table if exists data_vajapora.yearly_stats_help_b; 
	drop table if exists data_vajapora.yearly_stats_help_c; 
	drop table if exists data_vajapora.yearly_stats_help_e;
	-- drop table if exists data_vajapora.yearly_stats; 
	raise notice 'Auxiliary tables dropped.';
	
end $$; 

-- see data
(select *
from data_vajapora.yearly_stats_1 
limit 1000
)

union all 

(select *
from data_vajapora.yearly_stats_2
limit 1000
); 
