/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=692143853
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

Tasks: SPU Journey Analysis
1. Need to generate 20k SPU  whose age >=30days
2. Need to analyse them by:
	a) Customer/Supplier Add tendency
	b) Transaction Record Tendency
	c) Tagada SMS sending Tendency (Manual)
	d) SMS consumption Tendency (Auto SMS)
	e) Time Consumption Tendency
*/

/* c) Tagada SMS sending Tendency (Manual) */

-- merchant-wise max SMS-consumption timeframes
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	case 
		when seq=1 then '1 Tagada SMS'
		when seq>=2 and seq<=5 then '2-5 Tagada SMS'
		when seq>=6 and seq<=10 then '6-10 Tagada SMS'
		when seq>=11 and seq<=15 then '11-15 Tagada SMS'
		when seq>15 then '15+ Tagada SMS'
	end seq_cat,
	merchant_mobile, 
	reg_date, 
	max(created_at) created_at
from
	(select merchant_mobile, reg_date, created_at, row_number() over(partition by merchant_mobile order by created_at asc) seq
	from 
		public.notification_tagadasms tbl1 
		
		inner join 
	
		(select mobile_no merchant_mobile, reg_date
		from test.spu_prime_20k
		) tbl2 using(merchant_mobile)
	) tbl1
group by 1, 2, 3; 

-- cumulative tendencies
select *
from 
	(-- 1 Tagada SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_1_tagada_sms, sum(tbl2.merchants_consumed_1_tagada_sms) merchants_consumed_1_tagada_sms_cumulative
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_1_tagada_sms
		from data_vajapora.help_b
		where seq_cat='1 Tagada SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_1_tagada_sms
		from data_vajapora.help_b
		where seq_cat='1 Tagada SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	where tbl1.days_to_consume<31
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(-- 2-5 Tagada SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_2_to_5_tagada_sms, sum(tbl2.merchants_consumed_2_to_5_tagada_sms) merchants_consumed_2_to_5_tagada_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_2_to_5_tagada_sms
		from data_vajapora.help_b
		where seq_cat='2-5 Tagada SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_2_to_5_tagada_sms
		from data_vajapora.help_b
		where seq_cat='2-5 Tagada SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	where tbl1.days_to_consume<31
	group by 1, 2
	) tbl2 using(days_to_consume)
	
	inner join 
	
	(-- 6-10 Tagada SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_6_to_10_tagada_sms, sum(tbl2.merchants_consumed_6_to_10_tagada_sms) merchants_consumed_6_to_10_tagada_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_6_to_10_tagada_sms
		from data_vajapora.help_b
		where seq_cat='6-10 Tagada SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_6_to_10_tagada_sms
		from data_vajapora.help_b
		where seq_cat='6-10 Tagada SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	where tbl1.days_to_consume<31
	group by 1, 2
	) tbl3 using(days_to_consume)
	
	inner join 
	
	(-- 11-15 Tagada SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_11_to_15_tagada_sms, sum(tbl2.merchants_consumed_11_to_15_tagada_sms) merchants_consumed_11_to_15_tagada_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_11_to_15_tagada_sms
		from data_vajapora.help_b
		where seq_cat='11-15 Tagada SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_11_to_15_tagada_sms
		from data_vajapora.help_b
		where seq_cat='11-15 Tagada SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	where tbl1.days_to_consume<31
	group by 1, 2
	) tbl4 using(days_to_consume) 
	
	inner join 
	
	(-- 15+ Tagada SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_15_plus_tagada_sms, sum(tbl2.merchants_consumed_15_plus_tagada_sms) merchants_consumed_15_plus_tagada_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_15_plus_tagada_sms
		from data_vajapora.help_b
		where seq_cat='15+ Tagada SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_15_plus_tagada_sms
		from data_vajapora.help_b
		where seq_cat='15+ Tagada SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	where tbl1.days_to_consume<31
	group by 1, 2
	) tbl5 using(days_to_consume)
order by 1; 

/* d) SMS consumption Tendency (Auto SMS) */ 

-- merchant-wise max SMS-consumption timeframes
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	case 
		when seq>=1 and seq<=5 then '1-5 SMS'
		when seq>=6 and seq<=10 then '6-10 SMS'
		when seq>=11 and seq<=15 then '11-15 SMS'
		when seq>15 then '15+ SMS'
	end seq_cat,
	merchant_mobile, 
	reg_date, 
	max(created_at) created_at
from
	(select merchant_mobile, reg_date, created_at, row_number() over(partition by merchant_mobile order by created_at asc) seq
	from 
		(select mobile_no merchant_mobile, request_time created_at
		from public.t_scsms_message_archive_v2
		) tbl1 
		
		inner join 
	
		(select mobile_no merchant_mobile, reg_date
		from test.spu_prime_20k
		) tbl2 using(merchant_mobile)
	) tbl1
group by 1, 2, 3; 

-- cumulative tendencies
select *
from 
	(select generate_series(1, 30) days_to_consume) tbl0 
	
	left join 
	
	(-- 1-5 SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_1_to_5_sms, sum(tbl2.merchants_consumed_1_to_5_sms) merchants_consumed_1_to_5_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_1_to_5_sms
		from data_vajapora.help_b
		where seq_cat='1-5 SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_1_to_5_sms
		from data_vajapora.help_b
		where seq_cat='1-5 SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	group by 1, 2
	) tbl2 using(days_to_consume)
	
	left join 
	
	(-- 6-10 SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_6_to_10_sms, sum(tbl2.merchants_consumed_6_to_10_sms) merchants_consumed_6_to_10_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_6_to_10_sms
		from data_vajapora.help_b
		where seq_cat='6-10 SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_6_to_10_sms
		from data_vajapora.help_b
		where seq_cat='6-10 SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	group by 1, 2
	) tbl3 using(days_to_consume)
	
	left join 
	
	(-- 11-15 SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_11_to_15_sms, sum(tbl2.merchants_consumed_11_to_15_sms) merchants_consumed_11_to_15_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_11_to_15_sms
		from data_vajapora.help_b
		where seq_cat='11-15 SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_11_to_15_sms
		from data_vajapora.help_b
		where seq_cat='11-15 SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	group by 1, 2
	) tbl4 using(days_to_consume) 
	
	left join 
	
	(-- 15+ SMS
	select tbl1.days_to_consume, tbl1.merchants_consumed_15_plus_sms, sum(tbl2.merchants_consumed_15_plus_sms) merchants_consumed_15_plus_sms_cumulative                                                                   
	from 
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_15_plus_sms
		from data_vajapora.help_b
		where seq_cat='15+ SMS'
		group by 1
		) tbl1 
		
		inner join 
		
		(select 
			date(created_at)-reg_date+1 days_to_consume, 
			count(distinct merchant_mobile) merchants_consumed_15_plus_sms
		from data_vajapora.help_b
		where seq_cat='15+ SMS'
		group by 1
		) tbl2 on(tbl1.days_to_consume>=tbl2.days_to_consume)
	group by 1, 2
	) tbl5 using(days_to_consume)
order by 1; 

