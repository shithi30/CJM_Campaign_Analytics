/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit#gid=958884661
- Function: 
- Table:
- File: 
- Presentation: https://docs.google.com/presentation/d/1Cypt9wrz1geDEhirwFQWfP-e82vQbY53ax1xTR9yyeY/edit#slide=id.p
- Email thread: 
- Notes (if any): 
*/

/*
-- possible points of analysis by Md. Nazrul Islam
1. % of growth in 1st day 'Add Customer' with jer / without jer / Customer ?
2. Aha moment customer's and merchant's transaction pattern ?
3. How much cost of 'Aha moment' (SMS cost)?
4. How much growth in % of reg to 1st DAU for this feature ?
5. How much growth in % of  first 3 days/ 7 days reg to active user ? 
6. % Contibution of Aha moments merchant's to the 3RAU, 10RAU, DPU ?
7. Aha moment merchnat's behaviour vs non Aah moment merchants behaviour ?
*/

-- Aha-moment daily metrics: TACs, RAUs/PUs, CREDIT_SALEs
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	reg_date,
	case when reg_date>='2021-05-10' then 'Aha' else 'non-Aha' end reg_impression,
	count(distinct mobile) regs,
	count(distinct case when start_balance!=0 then id else null end) tac_with_jer,
	count(distinct case when start_balance=0 then id else null end) tac_without_jer,
	count(distinct id) tac,
	count(distinct contact) tac_custs,
	count(distinct mobile_no) tac_users,
	count(distinct id)*1.00/count(distinct mobile_no) tac_rate,
	count(distinct mobile_no)*1.00/count(distinct mobile) reg_to_tac_pct,
	count(distinct case when start_balance!=0 then mobile_no else null end)*1.00/count(distinct mobile) reg_to_tac_with_jer_pct,
	count(distinct case when start_balance=0 then mobile_no else null end)*1.00/count(distinct mobile) reg_to_tac_without_jer_pct,
	count(distinct rau_10_mobile)*1.00/count(distinct mobile) rau_10_contrib_pct,
	count(distinct rau_3_mobile)*1.00/count(distinct mobile) rau_3_contrib_pct,
	count(distinct pu_mobile)*1.00/count(distinct mobile) pu_contrib_pct,
	count(distinct auto_id) credit_sale_txns,
	count(distinct trt_cust) credit_sale_custs,
	count(distinct trt_cust)*1.00/count(distinct contact) add_to_credit_sale_custs_pct
from 
	(select mobile_number mobile, date(created_at) reg_date
	from public.register_usermobile 
	where date(created_at)>='2021-04-15'
	) tbl1 
	
	left join 
		
	(select id, contact, start_balance, date(create_date) tac_date, mobile_no
	from public.account 
	where 
		type=2
		and date(create_date)>='2021-04-15'
	) tbl2 on(tbl1.mobile=tbl2.mobile_no and tbl1.reg_date=tbl2.tac_date)
	
	left join 
	
	(select contact trt_cust, mobile_no merchant_mobile, auto_id, created_datetime
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type='CREDIT_SALE'
		and created_datetime>='2021-04-15'
	) tbl6 on(tbl2.contact=tbl6.trt_cust and tbl2.mobile_no=tbl6.merchant_mobile and tbl2.tac_date=tbl6.created_datetime)
	
	left join 
	
	(select distinct mobile_no rau_10_mobile 
	from tallykhata.tallykahta_regular_active_user_new
	where rau_category=10
	) tbl3 on(tbl1.mobile=tbl3.rau_10_mobile)
	
	left join 
	
	(select distinct mobile_no rau_3_mobile 
	from tallykhata.tallykhata_regular_active_user
	where rau_category=3
	) tbl4 on(tbl1.mobile=tbl4.rau_3_mobile)
	
	left join 
	
	(select distinct mobile_no pu_mobile
	from tallykhata.tallykhata_usages_data_temp_v1
	where 
		report_date=current_date-1
		and total_active_days>=10
	) tbl5 on(tbl1.mobile=tbl5.pu_mobile)
group by 1 ; 

-- Aha-moment daily metrics: TRT, 1st DAU
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	reg_date,
	sum(case when entry_type=1 then 1 else 0 end)*1.00/count(distinct case when entry_type=1 then txn_mobile else null end) trt_rate,
	count(distinct txn_mobile)*1.00/count(distinct reg_mobile) reg_to_active_pct
from 
	(select mobile_number reg_mobile, date(created_at) reg_date
	from public.register_usermobile 
	where date(created_at)>='2021-04-15' and date(created_at)<current_date -- current_date is to avoid div by 0
	) tbl1 
	
	left join 
	
	(select mobile_no txn_mobile, created_datetime, entry_type
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>='2021-04-15'
	) tbl2 on(tbl1.reg_mobile=tbl2.txn_mobile and tbl1.reg_date=tbl2.created_datetime)
group by 1;

-- Aha-moment daily metrics: events
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	reg_date, 
	count(distinct case when open_events>=5 then reg_mobile else null end) reg_to_5_opens,
	count(distinct case when open_events>=5 then reg_mobile else null end)*1.00/count(distinct reg_mobile) reg_to_5_opens_pct
from 
	(select reg_date, reg_mobile, count(distinct pk_id) open_events
	from 
		(select mobile_number reg_mobile, date(created_at) reg_date
		from public.register_usermobile 
		where date(created_at)>='2021-04-15'
		) tbl1 
		
		left join 
		
		(select mobile_no event_mobile, pk_id, event_date
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			event_name='app_opened'
			and event_date>='2021-04-15'
		) tbl2  on(tbl1.reg_mobile=tbl2.event_mobile and tbl1.reg_date=tbl2.event_date)
	group by 1, 2
	) tbl3
group by 1; 

drop table if exists data_vajapora.help_e;
create table data_vajapora.help_e as
select reg_date, avg(sec_with_tk)/60.00 avg_mins
from 
	(select mobile_number reg_mobile, date(created_at) reg_date
	from public.register_usermobile 
	where date(created_at)>='2021-04-15'
	) tbl1 
	
	inner join 
	
	(select mobile_no, sec_with_tk, event_date
	from tallykhata.daily_times_spent_individual
	where event_date>='2021-04-15'
	) tbl2  on(tbl1.reg_mobile=tbl2.mobile_no and tbl1.reg_date=tbl2.event_date)
group by 1; 

-- Aha-moment daily metrics: all combined
select *
from 
	data_vajapora.help_b tbl1
	inner join 
	data_vajapora.help_c tbl2 using(reg_date)
	inner join 
	data_vajapora.help_d tbl3 using(reg_date)
	inner join 
	data_vajapora.help_e tbl4 using(reg_date)
where reg_date<current_date 
order by 1 asc; 
