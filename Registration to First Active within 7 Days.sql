/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=157421127
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I have analyzed Registration-to-1st-Active tendencies cumulatively for the first 7 days after registration. Findings:
- There are 3.0.0-spikes on May-09, May-10
- Eid made May-12, 13, 14 cause a downfall
- The metrics are on the rise since May-15
- 95% of transacting users get their 1st txn done by 1 day of reg.

NB: Data since May-13 will eventually stabilize. 

*/

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *, fst_txn_date-reg_date reg_to_fst_txn_gap_days
from 
	(-- merchants who registered within the last 40 days
	select mobile mobile_no, registration_date reg_date 
	from tallykhata.tallykhata_user_personal_info
	where registration_date>=current_date-40 
	) tbl1
	
	left join 
	
	(-- merchants' first day of activity after registration
	select mobile_no, min(created_datetime) fst_txn_date 
	from tallykhata.tallykhata_fact_info_final
	group by 1
	) tbl2 using(mobile_no)
where 
	fst_txn_date>=reg_date
	or fst_txn_date is null; 

-- cumulative analysis
select 
	tbl1.reg_to_fst_txn_gap_days, 
	tbl1.merchants, 
	tbl3.total_merchants,
	sum(tbl2.merchants) cum_merchants,
	sum(tbl2.merchants)*1.00/tbl3.total_merchants cum_merchants_pct
from 
	(select reg_to_fst_txn_gap_days, count(distinct mobile_no) merchants
	from data_vajapora.help_a
	where reg_to_fst_txn_gap_days<=7
	group by 1
	) tbl1 
	
	inner join 
	
	(select reg_to_fst_txn_gap_days, count(distinct mobile_no) merchants
	from data_vajapora.help_a
	where reg_to_fst_txn_gap_days<=7
	group by 1
	) tbl2 on(tbl1.reg_to_fst_txn_gap_days>=tbl2.reg_to_fst_txn_gap_days),
	
	(select count(distinct mobile_no) total_merchants 
	from data_vajapora.help_a
	where reg_to_fst_txn_gap_days<=7
	) tbl3
group by 1, 2, 3; 

-- daily cumulative metrics
select 
	reg_date,
	
	count(distinct mobile_no) regs,
	
	/*count(distinct case when reg_to_fst_txn_gap_days<=0 then mobile_no else null end) fst_active_till_day_0,
	count(distinct case when reg_to_fst_txn_gap_days<=1 then mobile_no else null end) fst_active_till_day_1,
	count(distinct case when reg_to_fst_txn_gap_days<=2 then mobile_no else null end) fst_active_till_day_2,
	count(distinct case when reg_to_fst_txn_gap_days<=3 then mobile_no else null end) fst_active_till_day_3,
	count(distinct case when reg_to_fst_txn_gap_days<=4 then mobile_no else null end) fst_active_till_day_4,
	count(distinct case when reg_to_fst_txn_gap_days<=5 then mobile_no else null end) fst_active_till_day_5,
	count(distinct case when reg_to_fst_txn_gap_days<=6 then mobile_no else null end) fst_active_till_day_6,
	count(distinct case when reg_to_fst_txn_gap_days<=7 then mobile_no else null end) fst_active_till_day_7,*/
	
	count(distinct case when reg_to_fst_txn_gap_days<=0 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_0_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=1 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_1_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=2 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_2_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=3 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_3_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=4 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_4_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=5 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_5_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=6 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_6_pct,
	count(distinct case when reg_to_fst_txn_gap_days<=7 then mobile_no else null end)*1.00/count(distinct mobile_no) fst_active_till_day_7_pct
from data_vajapora.help_a
where reg_date<current_date
group by 1
order by 1 asc; 
