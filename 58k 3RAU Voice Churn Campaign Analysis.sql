/*
- Viz: 
- Data (for summary metrics): https://docs.google.com/spreadsheets/d/1aPxDeL6dn65nLt7Qp4QswNAkBlXm9gea11PkarJ3iKw/edit#gid=1784199747
- Table:
- File (for user-wise metrics): Cam-3_3RAU - 58k Data.xlsx
- Email thread: 
- Notes (if any): set limit to 60k on IDE
*/

-- for txns and events
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select mobile_no, id, event_date, entry_type
from tallykhata.event_transacting_fact
where 
	(entry_type=1 or event_name='app_opened')
	and event_date>='2021-05-08' and event_date<='2021-05-22'; -- change here
	
-- user-wise metrics
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	tg_name, mobile_no, reg_day_version, share_day_version, last_act_date, current_version, 
	
	count(case when entry_type=2 then id else null end)+count(case when entry_type=1 then id else null end) open_plus_trt,
	
	count(case when entry_type=2 then id else null end) opens,
	count(case when entry_type=1 then id else null end) trt,
	
	count(case when entry_type=1 and event_date='2021-05-08' then id else null end) day_1_trt,
	count(case when entry_type=1 and event_date='2021-05-09' then id else null end) day_2_trt,
	count(case when entry_type=1 and event_date='2021-05-10' then id else null end) day_3_trt,
	count(case when entry_type=1 and event_date='2021-05-11' then id else null end) day_4_trt,
	count(case when entry_type=1 and event_date='2021-05-12' then id else null end) day_5_trt,
	count(case when entry_type=1 and event_date='2021-05-13' then id else null end) day_6_trt,
	count(case when entry_type=1 and event_date='2021-05-14' then id else null end) day_7_trt,
	count(case when entry_type=1 and event_date='2021-05-15' then id else null end) day_8_trt,
	count(case when entry_type=1 and event_date='2021-05-16' then id else null end) day_9_trt,
	count(case when entry_type=1 and event_date='2021-05-17' then id else null end) day_10_trt,
	count(case when entry_type=1 and event_date='2021-05-18' then id else null end) day_11_trt,
	count(case when entry_type=1 and event_date='2021-05-19' then id else null end) day_12_trt,
	count(case when entry_type=1 and event_date='2021-05-20' then id else null end) day_13_trt,
	count(case when entry_type=1 and event_date='2021-05-21' then id else null end) day_14_trt,
	count(case when entry_type=1 and event_date='2021-05-22' then id else null end) day_15_trt,
	
	count(case when entry_type=2 and event_date='2021-05-08' then id else null end) day_1_opens,
	count(case when entry_type=2 and event_date='2021-05-09' then id else null end) day_2_opens,
	count(case when entry_type=2 and event_date='2021-05-10' then id else null end) day_3_opens,
	count(case when entry_type=2 and event_date='2021-05-11' then id else null end) day_4_opens,
	count(case when entry_type=2 and event_date='2021-05-12' then id else null end) day_5_opens,
	count(case when entry_type=2 and event_date='2021-05-13' then id else null end) day_6_opens,
	count(case when entry_type=2 and event_date='2021-05-14' then id else null end) day_7_opens,
	count(case when entry_type=2 and event_date='2021-05-15' then id else null end) day_8_opens,
	count(case when entry_type=2 and event_date='2021-05-16' then id else null end) day_9_opens,
	count(case when entry_type=2 and event_date='2021-05-17' then id else null end) day_10_opens,
	count(case when entry_type=2 and event_date='2021-05-18' then id else null end) day_11_opens,
	count(case when entry_type=2 and event_date='2021-05-19' then id else null end) day_12_opens,
	count(case when entry_type=2 and event_date='2021-05-20' then id else null end) day_13_opens,
	count(case when entry_type=2 and event_date='2021-05-21' then id else null end) day_14_opens,
	count(case when entry_type=2 and event_date='2021-05-22' then id else null end) day_15_opens
from 
	(select tg_name, mobile_no, last_active_day last_act_date
	from data_vajapora.rau3_churn_result_20210504
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(app_version_name) current_version
	from data_vajapora.version_wise_days
	where date(update_or_reg_datetime)<=current_date 
	group by 1 
	) tbl2 using(mobile_no)
	
	inner join 
	
	(select mobile_no, min(app_version_name) reg_day_version
	from data_vajapora.version_wise_days
	where date(update_or_reg_datetime)<=current_date 
	group by 1 
	) tbl3 using(mobile_no)
	
	inner join 
	
	(select mobile_no, max(app_version_name) share_day_version
	from data_vajapora.version_wise_days
	where date(update_or_reg_datetime)<='2021-05-04'
	group by 1 
	) tbl4 using(mobile_no)
	
	left join 
	
	data_vajapora.help_d tbl5 using(mobile_no)
group by 1, 2, 3, 4, 5, 6; 
/*having 
	share_day_version!=current_version
	and current_version='3.0.0'*/
select *
from data_vajapora.help_b; 

-- summary metrics
select 
	count(pitched_mobile_no) pitched_users,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where open_plus_trt!=0
	) winback,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where trt!=0
	) min_1_trt_users,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where 
		open_plus_trt!=0
		and mobile_no in 
			(select mobile_no 
			from tallykhata.historical_users_segmented_new_modality
			where 
				date(data_generation_time)=current_date 
				and segment='4: -(Churn)'
			)
	) winback_to_churn,
	
	count(rau_3_mobile_no_txn) rau_3_txn,
	count(rau_3_mobile_no_ev) rau_3_ev,
	count(rau_10_mobile_no_txn) rau_10_txn,
	count(rau_10_mobile_no_ev) rau_10_ev,
	count(pu_mobile) pus, 
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where share_day_version!=current_version
	) updated,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where 
		share_day_version!=current_version
		and current_version='3.0.0'
	) updated_to_latest_version
	
from 
	(select mobile_no pitched_mobile_no
	from data_vajapora.help_b
	) tbl1
		
	left join 
	
	(select distinct mobile_no rau_3_mobile_no_ev
	from tallykhata.regular_active_user_event
	where 
		rau_category=3
		and report_date>='2021-05-08'
	) tbl2 on(tbl1.pitched_mobile_no=tbl2.rau_3_mobile_no_ev)
	
	left join 
	
	(select distinct mobile_no rau_10_mobile_no_ev
	from tallykhata.regular_active_user_event
	where 
		rau_category=10
		and report_date>='2021-05-08'
	) tbl3 on(tbl1.pitched_mobile_no=tbl3.rau_10_mobile_no_ev)
	
	left join 
	
	(select distinct mobile_no rau_3_mobile_no_txn 
	from tallykhata.tallykhata_regular_active_user 
	where 
		rau_category=3
		and rau_date>='2021-05-08'
	) tbl4 on(tbl1.pitched_mobile_no=tbl4.rau_3_mobile_no_txn)
	
	left join 
	
	(select distinct mobile_no rau_10_mobile_no_txn
	from tallykhata.tallykahta_regular_active_user_new
	where 
		rau_category=10
		and rau_date>='2021-05-08'
	) tbl5 on(tbl1.pitched_mobile_no=tbl5.rau_10_mobile_no_txn)
	
	left join 
	
	(select distinct mobile_no pu_mobile
	from tallykhata.tallykhata_usages_data_temp_v1
	where 
		total_active_days>=10
		and report_date>='2021-05-08'
	) tbl6 on(tbl1.pitched_mobile_no=tbl6.pu_mobile); 
