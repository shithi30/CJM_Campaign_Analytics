/*
- Viz: https://docs.google.com/spreadsheets/d/1aPxDeL6dn65nLt7Qp4QswNAkBlXm9gea11PkarJ3iKw/edit#gid=1940946959
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

- change dates and TG 
- 3RAU churn compare with organic: https://docs.google.com/spreadsheets/d/1aPxDeL6dn65nLt7Qp4QswNAkBlXm9gea11PkarJ3iKw/edit#gid=746251800 
- 58k churn (all from data_vajapora.rau3_churn_result_20210504): https://docs.google.com/spreadsheets/d/1aPxDeL6dn65nLt7Qp4QswNAkBlXm9gea11PkarJ3iKw/edit#gid=2017413123

*/


drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select remarks tg_name, mobile_no
	from data_vajapora.tk_churn_data_new
	where remarks='Became 3RAU'
	) tbl1
	
	inner join 
	
	(select mobile_no, id, event_date, entry_type
	from tallykhata.event_transacting_fact
	where 
		(entry_type=1 or event_name='app_opened')
		and event_date>='2021-04-06' and event_date<='2021-05-30'
	) tbl2 using(mobile_no); 
	
select 
	count(pitched_mobile_no) pitched_users,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	) winback,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where entry_type=1
	) min_1_trt_users,
	
	(select count(distinct mobile_no)
	from data_vajapora.help_b
	where mobile_no in 
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
	count(pu_mobile) pus
from 
	(select mobile_no pitched_mobile_no
	from data_vajapora.tk_churn_data_new
	where remarks='Became 3RAU'
	) tbl1
		
	left join 
	
	(select distinct mobile_no rau_3_mobile_no_ev
	from tallykhata.regular_active_user_event
	where 
		rau_category=3
		and report_date>='2021-04-06' and report_date<='2021-05-30'
	) tbl2 on(tbl1.pitched_mobile_no=tbl2.rau_3_mobile_no_ev)
	
	left join 
	
	(select distinct mobile_no rau_10_mobile_no_ev
	from tallykhata.regular_active_user_event
	where 
		rau_category=10
		and report_date>='2021-04-06' and report_date<='2021-05-30'
	) tbl3 on(tbl1.pitched_mobile_no=tbl3.rau_10_mobile_no_ev)
	
	left join 
	
	(select distinct mobile_no rau_3_mobile_no_txn 
	from tallykhata.tallykhata_regular_active_user 
	where 
		rau_category=3
		and rau_date>='2021-04-06' and rau_date<='2021-05-30'
	) tbl4 on(tbl1.pitched_mobile_no=tbl4.rau_3_mobile_no_txn)
	
	left join 
	
	(select distinct mobile_no rau_10_mobile_no_txn
	from tallykhata.tallykahta_regular_active_user_new
	where 
		rau_category=10
		and rau_date>='2021-04-06' and rau_date<='2021-05-30'
	) tbl5 on(tbl1.pitched_mobile_no=tbl5.rau_10_mobile_no_txn)
	
	left join 
	
	(select distinct mobile_no pu_mobile
	from tallykhata.tallykhata_usages_data_temp_v1
	where 
		total_active_days>=10
		and report_date>='2021-04-06' and report_date<='2021-05-30'
	) tbl6 on(tbl1.pitched_mobile_no=tbl6.pu_mobile); 

/*
-- for 3RAU churn organic cases (change dates)
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no
from data_vajapora.rau3_churn_result_20210504
where mobile_no not in 
	(select mobile_no
	from data_vajapora.tk_churn_data_new
	where remarks='Became 3RAU'
	) 
order by random() 
limit 1000; 

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no
	from data_vajapora.help_a
	) tbl1
	
	inner join 
	
	(select mobile_no, id, event_date, entry_type
	from tallykhata.event_transacting_fact
	where 
		(entry_type=1 or event_name='app_opened')
		and event_date>='2021-04-06' and event_date<='2021-06-06'
	) tbl2 using(mobile_no); 
*/
