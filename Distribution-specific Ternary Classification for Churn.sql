/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: churn_pred_6.ipynb
- Path: 
- Document/Presentation: https://docs.google.com/document/d/1npCwp_TH3B6dsXdhysSq2aDiP-CyWbvltvNMOwR91e8/edit
- Email thread: 
- Notes (if any): Train, test accuracy - 0.73
*/

-- + Features: if 3RAU/10RAU/PU, how many days, loc info, BI-type, version info 
-- + Classes: another intermediate class

-- active days in last 42 days
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no,
	count(case when event_date>='2021-06-15'::date-42 and event_date<'2021-06-15'::date-35 then event_date else null end) week_1_active_days,
	count(case when event_date>='2021-06-15'::date-35 and event_date<'2021-06-15'::date-28 then event_date else null end) week_2_active_days,
	count(case when event_date>='2021-06-15'::date-28 and event_date<'2021-06-15'::date-21 then event_date else null end) week_3_active_days,
	count(case when event_date>='2021-06-15'::date-21 and event_date<'2021-06-15'::date-14 then event_date else null end) week_4_active_days,
	count(case when event_date>='2021-06-15'::date-14 and event_date<'2021-06-15'::date-7 then event_date else null end) week_5_active_days,
	count(case when event_date>='2021-06-15'::date-7 and event_date<'2021-06-15' then event_date else null end) week_6_active_days
from tallykhata.tallykhata_user_date_sequence_final 
where event_date>='2021-06-15'::date-42 and event_date<'2021-06-15'::date
group by 1;

-- TRT/TACS in last 42 days
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select 
	mobile_no,
	
	count(case when created_datetime>='2021-06-15'::date-42 and created_datetime<'2021-06-15'::date-35 and entry_type=1 then auto_id else null end) week_1_trt,
	count(case when created_datetime>='2021-06-15'::date-35 and created_datetime<'2021-06-15'::date-28 and entry_type=1 then auto_id else null end) week_2_trt,
	count(case when created_datetime>='2021-06-15'::date-28 and created_datetime<'2021-06-15'::date-21 and entry_type=1 then auto_id else null end) week_3_trt,
	count(case when created_datetime>='2021-06-15'::date-21 and created_datetime<'2021-06-15'::date-14 and entry_type=1 then auto_id else null end) week_4_trt,
	count(case when created_datetime>='2021-06-15'::date-14 and created_datetime<'2021-06-15'::date-7 and entry_type=1 then auto_id else null end) week_5_trt,
	count(case when created_datetime>='2021-06-15'::date-7 and created_datetime<'2021-06-15' and entry_type=1 then auto_id else null end) week_6_trt,
	
	count(case when created_datetime>='2021-06-15'::date-42 and created_datetime<'2021-06-15'::date-35 and entry_type=2 then auto_id else null end) week_1_tacs,
	count(case when created_datetime>='2021-06-15'::date-35 and created_datetime<'2021-06-15'::date-28 and entry_type=2 then auto_id else null end) week_2_tacs,
	count(case when created_datetime>='2021-06-15'::date-28 and created_datetime<'2021-06-15'::date-21 and entry_type=2 then auto_id else null end) week_3_tacs,
	count(case when created_datetime>='2021-06-15'::date-21 and created_datetime<'2021-06-15'::date-14 and entry_type=2 then auto_id else null end) week_4_tacs,
	count(case when created_datetime>='2021-06-15'::date-14 and created_datetime<'2021-06-15'::date-7 and entry_type=2 then auto_id else null end) week_5_tacs,
	count(case when created_datetime>='2021-06-15'::date-7 and created_datetime<'2021-06-15' and entry_type=2 then auto_id else null end) week_6_tacs
from tallykhata.tallykhata_fact_info_final
where created_datetime>='2021-06-15'::date-42 and created_datetime<'2021-06-15'::date
group by 1;

-- roaming days in the last 42 days
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select 
	mobile_no,
	count(case when roaming_date>='2021-06-15'::date-42 and roaming_date<'2021-06-15'::date-35 then roaming_date else null end) week_1_roam_days,
	count(case when roaming_date>='2021-06-15'::date-35 and roaming_date<'2021-06-15'::date-28 then roaming_date else null end) week_2_roam_days,
	count(case when roaming_date>='2021-06-15'::date-28 and roaming_date<'2021-06-15'::date-21 then roaming_date else null end) week_3_roam_days,
	count(case when roaming_date>='2021-06-15'::date-21 and roaming_date<'2021-06-15'::date-14 then roaming_date else null end) week_4_roam_days,
	count(case when roaming_date>='2021-06-15'::date-14 and roaming_date<'2021-06-15'::date-7 then roaming_date else null end) week_5_roam_days,
	count(case when roaming_date>='2021-06-15'::date-7 and roaming_date<'2021-06-15' then roaming_date else null end) week_6_roam_days
from tallykhata.roaming_users
where roaming_date>='2021-06-15'::date-42 and roaming_date<'2021-06-15'::date
group by 1;

-- seconds spent in the last 42 days
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select 
	mobile_no,
	sum(case when event_date>='2021-06-15'::date-42 and event_date<'2021-06-15'::date-35 then sec_with_tk else 0 end) week_1_spent_sec,
	sum(case when event_date>='2021-06-15'::date-35 and event_date<'2021-06-15'::date-28 then sec_with_tk else 0 end) week_2_spent_sec,
	sum(case when event_date>='2021-06-15'::date-28 and event_date<'2021-06-15'::date-21 then sec_with_tk else 0 end) week_3_spent_sec,
	sum(case when event_date>='2021-06-15'::date-21 and event_date<'2021-06-15'::date-14 then sec_with_tk else 0 end) week_4_spent_sec,
	sum(case when event_date>='2021-06-15'::date-14 and event_date<'2021-06-15'::date-7 then sec_with_tk else 0 end) week_5_spent_sec,
	sum(case when event_date>='2021-06-15'::date-7 and event_date<'2021-06-15' then sec_with_tk else 0 end) week_6_spent_sec
from tallykhata.daily_times_spent_individual_data 
where event_date>='2021-06-15'::date-42 and event_date<'2021-06-15'::date
group by 1; 

-- building main dataset
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select 
	-- identifier
	mobile_no, 
	
	-- features
	'2021-06-15'::date-reg_date+1 days_with_tk,
	
	case when week_1_active_days is not null then week_1_active_days else 0 end week_1_active_days, 
	case when week_2_active_days is not null then week_2_active_days else 0 end week_2_active_days, 
	case when week_3_active_days is not null then week_3_active_days else 0 end week_3_active_days, 
	case when week_4_active_days is not null then week_4_active_days else 0 end week_4_active_days, 
	case when week_5_active_days is not null then week_5_active_days else 0 end week_5_active_days, 
	case when week_6_active_days is not null then week_6_active_days else 0 end week_6_active_days, 
	
	case when week_1_trt is not null then week_1_trt else 0 end week_1_trt, 
	case when week_2_trt is not null then week_2_trt else 0 end week_2_trt, 
	case when week_3_trt is not null then week_3_trt else 0 end week_3_trt, 
	case when week_4_trt is not null then week_4_trt else 0 end week_4_trt, 
	case when week_5_trt is not null then week_5_trt else 0 end week_5_trt, 
	case when week_6_trt is not null then week_6_trt else 0 end week_6_trt, 
	
	case when week_1_tacs is not null then week_1_tacs else 0 end week_1_tacs, 
	case when week_2_tacs is not null then week_2_tacs else 0 end week_2_tacs, 
	case when week_3_tacs is not null then week_3_tacs else 0 end week_3_tacs, 
	case when week_4_tacs is not null then week_4_tacs else 0 end week_4_tacs, 
	case when week_5_tacs is not null then week_5_tacs else 0 end week_5_tacs, 
	case when week_6_tacs is not null then week_6_tacs else 0 end week_6_tacs, 
	
	case when week_1_roam_days is not null then week_1_roam_days else 0 end week_1_roam_days, 
	case when week_2_roam_days is not null then week_2_roam_days else 0 end week_2_roam_days, 
	case when week_3_roam_days is not null then week_3_roam_days else 0 end week_3_roam_days, 
	case when week_4_roam_days is not null then week_4_roam_days else 0 end week_4_roam_days, 
	case when week_5_roam_days is not null then week_5_roam_days else 0 end week_5_roam_days, 
	case when week_6_roam_days is not null then week_6_roam_days else 0 end week_6_roam_days, 
	
	case when week_1_spent_sec is not null then week_1_spent_sec else 0 end week_1_spent_sec, 
	case when week_2_spent_sec is not null then week_2_spent_sec else 0 end week_2_spent_sec, 
	case when week_3_spent_sec is not null then week_3_spent_sec else 0 end week_3_spent_sec, 
	case when week_4_spent_sec is not null then week_4_spent_sec else 0 end week_4_spent_sec, 
	case when week_5_spent_sec is not null then week_5_spent_sec else 0 end week_5_spent_sec, 
	case when week_6_spent_sec is not null then week_6_spent_sec else 0 end week_6_spent_sec,
	
	/*case 
		when days_active_next_14_days is null or days_active_next_14_days=0 then 0
		else 1
	end days_active_next_14_days_cat*/
	
	case 
		when days_active_next_14_days is null or days_active_next_14_days=0 then 0
		when days_active_next_14_days>=1 and days_active_next_14_days<=3 then 1
		when days_active_next_14_days>=4 and days_active_next_14_days<=14 then 2
	end days_active_next_14_days_cat
	
from 
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>='2021-06-15'::date-42 and event_date<'2021-06-15'::date
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	) tbl10 using(mobile_no)
	
	left join 
	data_vajapora.help_a tbl5 using(mobile_no)
	left join 
	data_vajapora.help_b tbl6 using(mobile_no)
	left join 
	data_vajapora.help_c tbl7 using(mobile_no)
	left join 
	data_vajapora.help_d tbl8 using(mobile_no)
	
	left join 
	
	(select mobile_no, count(date_sequence) days_active_next_14_days
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>='2021-06-15' and event_date<'2021-06-15'::date+14
	group by 1
	) tbl9 using(mobile_no)
where reg_date<'2021-06-15'::date-42; 

-- distribution of labels
drop table if exists data_vajapora.help_f; 
create table data_vajapora.help_f as
select 
	days_active_next_14_days_cat, 
	count(mobile_no) merchants,
	round(count(mobile_no)*1.00/(select count(*) from data_vajapora.help_e), 2) merchants_pct
from data_vajapora.help_e
group by 1; 

-- building training/test dataset
select 
	-- identifier
	mobile_no, 
	-- features
	days_with_tk, 
	week_1_active_days, week_2_active_days, week_3_active_days, week_4_active_days, week_5_active_days, week_6_active_days, 
	week_1_trt, week_2_trt, week_3_trt, week_4_trt, week_5_trt, week_6_trt, 
	week_1_tacs, week_2_tacs, week_3_tacs, week_4_tacs, week_5_tacs, week_6_tacs, 
	week_1_roam_days, week_2_roam_days, week_3_roam_days, week_4_roam_days, week_5_roam_days, week_6_roam_days, 
	week_1_spent_sec, week_2_spent_sec, week_3_spent_sec, week_4_spent_sec, week_5_spent_sec, week_6_spent_sec,
	-- labels
	days_active_next_14_days_cat
from
	(select *, row_number() over(partition by days_active_next_14_days_cat order by random()) seq
	from data_vajapora.help_e
	) tbl1
	inner join 
	data_vajapora.help_f tbl2 using(days_active_next_14_days_cat)
where seq<=merchants_pct*100000
order by random(); 
