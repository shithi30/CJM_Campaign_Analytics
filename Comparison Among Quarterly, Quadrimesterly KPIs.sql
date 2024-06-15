/*
- Viz: 302.png
- Data: https://docs.google.com/spreadsheets/d/1tnVQWD-tDGHB0So4GwmVXOzq_vHZO9PXtTmpBZjw9Vw/edit#gid=2062009934
- Table:
- File: 
- Email thread: 
- Notes (if any): 
*/

/* KPIs proposed by Md. Nazrul Islam */
do $$

declare 
	var_quar_start date:='2021-01-01';
	var_quar_end date:='2021-04-30';
begin
	raise notice 'New OP goes below:'; 
	
	-- txn data
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select mobile_no, auto_id, created_datetime, entry_type
	from tallykhata.tallykhata_fact_info_final
	where created_datetime>=var_quar_start and created_datetime<=var_quar_end;
	raise notice 'Txn data generated'; 	

	-- 10RAU data
	drop table if exists data_vajapora.help_d; 
	create table data_vajapora.help_d as
	select distinct mobile_no
	from tallykhata.tallykahta_regular_active_user_new
	where 
		rau_date>=var_quar_start and rau_date<=var_quar_end
		and rau_category=10; 
	raise notice '10RAU data generated'; 	
	
	-- 3RAU data
	drop table if exists data_vajapora.help_e; 
	create table data_vajapora.help_e as	
	select distinct mobile_no
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_date>=var_quar_start and rau_date<=var_quar_end
		and rau_category=3;
	raise notice '3RAU data generated'; 	
	
	-- PU data
	drop table if exists data_vajapora.help_f; 
	create table data_vajapora.help_f as
	select distinct mobile_no
	from tallykhata.tallykhata_usages_data_temp_v1
	where 
		total_active_days>=10
		and report_date>=var_quar_start and report_date<=var_quar_end;
	raise notice 'PU data generated'; 	
	
	-- quarterly KPI data
	raise notice 'Generating quarterly KPIs'; 
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select 
		concat(var_quar_start, ' to ', var_quar_end) quar, 
		downloads, registrations,
		active_non_roamers, rau_10s, rau_3s, pus,
		dau_trt*1.00/active_non_roamers dau_trt_rate, dau_tacs*1.00/active_non_roamers dau_tacs_rate,
		rau_10_trt*1.00/rau_10s rau_10_trt_rate, rau_10_tacs*1.00/rau_10s rau_10_tacs_rate,
		pu_trt*1.00/pus pu_trt_rate, pu_tacs*1.00/pus pu_tacs_rate
	from 
		(select sum(download) downloads
		from tallykhata.tallykhata_playstore_installs_dashboard
		where date_time>=var_quar_start and date_time<=var_quar_end
		) tbl1, 
		
		(select count(distinct mobile_number) registrations
		from public.register_usermobile
		where date(created_at)>=var_quar_start and date(created_at)<=var_quar_end
		) tbl2, 
		
		(select count(distinct mobile_no) active_non_roamers
		from data_vajapora.help_c
		) tbl3,
		
		(select count(mobile_no) rau_10s
		from data_vajapora.help_d
		) tbl4,
		
		(select count(mobile_no) rau_3s
		from data_vajapora.help_e
		) tbl5,
		
		(select count(mobile_no) pus
		from data_vajapora.help_f
		) tbl6,
		
		(select count(auto_id) dau_trt
		from data_vajapora.help_c
		where entry_type=1
		) tbl7,
		
		(select count(auto_id) dau_tacs
		from data_vajapora.help_c
		where entry_type=2
		) tbl8,
		
		(select count(auto_id) rau_10_trt
		from 
			data_vajapora.help_d tbl1
			
			inner join 
			
			(select mobile_no, auto_id 
			from data_vajapora.help_c
			where entry_type=1
			) tbl2 using(mobile_no)
		) tbl9,
		
		(select count(auto_id) rau_10_tacs
		from 
			data_vajapora.help_d tbl1
			
			inner join 
			
			(select mobile_no, auto_id 
			from data_vajapora.help_c
			where entry_type=2
			) tbl2 using(mobile_no)
		) tbl10,
		
		(select count(auto_id) pu_trt
		from 
			data_vajapora.help_f tbl1
			
			inner join 
			
			(select mobile_no, auto_id 
			from data_vajapora.help_c
			where entry_type=1
			) tbl2 using(mobile_no)
		) tbl13,
		
		(select count(auto_id) pu_tacs
		from 
			data_vajapora.help_f tbl1
			
			inner join 
			
			(select mobile_no, auto_id 
			from data_vajapora.help_c
			where entry_type=2
			) tbl2 using(mobile_no)
		) tbl14;
end $$; 

select *
from data_vajapora.help_b;

/* KPIs proposed by Shithi Maitra */
do $$

declare 
	var_quar_start date:='2020-05-01';
	var_quar_end date:='2020-08-31';
begin
	raise notice 'New OP goes below:'; 
	
	-- 10RAU gain data
	drop table if exists data_vajapora.help_d; 
	create table data_vajapora.help_d as
	select mobile_no
	from tallykhata.tallykahta_regular_active_user_new
	where rau_category=10
	group by 1 
	having min(rau_date)>=var_quar_start and min(rau_date)<=var_quar_end; 
	raise notice '10RAU gain data generated'; 

	-- 3RAU gain data
	drop table if exists data_vajapora.help_e; 
	create table data_vajapora.help_e as
	select mobile_no
	from tallykhata.tallykhata_regular_active_user
	where rau_category=3
	group by 1 
	having min(rau_date)>=var_quar_start and min(rau_date)<=var_quar_end; 
	raise notice '3RAU gain data generated'; 

	-- 10RAU churn data
	drop table if exists data_vajapora.help_f; 
	create table data_vajapora.help_f as
	select mobile_no
	from tallykhata.tallykahta_regular_active_user_new
	where rau_category=10
	group by 1 
	having max(rau_date)>=var_quar_start and max(rau_date)<=var_quar_end; 
	raise notice '10RAU churn data generated'; 

	-- 3RAU churn data
	drop table if exists data_vajapora.help_g; 
	create table data_vajapora.help_g as
	select mobile_no
	from tallykhata.tallykhata_regular_active_user
	where rau_category=3
	group by 1 
	having max(rau_date)>=var_quar_start and max(rau_date)<=var_quar_end; 
	raise notice '3RAU churn data generated'; 

	-- quarterly KPI data
	raise notice 'Generating quarterly KPIs'; 
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select
		concat(var_quar_start, ' to ', var_quar_end) quar, 
		gained_rau_10s, churned_rau_10s,
		gained_rau_3s, churned_rau_3s
	from 
		(select count(mobile_no) gained_rau_10s
		from data_vajapora.help_d
		) tbl1,
		
		(select count(mobile_no) gained_rau_3s
		from data_vajapora.help_e
		) tbl2,
		
		(select count(mobile_no) churned_rau_10s
		from data_vajapora.help_f
		) tbl3,
		
		(select count(mobile_no) churned_rau_3s
		from data_vajapora.help_g
		) tbl4; 
end $$; 

select *
from data_vajapora.help_b;

/* KPIs proposed by Shithi Maitra */
do $$

declare 
	var_quar_start date:='2020-05-01';
	var_quar_end date:='2020-08-31';
begin 
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select *, fst_rau_date-reg_date reg_to_rau_days
	from 
		(select mobile mobile_no, registration_date reg_date
		from tallykhata.tallykhata_user_personal_info 
		where registration_date>=var_quar_start and registration_date<=var_quar_end 
		) tbl1
		
		inner join 
		
		(select mobile_no, min(rau_date) fst_rau_date
		from tallykhata.tallykahta_regular_active_user_new
		where rau_category=10
		group by 1 
		having min(rau_date)>=var_quar_start and min(rau_date)<=var_quar_end 
		) tbl2 using(mobile_no)
	where fst_rau_date-reg_date>=9 -- 3RAUS: 2, 10RAUs: 9
	limit 20000; -- sample size
	
	-- days taken to enter RAU
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as
	select reg_to_rau_days, raus, cum_raus, cum_raus/sampl_sz cum_raus_pct
	from 
		(select tbl1.reg_to_rau_days, tbl1.raus, sum(tbl2.raus) cum_raus
		from 
			(select reg_to_rau_days, count(mobile_no) raus
			from data_vajapora.help_a
			group by 1
			) tbl1 
			
			inner join 
			
			(select reg_to_rau_days, count(mobile_no) raus
			from data_vajapora.help_a
			group by 1
			) tbl2 on(tbl2.reg_to_rau_days<=tbl1.reg_to_rau_days)
		group by 1, 2
		) tbl1,
		
		(select count(mobile_no) sampl_sz
		from data_vajapora.help_a
		) tbl2
	order by 1 asc; 
end $$; 

select *
from data_vajapora.help_b; 
