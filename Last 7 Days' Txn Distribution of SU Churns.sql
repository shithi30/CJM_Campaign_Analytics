/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=105509216 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=624923832
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
	According to this morning's discussion, I have distributed 60 days' (excluding last 7 days) permanent SU-churns. 
	Observations: 
	- ~65% transacted >2 days in the prior 7 days
	- 28% transacted 4 days in the prior 7 days 
	- 20% transacted 3 days in the prior 7 days
	- 10% did not transact at all in the last 7 days
	
	According to a later discussion, the analysis is now available for SUs who entered and left the segment only once in their lifetime. 
	Finding: 65% of these churned SUs remain as 3RAUs. 
*/

do $$

declare
	var_date date:=current_date-5; 

begin
	raise notice 'New OP goes below: '; 

	-- SPUs permanently churned
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as 
	select tbl1.report_date+1 report_date, tbl1.mobile_no
	from 
		(select report_date, mobile_no 
		from tallykhata.tk_spu_aspu_data 
		where pu_type in('SPU', 'Sticky SPU') 
		) tbl1  
		
		left join 
		
		(select report_date, mobile_no 
		from tallykhata.tk_spu_aspu_data 
		where pu_type in('SPU', 'Sticky SPU') 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.report_date=tbl1.report_date+1)
		
		left join 
		
		(select mobile_no, max(report_date) max_spu_date 
		from tallykhata.tk_spu_aspu_data 
		where pu_type in('SPU', 'Sticky SPU')
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
	where 
		tbl2.mobile_no is null
		and tbl3.max_spu_date=tbl1.report_date;
	
	-- retained today
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as 
	select mobile_no 
	from cjm_segmentation.retained_users
	where report_date=current_date; 

	loop
		delete from data_vajapora.su_permanent_churn_distrib 
		where report_date=var_date;
	
		-- last 7 days' txn DAUs
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct created_datetime report_date, mobile_no 
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>=var_date-7 and created_datetime<var_date; 
		
		-- distribution of last 7 days' txn DAUs
		insert into data_vajapora.su_permanent_churn_distrib
		select 
			var_date report_date, 
			count(mobile_no) sus_permanently_churned, 
			count(case when days_txned=0 then mobile_no else null end) prior_txn_days_0,
			count(case when days_txned=1 then mobile_no else null end) prior_txn_days_1,
			count(case when days_txned=2 then mobile_no else null end) prior_txn_days_2,
			count(case when days_txned=3 then mobile_no else null end) prior_txn_days_3,
			count(case when days_txned=4 then mobile_no else null end) prior_txn_days_4,
			count(case when days_txned=5 then mobile_no else null end) prior_txn_days_5,
			count(case when days_txned=6 then mobile_no else null end) prior_txn_days_6,
			count(case when days_txned=7 then mobile_no else null end) prior_txn_days_7, 
			null sus_permanently_churned_and_uninstalled
		from 
			(select
				mobile_no, 
				count(report_date) days_txned
			from 
				(select mobile_no 
				from data_vajapora.help_a 
				where report_date=var_date
				) tbl1 
				left join 
				data_vajapora.help_b tbl2 using(mobile_no)
			group by 1
			) tbl1
		group by 1;
	
		-- added later
		update data_vajapora.su_permanent_churn_distrib
		set sus_permanently_churned_and_uninstalled= 
			(select count(mobile_no)
			from 
				(select mobile_no 
				from data_vajapora.help_a 
				where report_date=var_date
				) tbl1 
				left join 
				data_vajapora.help_c tbl2 using(mobile_no)
			where tbl2.mobile_no is null
			)
		where report_date=var_date; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$; 

select * 
from data_vajapora.su_permanent_churn_distrib
order by 1; 

do $$

declare
	var_date date:=current_date-5; 

begin
	raise notice 'New OP goes below: '; 

	-- SPUs permanently churned (SUs who entered and left the segment only once)
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select *, spu_date_2-spu_date_1 gap_days
	from 
		(select mobile_no, report_date spu_date_1, lead(report_date, 1) over(partition by mobile_no order by report_date) spu_date_2
		from tallykhata.tk_spu_aspu_data 
		where pu_type in('SPU', 'Sticky SPU')
		) tbl1; 
			
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as
	select * 
	from 
		data_vajapora.help_a tbl1
		
		left join 
	
		(select mobile_no
		from data_vajapora.help_a
		group by 1 
		having count(*)=1
		) tbl2 using(mobile_no)
		
		left join 
		
		(select distinct mobile_no 
		from data_vajapora.help_a 
		where gap_days>1
		) tbl3 using(mobile_no) 
	where 
		tbl2.mobile_no is null 
		and tbl3.mobile_no is null; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, max(spu_date_1)+1 report_date
	from data_vajapora.help_b
	group by 1;
	
	-- retained today
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as 
	select mobile_no 
	from cjm_segmentation.retained_users
	where report_date=current_date; 

	loop
		delete from data_vajapora.su_permanent_churn_distrib_2 
		where report_date=var_date;
	
		-- last 7 days' txn DAUs
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct created_datetime report_date, mobile_no 
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>=var_date-7 and created_datetime<var_date; 
		
		-- distribution of last 7 days' txn DAUs
		insert into data_vajapora.su_permanent_churn_distrib_2
		select 
			var_date report_date, 
			count(mobile_no) sus_permanently_churned, 
			count(case when days_txned=0 then mobile_no else null end) prior_txn_days_0,
			count(case when days_txned=1 then mobile_no else null end) prior_txn_days_1,
			count(case when days_txned=2 then mobile_no else null end) prior_txn_days_2,
			count(case when days_txned=3 then mobile_no else null end) prior_txn_days_3,
			count(case when days_txned=4 then mobile_no else null end) prior_txn_days_4,
			count(case when days_txned=5 then mobile_no else null end) prior_txn_days_5,
			count(case when days_txned=6 then mobile_no else null end) prior_txn_days_6,
			count(case when days_txned=7 then mobile_no else null end) prior_txn_days_7, 
			null sus_permanently_churned_and_uninstalled
		from 
			(select
				mobile_no, 
				count(report_date) days_txned
			from 
				(select mobile_no 
				from data_vajapora.help_a 
				where report_date=var_date
				) tbl1 
				left join 
				data_vajapora.help_b tbl2 using(mobile_no)
			group by 1
			) tbl1
		group by 1;
	
		-- added later
		update data_vajapora.su_permanent_churn_distrib_2
		set sus_permanently_churned_and_uninstalled= 
			(select count(mobile_no)
			from 
				(select mobile_no 
				from data_vajapora.help_a 
				where report_date=var_date
				) tbl1 
				left join 
				data_vajapora.help_c tbl2 using(mobile_no)
			where tbl2.mobile_no is null
			)
		where report_date=var_date; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$; 

select * 
from data_vajapora.su_permanent_churn_distrib_2
order by 1; 
