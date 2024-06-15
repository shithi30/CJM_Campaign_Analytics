/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2128899838
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
	>> Why SU churn increased for the last few days ?
	Txn days reduced, no. of txns remained uneffected. 
	tallykhata.fn_spu_aspu_summary_generation() by Mahmud
*/

-- txns per day per customer
do $$ 

declare 
	var_date date:=current_date-70; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.help_a 
		where txn_date=var_date; 
	
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select mobile_no, count(auto_id) txns, var_date txn_date
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		group by 1; 
	
		insert into data_vajapora.help_a  
		select * 
		from data_vajapora.help_b; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select txn_date, count(*) entries
from data_vajapora.help_a 
group by 1 
order by 1; 

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.less_su_inv 
		where report_date=var_date; 
	
		-- day-to-day SU churns
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as 
		select mobile_no
		from 
			(select mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU', 'Sticky SPU')
				and report_date=var_date-1
			) tbl1 
			
			left join 
			
			(select mobile_no
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU', 'Sticky SPU')
				and report_date=var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null;  

		insert into data_vajapora.less_su_inv 
		select 
			var_date report_date, 
			(select count(*) from data_vajapora.help_c) churned_spus,
			* 
		from 
			(-- 1. >=24 txn days in last 30 days
			select count(*) greaterequal_24_txn_days_last_30_days 
			from 
				(select mobile_no, count(txn_date) txn_days_last_30_days
				from 
					data_vajapora.help_a 
					inner join 
					data_vajapora.help_c using(mobile_no)
				where txn_date>=var_date-30 and txn_date<var_date
				group by 1 
				having count(txn_date)>23
				) tbl1
			) tbl1, 
			
			(-- 2. >= 60 txn last 30 days
			select count(*) greaterequal_60_txns_last_30_days 
			from
				(select mobile_no, sum(txns) txns_last_30_days 
				from 
					data_vajapora.help_a 
					inner join 
					data_vajapora.help_c using(mobile_no) 
				where txn_date>=var_date-30 and txn_date<var_date
				group by 1 
				having sum(txns)>59
				) tbl1 
			) tbl2, 
			
			(-- 3. >=10 txns in each of last 4 weeks
			select count(*) greaterequal_10_txns_each_4_weeks 
			from
				(select 
					mobile_no, 
					sum(case when txn_date>=var_date-28 and txn_date<var_date-21 then txns else null end) txns_last_week_4,
					sum(case when txn_date>=var_date-21 and txn_date<var_date-14 then txns else null end) txns_last_week_3,
					sum(case when txn_date>=var_date-14 and txn_date<var_date-07 then txns else null end) txns_last_week_2,
					sum(case when txn_date>=var_date-07 and txn_date<var_date-00 then txns else null end) txns_last_week_1
				from 
					data_vajapora.help_a 
					inner join 
					data_vajapora.help_c using(mobile_no) 
				where txn_date>=var_date-28 and txn_date<var_date
				group by 1 
				having 
					    sum(case when txn_date>=var_date-28 and txn_date<var_date-21 then txns else null end)>9 
					and sum(case when txn_date>=var_date-21 and txn_date<var_date-14 then txns else null end)>9 
					and sum(case when txn_date>=var_date-14 and txn_date<var_date-07 then txns else null end)>9 
					and sum(case when txn_date>=var_date-07 and txn_date<var_date-00 then txns else null end)>9
				) tbl1 
			) tbl3; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
	
end $$;

select * 
from data_vajapora.less_su_inv tbl1 
order by 1; 

/*
-- real SUs, no ASPU/sticky
select report_date, count(*) sus
from tallykhata.tk_spu_aspu_data 
where 
	1=1
	and pu_type in('SPU')
	and pu_subtype is null
	and report_date>=current_date-30
group by 1
order by 1; 
*/
