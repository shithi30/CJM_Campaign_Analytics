/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1169147213
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
*/

-- Eid 2022: '2022-05-03'::date
-- Eid 2021: '2021-05-14'::date

do $$

declare 
	var_date date:='2021-05-14'::date; -- change here
	var_start_date date:=var_date-7;
	var_end_date date:=var_date+7;
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.eid_comparisons 
		where report_date=var_start_date; 
	
		insert into data_vajapora.eid_comparisons
	
		select mobile_no, var_start_date report_date, txn_type remark
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_start_date
		
		union all
		
		select mobile_no, var_start_date report_date, event_name remark
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_start_date
			and event_name not in ('in_app_message_received','inbox_message_received')
			
		union all
			
		select ss.mobile_number mobile_no, var_start_date report_date, 'unverified' remark
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_start_date; 
			
		commit; 
		raise notice 'Data generated for: %', var_start_date; 
		var_start_date:=var_start_date+1; 
		if var_start_date=var_end_date+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.eid_comparisons; 

do $$

declare 
	var_date date:='2021-05-14'::date-7; -- change here
begin  
	raise notice 'New OP goes below:'; 
	loop
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as 
		select *
		from data_vajapora.eid_comparisons 
		where report_date=var_date; 
		
		delete from data_vajapora.eid_comparisons_2
		where report_date=var_date; 
	
		insert into data_vajapora.eid_comparisons_2
		select * 
		from 
			(select report_date, count(distinct mobile_no) dau 
			from data_vajapora.help_a
			group by 1
			) tbl1 
			
			inner join 
			
			(select report_date, count(distinct mobile_no) txn_dau 
			from data_vajapora.help_a
			where remark in(select distinct txn_type from tallykhata.tallykhata_fact_info_final where created_datetime=current_date-1)
			group by 1
			) tbl2 using(report_date)
			
			inner join 
			
			(select report_date, count(distinct mobile_no) pu_dau
			from 
				data_vajapora.help_a tbl1 
				
				inner join 
				
				(select mobile_no
				from tallykhata.tk_power_users_10
				where report_date=var_date
				) tbl2 using(mobile_no)
			group by 1
			) tbl3 using(report_date); 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-05-14'::date+7+1 then exit; -- change here
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.eid_comparisons_2;
