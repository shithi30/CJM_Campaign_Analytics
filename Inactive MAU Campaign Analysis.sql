/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1065396466
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Request for MAU campaign data!
- Notes (if any): 
*/

do $$ 

declare 
	var_date date:='2021-12-15'::date; 
begin 
	raise notice 'New OP goes below:'; 

	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select distinct mobile_no, event_date 
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		date_part('year', event_date)=date_part('year', var_date)
		and date_part('month', event_date)=date_part('month', var_date)
		and event_name='app_opened'; 
	raise notice 'MAUs generated.';

	loop
		delete from data_vajapora.winback_to_mau_analysis
		where report_date=var_date; 
	
		insert into data_vajapora.winback_to_mau_analysis
		select 
			var_date report_date, 
			count(case when tbl2.mobile_no is null and tbl4.mobile_no is not null then tbl1.mobile_no else null end) tg_inactive, 
			count(case when tbl2.mobile_no is null and tbl4.mobile_no is not null and tbl3.mobile_no is not null then tbl1.mobile_no else null end) winback_to_mau
		from 
			(select mobile_no 
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl1 
			
			left join 
			
			(select distinct mobile_no 
			from data_vajapora.help_a
			where event_date>concat(left(var_date::text, 7), '-01')::date-1 and event_date<var_date
			) tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_no 
			from data_vajapora.help_a
			where event_date=var_date
			) tbl3 using(mobile_no)
			
			left join 
			
			(select mobile_number mobile_no 
			from public.register_usermobile 
			where date(created_at)<=concat(left(var_date::text, 7), '-01')::date-1
			) tbl4 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-12-31'::date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.winback_to_mau_analysis
order by 1; 
