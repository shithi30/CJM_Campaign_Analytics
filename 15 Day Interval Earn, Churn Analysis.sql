/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1129088540
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
	Like to know how many common merchants in Jan 1st SPUs and Jan 15 SPUs.
	For example, Jan 1 there are 10,000 SPUs and Jan 15 there are 12,000 SPUs. 
	There some losses from Jan 1 and some new and some winback. So ultimately how of Jan1 is available in Jan15's count.
*/

do $$ 

declare 
	var_date date:='2021-09-01'::date;
	pres_date date; 
	prev_date date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		if date_part('day', var_date) not in(1, 15) then 
			var_date:=var_date+1; 
			if var_date=current_date then exit; 
			end if; 
			continue;  
		elsif date_part('day', var_date)=15 then
			pres_date:=var_date; 
			prev_date:=concat(left(var_date::text, 7), '-01')::date; 
		elsif date_part('day', var_date)=1 then
			pres_date:=var_date; 
			prev_date:=concat(left((var_date-interval '1 month')::text, 7), '-15')::date;
		end if; 
	
		delete from data_vajapora.quarterly_spu_earn_churn  
		where report_date=pres_date; 
	
		insert into data_vajapora.quarterly_spu_earn_churn 
		select report_date, spu, spu_continued, spu_earned, spu_churned
		from 
			(select 
				pres_date report_date, 
				count(tbl1.mobile_no) spu,
				count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) spu_continued, 
				count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) spu_earned 
			from 
				(select mobile_no 
				from tallykhata.tk_spu_aspu_data 
				where 
					pu_type='SPU' 
					and report_date=pres_date
				) tbl1  
				
				left join 
				
				(select mobile_no 
				from tallykhata.tk_spu_aspu_data 
				where 
					pu_type='SPU' 
					and report_date=prev_date
				) tbl2 using(mobile_no)
			) tbl1 
			
			inner join 
			
			(select 
				pres_date report_date, 
				count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) spu_churned 
			from 
				(select mobile_no 
				from tallykhata.tk_spu_aspu_data 
				where 
					pu_type='SPU' 
					and report_date=prev_date 
				) tbl1  
				
				left join 
				
				(select mobile_no 
				from tallykhata.tk_spu_aspu_data 
				where 
					pu_type='SPU' 
					and report_date=pres_date 
				) tbl2 using(mobile_no)
			) tbl2 using(report_date); 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if;
	end loop; 
end $$; 

select *
from data_vajapora.quarterly_spu_earn_churn; 

