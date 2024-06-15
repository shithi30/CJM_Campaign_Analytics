/*
- Viz: 
	- cumulative: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=499783346
	- non-cumulative: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=218655772
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- cumulative
do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	
	delete from data_vajapora.monthly_usage_dist_1
	where active_date>=var_date; 
	
	raise notice 'New:'; 
	
	loop
		insert into data_vajapora.monthly_usage_dist_1
		select  
			var_date::date active_date,
			count(case when days_act_pct>=0.10 then mobile_no else null end) active_greater_equal_10_pct_days,
			count(case when days_act_pct>=0.20 then mobile_no else null end) active_greater_equal_20_pct_days,
			count(case when days_act_pct>=0.30 then mobile_no else null end) active_greater_equal_30_pct_days,
			count(case when days_act_pct>=0.40 then mobile_no else null end) active_greater_equal_40_pct_days,
			count(case when days_act_pct>=0.50 then mobile_no else null end) active_greater_equal_50_pct_days,
			count(case when days_act_pct>=0.60 then mobile_no else null end) active_greater_equal_60_pct_days,
			count(case when days_act_pct>=0.70 then mobile_no else null end) active_greater_equal_70_pct_days,
			count(case when days_act_pct>=0.80 then mobile_no else null end) active_greater_equal_80_pct_days,
			count(case when days_act_pct>=0.90 then mobile_no else null end) active_greater_equal_90_pct_days
		from 
			(select mobile_no, count(event_date)*1.00/30 days_act_pct
			from data_vajapora.user_date_seq 
			where event_date>var_date-30 and event_date<=var_date
			group by 1
			) tbl1; 
		
		raise notice '%', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	end loop; 
	
end $$; 

/*
truncate table data_vajapora.monthly_usage_dist_1; 

select *
from data_vajapora.monthly_usage_dist_1
order by 1 desc; 
*/

-- non-cumulative
do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	
	delete from data_vajapora.monthly_usage_dist_3
	where active_date>=var_date; 
	
	raise notice 'New:'; 
	
	loop
		insert into data_vajapora.monthly_usage_dist_3
		select  
			var_date::date active_date,
			count(case when days_act_pct>=0.00 and days_act_pct<0.10 then mobile_no else null end) active_0_to_10_pct_days,
			count(case when days_act_pct>=0.10 and days_act_pct<0.20 then mobile_no else null end) active_10_to_20_pct_days,
			count(case when days_act_pct>=0.20 and days_act_pct<0.30 then mobile_no else null end) active_20_to_30_pct_days,
			count(case when days_act_pct>=0.30 and days_act_pct<0.40 then mobile_no else null end) active_30_to_40_pct_days,
			count(case when days_act_pct>=0.40 and days_act_pct<0.50 then mobile_no else null end) active_40_to_50_pct_days,
			count(case when days_act_pct>=0.50 and days_act_pct<0.60 then mobile_no else null end) active_50_to_60_pct_days,
			count(case when days_act_pct>=0.60 and days_act_pct<0.70 then mobile_no else null end) active_60_to_70_pct_days,
			count(case when days_act_pct>=0.70 and days_act_pct<0.80 then mobile_no else null end) active_70_to_80_pct_days,
			count(case when days_act_pct>=0.80 and days_act_pct<0.90 then mobile_no else null end) active_80_to_90_pct_days,
			count(case when days_act_pct>=0.90 and days_act_pct<=1.00 then mobile_no else null end) active_90_to_100_pct_days
		from 
			(select mobile_no, count(event_date)*1.00/30 days_act_pct
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>var_date-30 and event_date<=var_date
			group by 1
			) tbl1; 
		
		raise notice '%', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	end loop; 
	
end $$; 

/*
truncate table data_vajapora.monthly_usage_dist_3; 

select *
from data_vajapora.monthly_usage_dist_3
order by 1 desc; 
*/













