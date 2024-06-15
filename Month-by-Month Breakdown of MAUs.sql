/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1476212653
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Nazrul, requesting you to share a analysis of MAU (Jan - Sep)
	1. First time this month
	2. Active last month
	3. Inactive last month (comeback)
	4. Inactive last 2 months (comeback)
	5. Inactive last 3 or more months (comeback)
*/

do $$

declare 
	var_month int:=1;
begin 
	raise notice 'New OP goes below:'; 
	loop
		-- first active dates on the month
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select mobile_no, min(event_date) this_month_first_event_date, min(date_sequence) this_month_first_date_seq
		from tallykhata.tallykhata_user_date_sequence_final 
		where 
			date_part('year', event_date)=date_part('year', current_date)
			and date_part('month', event_date)=var_month 
		group by 1;
		
		-- after how many months return is observed (null for newbies)
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select 
			tbl1.mobile_no, this_month_first_event_date, this_month_first_date_seq, last_event_date_before_this_month, date_seq,
			(date_part('year', this_month_first_event_date)*12+date_part('month', this_month_first_event_date))
			-
			(date_part('year', last_event_date_before_this_month)*12+date_part('month', last_event_date_before_this_month))
			active_after_months    
		from 
			data_vajapora.help_a tbl1 
			
			left join 
			
			(select mobile_no, event_date last_event_date_before_this_month, date_sequence date_seq
			from tallykhata.tallykhata_user_date_sequence_final 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.this_month_first_date_seq=tbl2.date_seq+1); 
		
		-- distribution
		insert into data_vajapora.mau_stats_temp
		select 
			case 
				when var_month<10 then concat(date_part('year', current_date), '-0', var_month) 
				else concat(date_part('year', current_date), '-', var_month)
			end year_month,
			count(mobile_no) maus,
			count(case when active_after_months is null then mobile_no else null end) first_time_this_month,
			count(case when active_after_months=1 then mobile_no else null end) active_last_month,
			count(case when active_after_months=2 then mobile_no else null end) inactive_last_month,
			count(case when active_after_months=3 then mobile_no else null end) inactive_last_2_months,
			count(case when active_after_months>3 then mobile_no else null end) inactive_last_3_or_more_months
		from data_vajapora.help_b;
		
		raise notice 'Data generated for month: %', var_month;
		var_month:=var_month+1;
		if var_month=date_part('month', current_date)+1 then exit;
		end if; 
	end loop;
	
end $$; 

select *
from data_vajapora.mau_stats_temp;

