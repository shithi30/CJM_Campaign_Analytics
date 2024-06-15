/*
- Viz: https://docs.google.com/spreadsheets/d/1pQPURgu2txQxKX7ZmDnfY9Du0Lx-55-ch3xfxF25vcw/edit#gid=643239381
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

do $$ 

declare 
	var_date date:=current_date-21; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.dau_loss_analysis
		where report_date=var_date; 
	
		insert into data_vajapora.dau_loss_analysis
		select 
			var_date report_date, 
			to_char(var_date, 'Day') report_weekday, 
			count(mobile_no) daus_traced,
			count(case when active_after_days is null then mobile_no else null end) active_on_reg_day, 
			count(case when active_after_days>=1 and active_after_days<=3 then mobile_no else null end) active_after_1_to_3_days,
			count(case when active_after_days>=4 and active_after_days<=7 then mobile_no else null end) active_after_4_to_7_days,
			count(case when active_after_days>=8 and active_after_days<=10 then mobile_no else null end) active_after_8_to_10_days,
			count(case when active_after_days>=11 and active_after_days<=15 then mobile_no else null end) active_after_11_to_15_days,
			count(case when active_after_days>=16 then mobile_no else null end) active_after_more_than_15_days
		from 
			(select mobile_no, date_sequence-1 date_sequence
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl1
		
			left join 
		
			(select mobile_no, event_date last_active_date, date_sequence, var_date-event_date active_after_days
			from tallykhata.tallykhata_user_date_sequence_final 
			) tbl2 using(mobile_no, date_sequence); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select
	concat(report_date, ' ', left(report_weekday, 3)) report_day,
	total_active_user_db_event daus_on_dashboard,
	daus_traced, 
	active_on_reg_day, 
	active_after_1_to_3_days, 
	active_after_4_to_7_days, 
	active_after_8_to_10_days, 
	active_after_11_to_15_days, 
	active_after_more_than_15_days
from 
	data_vajapora.dau_loss_analysis tbl1 
	
	inner join 
	
	(select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event,
		tbl_2.total_active_user_db,
		to_char(tbl_1.report_date,'yyyymmdd')::int  as serial_no
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event','Non Verified')
		group by d.report_date
		) as tbl_1 
	left join 
	
	(
		select 
			d.report_date,
			'DAU [DB]' as category,
			sum(d.total_active_user) as total_active_user_db
		from tallykhata.tallykhata.daily_active_user_data as d
		where d.category in('(DAU) Verified','Non Verified')
		group by d.report_date
	) as tbl_2 on tbl_1.report_date = tbl_2.report_date
	) tbl2 using(report_date)
order by 1; 
	