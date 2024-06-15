/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=677536451
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

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select concat(to_char(report_date, 'yyyy-mm'), '-01')::date report_date, count(distinct mobile_no) monthly_pus
from tallykhata.tk_power_users_10 
where report_date>='2021-10-01' and report_date<current_date
group by 1; 

select * 
from 
	-- monthly total PUs
	data_vajapora.help_a tbl1 
	
	inner join 
	
	(-- SPUs earned
	select 
		tbl1.report_date, 
		count(distinct tbl1.mobile_no) spu_earned, 
		count(distinct case when tbl3.min_spu_date=tbl1.report_date then tbl1.mobile_no else null end) spu_earned_new, 
		count(distinct case when tbl3.min_spu_date!=tbl1.report_date then tbl1.mobile_no else null end) spu_earned_winback
	from 
		(select concat(to_char(report_date, 'yyyy-mm'), '-01')::date report_date, mobile_no 
		from tallykhata.tk_spu_aspu_data 
		where pu_type='SPU' 
		) tbl1  
		
		left join 
		
		(select concat(to_char(report_date, 'yyyy-mm'), '-01')::date report_date, mobile_no 
		from tallykhata.tk_spu_aspu_data 
		where pu_type='SPU' 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date=tbl2.report_date+1)
		
		left join 
		
		(select mobile_no, min(concat(to_char(report_date, 'yyyy-mm'), '-01')::date) min_spu_date 
		from tallykhata.tk_spu_aspu_data 
		where pu_type='SPU'
		group by 1
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
	where tbl2.mobile_no is null
	group by 1
	) tbl3 using(report_date); 
