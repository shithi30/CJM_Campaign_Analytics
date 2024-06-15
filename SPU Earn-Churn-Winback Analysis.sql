/*
- Viz: https://docs.google.com/spreadsheets/d/1t_By3e36_-P3gY--LZcTm2MoQRcG_5wAYTXuyace750/edit#gid=0
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Also, it is now visualized on dashboard (bottom right). 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_spu_earn_churn_winback()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Visualization of SPUs' day-to-day dynamism
Date of development     : 03-Jan-22
Version                 : 01
Source data table(s)    : tallykhata.tk_spu_aspu_data 
Auxiliary data table(s) : none
Target data table(s)    : data_vajapora.spu_earn_churn_winback
*/

declare 

begin 
	raise notice 'Data generation started at %.', now(); 
	
	drop table if exists data_vajapora.spu_earn_churn_winback; 
	create table data_vajapora.spu_earn_churn_winback as
	select *
	from 
		(-- total SPUs
		select report_date, count(mobile_no) spus
		from tallykhata.tk_spu_aspu_data 
		where pu_type='SPU' 
		group by 1
		) tbl1 
		
		inner join 
		
		(-- SPUs continued
		select tbl1.report_date, count(distinct tbl1.mobile_no) spu_continued
		from 
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl1  
			
			inner join 
			
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date=tbl2.report_date+1)
		group by 1
		) tbl2 using(report_date)
			 
		inner join 
		
		(-- SPUs earned
		select 
			tbl1.report_date, 
			count(distinct tbl1.mobile_no) spu_earned, 
			count(distinct case when tbl3.min_spu_date=tbl1.report_date then tbl1.mobile_no else null end) spu_earned_new, 
			count(distinct case when tbl3.min_spu_date!=tbl1.report_date then tbl1.mobile_no else null end) spu_earned_winback
		from 
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl1  
			
			left join 
			
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.report_date=tbl2.report_date+1)
			
			left join 
			
			(select mobile_no, min(report_date) min_spu_date 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU'
			group by 1
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
		where tbl2.mobile_no is null
		group by 1
		) tbl3 using(report_date)
		
		inner join 
		
		(-- SPUs churned
		select tbl1.report_date+1 report_date, count(distinct tbl1.mobile_no) spu_churned
		from 
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl1  
			
			left join 
			
			(select report_date, mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where pu_type='SPU' 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.report_date=tbl1.report_date+1)
		where tbl2.mobile_no is null
		group by 1
		) tbl4 using(report_date)
	order by 1; 

	raise notice 'Data generation finished at %.', now();

END;
$function$
;

-- select data_vajapora.fn_spu_earn_churn_winback(); 

select *
from data_vajapora.spu_earn_churn_winback; 
