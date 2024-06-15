/*
- Viz: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/dQKnB
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

CREATE OR REPLACE FUNCTION tallykhata.fn_spu_registration_cohort_trends()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	
begin 
	drop table if exists tallykhata.spu_registration_cohort_trends; 
	create table tallykhata.spu_registration_cohort_trends as
	select 
		report_date, 
		
		count(mobile_no) spus, 
		
		count(case when reg_date>=report_date-42 and reg_date<report_date then mobile_no else null end) reg_in_6_weeks, 
		count(case when reg_date>=report_date-84 and reg_date<report_date-42 then mobile_no else null end) reg_in_7_to_12_weeks, 
		count(case when reg_date>=report_date-168 and reg_date<report_date-84 then mobile_no else null end) reg_in_13_to_24_weeks, 
		count(case when reg_date>=report_date-336 and reg_date<report_date-168 then mobile_no else null end) reg_in_25_to_48_weeks, 
		count(case when reg_date>=report_date-672 and reg_date<report_date-336 then mobile_no else null end) reg_in_49_to_96_weeks, 
		
		count(case when reg_date<report_date-672 then mobile_no else null end) reg_in_more_than_96_weeks, 
		
		count(case when reg_date>=report_date then mobile_no else null end) reg_after_report_date
	from 
		(select report_date, mobile_no
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type in ('SPU','Sticky SPU')
			and report_date>=current_date-210 and report_date<current_date
		) tbl1 
		
		inner join 
		
		(select date(created_at) reg_date, mobile_number mobile_no
		from public.register_usermobile  
		) tbl2 using(mobile_no)
	group by 1 
	order by 1; 
		
END;
$function$
;

-- select tallykhata.fn_spu_registration_cohort_trends(); 

select *
from tallykhata.spu_registration_cohort_trends; 
