/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1364879348
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 

I have analyzed 20,000 current PUs who registered within the last 90 days: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1364879348
Findings:
- 55% enter the PU-zone within 14 days of registration
- 80% enter the PU-zone within 21 days of registration
- 90% enter the PU-zone within 28 days of registration

*/

CREATE OR REPLACE FUNCTION tallykhata.fn_reg_to_first_pu_cumulative_journey()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of days taken for recently registered merchants to enter the PU-zone
Auxiliary data table(s) : tallykhata.reg_to_first_pu_details
Target data table(s)    : tallykhata.reg_to_first_pu_cumulative_journey
*/

declare

begin
	
	-- present PUs who registered within the last 90 days
	drop table if exists tallykhata.reg_to_first_pu_details; 
	create table tallykhata.reg_to_first_pu_details as
	select *, first_pu_date-reg_date days_from_reg_to_first_pu
	from 
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile 
		) tbl1 
		
		inner join 
		
		(select distinct mobile_no
		from tallykhata.tk_power_users_10 
		where report_date=current_date-1
		) tbl2 using(mobile_no)
		
		inner join 
		
		(select mobile_no, min(report_date) first_pu_date 
		from tallykhata.tk_power_users_10 
		group by 1
		) tbl3 using(mobile_no)
	where 
		first_pu_date-reg_date>=10
		and reg_date>=current_date-90-1
	order by random()
	limit 20000; -- sample size
	
	-- cumulative analysis
	drop table if exists tallykhata.reg_to_first_pu_cumulative_journey; 
	create table tallykhata.reg_to_first_pu_cumulative_journey as
	select 
		tbl1.days_from_reg_to_first_pu, 
		tbl3.total_pus,
		tbl1.pus, 
		sum(tbl2.pus) cumulative_pus,
		sum(tbl2.pus)*1.00/total_pus cumulative_pus_pct
	from 
		(select days_from_reg_to_first_pu, count(mobile_no) pus
		from tallykhata.reg_to_first_pu_details
		group by 1
		) tbl1 
		
		inner join 
		
		(select days_from_reg_to_first_pu, count(mobile_no) pus
		from tallykhata.reg_to_first_pu_details
		group by 1
		) tbl2 on(tbl1.days_from_reg_to_first_pu>=tbl2.days_from_reg_to_first_pu),
		
		(select count(mobile_no) total_pus
		from tallykhata.reg_to_first_pu_details
		) tbl3
	group by 1, 2, 3
	order by 1; 

	raise notice 'Analysis completed successfully.'; 

	-- dropping auxiliary tables
	drop table if exists tallykhata.reg_to_first_pu_details; 

END;
$function$
;

/*
select tallykhata.fn_reg_to_first_pu_cumulative_journey(); 

select *
from tallykhata.reg_to_first_pu_cumulative_journey; 
*/
