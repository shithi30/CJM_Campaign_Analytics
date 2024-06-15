/*
- Viz: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/dQKnB
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=287873983
- Function: data_vajapora.fn_pu_reg_week_info()
- Table: data_vajapora.pu_reg_week_info
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_pu_reg_week_info()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of what weekly registration-cohort PUs are coming from 
Auxiliary data table(s) : none
Target data table       : data_vajapora.pu_reg_week_info
*/

declare 
	var_date date:=current_date-7; 
begin 
	
	-- deleting recent data for regeneration
	delete from data_vajapora.pu_reg_week_info where report_date>=var_date;
	
	loop
		raise notice 'Inserting data for: %', var_date; 	
	
		-- inserting recent data
		insert into data_vajapora.pu_reg_week_info
		select 
			report_date, 
			count(case when back_to_reg='Reg in 3 weeks' then pu else null end) reg_in_3_weeks,
			count(case when back_to_reg='Reg in 4 to 6 weeks' then pu else null end) reg_in_4_to_6_weeks,
			count(case when back_to_reg='Reg in 7 to 9 weeks' then pu else null end) reg_in_7_to_9_weeks,
			count(case when back_to_reg='Reg in more than 10 weeks' then pu else null end) reg_in_more_than_10_weeks
		from 
			(select 
				*, 
				case
					when report_date-reg_date<=21 then 'Reg in 3 weeks'
					when report_date-reg_date<=42 then 'Reg in 4 to 6 weeks'
					when report_date-reg_date<=63 then 'Reg in 7 to 9 weeks'
					else 'Reg in more than 10 weeks'
				end back_to_reg
			from 
				(select distinct report_date, mobile_no pu
				from data_vajapora.tk_power_users_10
				where report_date=var_date
				) tbl1 
				
				inner join 
				
				(select mobile_number pu, date(created_at) reg_date
				from public.register_usermobile 
				) tbl2 using(pu)
			) tbl1
		group by 1; 
	
		-- generating data till yesterday
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

END;
$function$
;

/*
select data_vajapora.fn_pu_reg_week_info();

select *
from data_vajapora.pu_reg_week_info;
*/
