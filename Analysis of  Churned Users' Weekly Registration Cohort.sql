/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1633512565
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

select 
	report_date, 
	count(case when back_to_reg='Reg in 3 weeks' then churned_mobile else null end) reg_in_3_weeks,
	count(case when back_to_reg='Reg in 4 to 6 weeks' then churned_mobile else null end) reg_in_4_to_6_weeks,
	count(case when back_to_reg='Reg in 7 to 9 weeks' then churned_mobile else null end) reg_in_7_to_9_weeks,
	count(case when back_to_reg='Reg in more than 10 weeks' then churned_mobile else null end) reg_in_more_than_10_weeks,
	count(distinct churned_mobile) total_churns
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
		(select date(data_generation_time) report_date, mobile_no churned_mobile
		from tallykhata.historical_users_segmented_new_modality
		) tbl1 
		
		inner join 
		
		(select mobile_number churned_mobile, date(created_at) reg_date
		from public.register_usermobile 
		) tbl2 using(churned_mobile)
	) tbl1
group by 1
order by 1 asc; 
