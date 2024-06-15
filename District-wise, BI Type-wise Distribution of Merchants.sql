/*
- Viz: 
	- piv: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=215570700
	- piv vals: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=855964845
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1205923448
- Function: 
- Table:
- File: 
- Path: 
- Presentation: 
- Email thread: District-wise Type-wise Merchants
- Notes (if any): 
*/

select 
	case when district_name is not null then district_name else 'location unavailable' end district, 
	new_bi_business_type, 
	count(mobile) merchants
from 
	(select mobile, new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	) tbl1 
	
	left join 
	
	(select mobile, district_name
	from data_vajapora.tk_users_location_sample_final  
	) tbl2 using(mobile)
-- where district_name is not null
group by 1, 2
order by 3 desc; 
