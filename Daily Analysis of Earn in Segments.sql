/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=912536365
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): Marketing was talking about some analysis where they could monitor transition of merchants from one segment to another. 
*/

with 
	segment_merchant_date as
	(select date(data_generation_time) segment_date, segment, mobile_no 
	from tallykhata.historical_users_segmented_new_modality 
	) 

select *
from 
	(select tbl1.segment_date segment_date, count(tbl1.mobile_no) earned_init
	from 
		(select * from segment_merchant_date where segment='0: -(Init)') tbl1 
		left join 
		(select * from segment_merchant_date where segment='0: -(Init)') tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.segment_date=tbl1.segment_date-1) 
	where tbl2.segment_date is null
	group by tbl1.segment_date
	) tbl1 
	
	inner join 
	
	(select tbl1.segment_date segment_date, count(tbl1.mobile_no) earned_cust
	from 
		(select * from segment_merchant_date where segment='1: Cust+') tbl1 
		left join 
		(select * from segment_merchant_date where segment='1: Cust+') tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.segment_date=tbl1.segment_date-1) 
	where tbl2.segment_date is null
	group by tbl1.segment_date
	) tbl2 using(segment_date)
	
	inner join 
	
	(select tbl1.segment_date segment_date, count(tbl1.mobile_no) earned_lt
	from 
		(select * from segment_merchant_date where segment='2: LT') tbl1 
		left join 
		(select * from segment_merchant_date where segment='2: LT') tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.segment_date=tbl1.segment_date-1) 
	where tbl2.segment_date is null
	group by tbl1.segment_date
	) tbl3 using(segment_date)
	
	inner join 
	
	(select tbl1.segment_date segment_date, count(tbl1.mobile_no) earned_ht
	from 
		(select * from segment_merchant_date where segment='3: HT') tbl1 
		left join 
		(select * from segment_merchant_date where segment='3: HT') tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.segment_date=tbl1.segment_date-1) 
	where tbl2.segment_date is null
	group by tbl1.segment_date
	) tbl4 using(segment_date)
	
	inner join 
	
	(select tbl1.segment_date segment_date, count(tbl1.mobile_no) earned_churn
	from 
		(select * from segment_merchant_date where segment='4: -(Churn)') tbl1 
		left join 
		(select * from segment_merchant_date where segment='4: -(Churn)') tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.segment_date=tbl1.segment_date-1) 
	where tbl2.segment_date is null
	group by tbl1.segment_date
	) tbl5 using(segment_date) 
where segment_date>'2021-04-21'
order by 1 asc; 