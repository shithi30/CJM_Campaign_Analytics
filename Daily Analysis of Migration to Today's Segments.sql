/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=726736650
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

select 
	date_segment, 
	
	-- Init
	sum(case when then_segment='0: -(Init)' and now_segment='1: Cust+' then merchants else 0 end) init_to_cust, 
	sum(case when then_segment='0: -(Init)' and now_segment='2: LT' then merchants else 0 end) init_to_lt, 
	sum(case when then_segment='0: -(Init)' and now_segment='3: HT' then merchants else 0 end) init_to_ht, 
	sum(case when then_segment='0: -(Init)' and now_segment='4: -(Churn)' then merchants else 0 end) init_to_churn, 
	
	-- Cust
	sum(case when then_segment='1: Cust+' and now_segment='0: -(Init)' then merchants else 0 end) cust_to_init, 
	sum(case when then_segment='1: Cust+' and now_segment='2: LT' then merchants else 0 end) cust_to_lt, 
	sum(case when then_segment='1: Cust+' and now_segment='3: HT' then merchants else 0 end) cust_to_ht,
	sum(case when then_segment='1: Cust+' and now_segment='4: -(Churn)' then merchants else 0 end) cust_to_churn,
	
	-- LT
	sum(case when then_segment='2: LT' and now_segment='0: -(Init)' then merchants else 0 end) lt_to_init, 
	sum(case when then_segment='2: LT' and now_segment='1: Cust+' then merchants else 0 end) lt_to_cust, 
	sum(case when then_segment='2: LT' and now_segment='3: HT' then merchants else 0 end) lt_to_ht,
	sum(case when then_segment='2: LT' and now_segment='4: -(Churn)' then merchants else 0 end) lt_to_churn,
	
	-- HT
	sum(case when then_segment='3: HT' and now_segment='0: -(Init)' then merchants else 0 end) ht_to_init, 
	sum(case when then_segment='3: HT' and now_segment='1: Cust+' then merchants else 0 end) ht_to_cust, 
	sum(case when then_segment='3: HT' and now_segment='2: LT' then merchants else 0 end) ht_to_lt, 
	sum(case when then_segment='3: HT' and now_segment='4: -(Churn)' then merchants else 0 end) ht_to_churn, 
	
	-- Churn
	sum(case when then_segment='4: -(Churn)' and now_segment='0: -(Init)' then merchants else 0 end) churn_to_init, 
	sum(case when then_segment='4: -(Churn)' and now_segment='1: Cust+' then merchants else 0 end) churn_to_cust, 
	sum(case when then_segment='4: -(Churn)' and now_segment='2: LT' then merchants else 0 end) churn_to_lt, 
	sum(case when then_segment='4: -(Churn)' and now_segment='3: HT' then merchants else 0 end) churn_to_ht
from 
	(select date_segment, then_segment, now_segment, count(tbl1.mobile_no) merchants 
	from 
		(select mobile_no, segment then_segment, date(data_generation_time) date_segment
		from tallykhata.historical_users_segmented_new_modality
		) tbl1 
		
		inner join 
		
		(select mobile_no, segment now_segment
		from tallykhata.historical_users_segmented_new_modality 
		where date(data_generation_time)=current_date
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and then_segment!=now_segment)
	group by 1, 2, 3
	) tbl1
group by 1
order by 1 asc; 
