select
	count(*) all_pus,
	count(case when if_admissible=1 then mobile_no else null end) admissible_pus,
	count(case when if_admissible=0 then mobile_no else null end) inadmissible_pus,
	avg(case when if_admissible=1 then tac else null end) admisible_pu_avg_tac,
	avg(tac) all_pu_avg_tac
from 
	(-- admissibilities defined: within 3 standard deviations
	select 
		*, 
		case 
			when tac>avg_pu_tac+3*stdev_pu_tac then 0
			when tac<avg_pu_tac-3*stdev_pu_tac then 0
			else 1
		end if_admissible
	from 
		(-- merchant-wise tac
		select mobile_no, count(id) tac
		from 
			public.account tbl1
			
			inner join 
			
			(-- PUs of today
			select distinct mobile_no
			from tallykhata.tk_power_users_10 
			where report_date=current_date-1
			) tbl2 using(mobile_no)
		where 
			type=2
			and is_active is true
		group by 1
		) tbl1,	
		
		(-- normal distribution params
		select ceil(avg(tac)) avg_pu_tac, ceil(stddev(tac)) stdev_pu_tac
		from 
			(-- merchant-wise tac
			select mobile_no, count(id) tac
			from 
				public.account tbl1
				
				inner join 
				
				(-- PUs of today
				select distinct mobile_no
				from tallykhata.tk_power_users_10 
				where report_date=current_date-1
				) tbl2 using(mobile_no)
			where 
				type=2
				and is_active is true
			group by 1
			) tbl1
		) tbl2
	) tbl3; 