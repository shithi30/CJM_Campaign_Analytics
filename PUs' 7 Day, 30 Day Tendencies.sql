/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1651514060
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	>> PUs 7 days tendency #of Credit customer , # of crdit sales, # of credit return
	>> long term credit txn users, # sale to number of user , # of time baki bikri kore # of time baki perot pay. [ > 8 months FAUs ]
*/

-- PUs who have been so at least once in the last 6 months
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, count(distinct to_char(report_date, 'YYYY-MM')) months_pu
from tallykhata.tk_power_users_10
where report_date>=current_date-interval '6 months' and report_date<current_date
group by 1
having count(distinct to_char(report_date, 'YYYY-MM'))>=6; 
	
-- daily tendencies within last 7 days/30 days
do $$

declare 
	var_date date:=current_date-15;
begin 
	raise notice 'New OP goes below:';

	loop
		insert into data_vajapora.help_a 
		select 
			var_date report_date,
			count(case when txn_type='CREDIT_SALE' then auto_id else null end)*1.00/count(distinct mobile_no) pu_avg_credit_sale,
			count(case when txn_type='CREDIT_SALE_RETURN' then auto_id else null end)*1.00/count(distinct mobile_no) pu_avg_credit_sale_return,
			count(distinct case when txn_type='CREDIT_SALE' then contact else null end)*1.00/count(distinct mobile_no) pu_avg_credit_sale_customers,
			count(distinct case when txn_type='CREDIT_SALE_RETURN' then contact else null end)*1.00/count(distinct mobile_no) pu_avg_credit_sale_return_customers                    
		from 
			(select mobile_no, contact, auto_id, input_amount, txn_type, created_datetime
			from tallykhata.tallykhata_fact_info_final 
			where 
				txn_type in('CREDIT_SALE', 'CREDIT_SALE_RETURN')
				and created_datetime>=var_date-7 and created_datetime<var_date -- 7/30 days
			) tbl1 
			
			inner join 
			
			(select distinct mobile_no, report_date created_datetime
			from tallykhata.tk_power_users_10
			where report_date>=var_date-7 and report_date<var_date -- 7/30 days
			) tbl2 using(mobile_no, created_datetime)
		
			inner join 
			
			data_vajapora.help_b tbl3 using(mobile_no); 
	
		raise notice 'Data generated for: %', var_date;  
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	
	end loop; 
end $$;

select *
from data_vajapora.help_a
order by 1; 