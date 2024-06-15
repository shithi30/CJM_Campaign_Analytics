/*
- Viz: https://docs.google.com/spreadsheets/d/12UNFsgcls6frQVlJoR_ZCsuO7i30EPKKyvpeCxOUnNI/edit#gid=0
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
	For lending, we need to know total sales of merchants. 
	We would like to know count of merchants whose total TRV >= 3* Tally TRV and Total TRV >= 3 lac taka for at least 25 days in last 30 days. 
	Is this number increasing?
*/

do $$

declare 
	var_date date:='2021-09-01'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		-- merchants who transacted >=25 days in the last 30 days
		drop table if exists data_vajapora.temp_b; 
		create table data_vajapora.temp_b as
		select mobile_no, count(created_datetime) txn_rec_days_last_30_days
		from tallykhata.tallykhata_transacting_user_date_sequence_final  
		where created_datetime>=var_date-30 and created_datetime<var_date 
		group by 1 
		having count(created_datetime)>=25;
		raise notice 'step-01 completed'; 
			
		-- last 30 days' transactions
		drop table if exists data_vajapora.temp_a; 
		create table data_vajapora.temp_a as
		select created_datetime, mobile_no, input_amount, txn_type 
		from tallykhata.tallykhata_fact_info_final  
		where 
			created_datetime>=var_date-30 and created_datetime<var_date 
			and is_suspicious_txn=0; 
		raise notice 'step-02 completed';
		
		-- necessary metrics
		insert into data_vajapora.lendworty_trends_init
		select 
			var_date report_date, 
			(select count(mobile_no) from data_vajapora.temp_b) transacted_25_or_more_days_last_30_days,
			count(mobile_no) lendworthy_merchants
		from 
			(select 
				mobile_no, 
				sum(input_amount) trv_last_30_days, 
				sum(case when txn_type in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE') then input_amount else 0 end) credit_trv_last_30_days
			from data_vajapora.temp_a
			group by 1
			) tbl1 
			
			inner join 
			
			data_vajapora.temp_b tbl2 using(mobile_no)
		where 
			trv_last_30_days>=credit_trv_last_30_days*3
			and trv_last_30_days>=300000; 	
	
		raise notice 'Data generated for: %', var_date; 
		var_date=var_date+1; 
		if var_date='2021-09-12'::date then exit; 
		end if; 
	end loop; 

end $$; 

select *
from data_vajapora.lendworty_trends_init
order by 1; 