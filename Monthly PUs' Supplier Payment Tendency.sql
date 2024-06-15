/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1sxB47kgTp2T1W8JDBt46KFsG6BgC-5utdoox1W7T_vQ/edit#gid=1281092695
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Sithi, we need a supplier payment report among PUs. 
	Total supplier pelam per month (per PU), and average delay between pelam and dilam. For the second part, logic may be data dependent. 
	Feel free to discuss if you want.
	
	Month-by-month PUs' supplier payment tendencies are (initially) ready. 
	- PUs are paying suppliers ~ 1.75 lac taka in recent months. 
	- We have traced 'credit purchase return's back to their corresponding 'credit purchase's for merchant-supplier pairs and have found an avg. delay of ~ 2 days (subject to change if logic changed) for repaying supplier credit.
*/

do $$

declare 
	var_month int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.pu_supplier_payment_stats
		where "month"=to_char(concat('2021-', var_month::text, '-01')::date, 'YYYY-MM'); 
	
		-- for supplier payment time
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *, row_number() over(partition by mobile_no, contact order by created_timestamp asc) seq
		from 
			(select mobile_no, contact, created_timestamp, txn_type
			from tallykhata.tallykhata_fact_info_final 
			where 
				txn_type in('CREDIT_PURCHASE', 'CREDIT_PURCHASE_RETURN')
				and date_part('month', created_datetime)=var_month
				and date_part('year', created_datetime)=2021
			) tbl1
			
			inner join 
				
			(select distinct mobile_no
			from tallykhata.tk_power_users_10
			where report_date=(date_trunc('month', concat('2021-', var_month::text, '-01')::date)+interval '1 MONTH - 1 day')::date
			) tbl2 using(mobile_no); 
		
		insert into data_vajapora.pu_supplier_payment_stats
		select *
		from 
			(-- supplier payment TRV
			select 
				to_char(concat('2021-', var_month::text, '-01')::date, 'YYYY-MM') "month",
				count(distinct mobile_no) pus_did_supplier_payment,
				sum(cleaned_amount) pus_total_supplier_payment,
				sum(cleaned_amount)*1.00/count(distinct mobile_no) pus_avg_supplier_payment
			from 
				(select mobile_no, cleaned_amount
				from tallykhata.tallykhata_fact_info_final 
				where 
					txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN')
					and date_part('month', created_datetime)=var_month
					and date_part('year', created_datetime)=2021
				) tbl1
				
				inner join 
					
				(select distinct mobile_no
				from tallykhata.tk_power_users_10
				where report_date=(date_trunc('month', concat('2021-', var_month::text, '-01')::date)+interval '1 MONTH - 1 day')::date
				) tbl2 using(mobile_no)
			) tbl1,
			
			(-- supplier payment time
			select avg(gap_days_between_supplier_pelam_dilam) avg_gap_days_between_supplier_pelam_dilam
			from 
				(select 
					-- *, 
					tbl1.mobile_no, 
					avg(date_part('day', tbl2.created_timestamp-tbl1.created_timestamp)) gap_days_between_supplier_pelam_dilam
				from 
					data_vajapora.help_a tbl1 
					inner join 
					data_vajapora.help_a tbl2 
					on(tbl1.mobile_no=tbl2.mobile_no and tbl1.contact=tbl2.contact and tbl1.seq=tbl2.seq-1)
				where 
					tbl1.txn_type='CREDIT_PURCHASE' 
					and tbl2.txn_type='CREDIT_PURCHASE_RETURN'
				group by 1
				) tbl1
			) tbl2; 
		
		raise notice 'Data prepared for: %', var_month; 
		var_month:=var_month+1;
		if var_month=10 then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.pu_supplier_payment_stats;

