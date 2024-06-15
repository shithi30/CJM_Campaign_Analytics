/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=734472445
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): No patterns found.
*/

do $$

declare 
	var_date date:=current_date-60;

begin

	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.help_b
		where created_datetime=var_date; 
	
		insert into data_vajapora.help_b
		select 
			created_datetime, 
			avg(case when new_bi_business_type='Grocery' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_grocery,
			avg(case when new_bi_business_type='Pharmacy' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_pharmacy,
			avg(case when new_bi_business_type='Electronics Store' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_electronics_store,
			avg(case when new_bi_business_type='Fabrics and Cloths' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_fabrics,
			avg(case when new_bi_business_type='Market & Supershop' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_supershop,
			avg(case when new_bi_business_type='MFS-Mobile Recharge Store' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_recharge,
			avg(case when new_bi_business_type ilike '%Wholes%' then expense_to_sale_ratio else null end) avg_expense_to_sale_ratio_wholeseller
		from 
			(select 
				created_datetime, 
				new_bi_business_type,
				mobile_no, 
				sum(case when txn_type in('CREDIT_SALE', 'CASH_SALE') then input_amount else 0 end) sale,
				sum(case when txn_type in('EXPENSE') then input_amount else 0 end) expense,
				sum(case when txn_type in('EXPENSE') then input_amount else 0 end)*1.00/
				sum(case when txn_type in('CREDIT_SALE', 'CASH_SALE') then input_amount else 0 end) expense_to_sale_ratio
			from 
				(select mobile_no, created_datetime, txn_type, input_amount
				from tallykhata.tallykhata_fact_info_final 
				where 
					created_datetime=var_date
					and txn_type in('CREDIT_SALE', 'CASH_SALE', 'EXPENSE')
				) tbl1
				
				inner join 
					
				(select mobile_no
				from tallykhata.tallykahta_regular_active_user_new 
				where 
					rau_category=10
					and rau_date=var_date
				) tbl2 using(mobile_no)
					
				inner join 
				
				(select mobile mobile_no, new_bi_business_type
				from tallykhata.tallykhata_user_personal_info
				) tbl3 using(mobile_no)
			group by 1, 2, 3
			having 
				sum(case when txn_type in('EXPENSE') then input_amount else 0 end)!=0
				and sum(case when txn_type in('CREDIT_SALE', 'CASH_SALE') then input_amount else 0 end)!=0
			) tbl1
		group by 1; 
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$; 

/*
truncate table data_vajapora.help_b;

select *
from data_vajapora.help_b;
*/
