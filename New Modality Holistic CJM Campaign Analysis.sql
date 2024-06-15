/*
- Viz: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1905672433
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
*/

do $$

declare 
	var_date date:=current_date-15; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.new_modality_cjm_analysis_holistic 
		where report_date=var_date; 
	
		insert into data_vajapora.new_modality_cjm_analysis_holistic 
		select *
		from 
			(select 
				created_datetime report_date, 
				
				count(auto_id)*1.00/count(distinct mobile_no) avg_trt, 
				count(case when txn_type_extended='cash' then auto_id else null end)*1.00/count(distinct case when txn_type_extended='cash' then mobile_no else null end) avg_cash_trt,
				count(case when txn_type_extended='tacs' then auto_id else null end)*1.00/count(distinct case when txn_type_extended='tacs' then mobile_no else null end) avg_tacs_trt,
				count(case when txn_type_extended='credit' then auto_id else null end)*1.00/count(distinct case when txn_type_extended='credit' then mobile_no else null end) avg_credit_trt, 
				
				sum(case when is_suspicious_txn=0 then input_amount else 0 end)*1.00/count(distinct mobile_no) avg_trv, 
				sum(case when txn_type_extended='cash' and is_suspicious_txn=0 then input_amount else 0 end)*1.00/count(distinct case when txn_type_extended='cash' then mobile_no else null end) avg_cash_trv,
				sum(case when txn_type_extended='credit' and is_suspicious_txn=0 then input_amount else 0 end)*1.00/count(distinct case when txn_type_extended='credit' then mobile_no else null end) avg_credit_trv
			from 	
				(select 
					mobile_no, 
					input_amount, 
					auto_id, 
					txn_type, 
					is_suspicious_txn, 
					case 
						when txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_PURCHASE', 'EXPENSE', 'CASH_SALE', 'CASH_ADJUSTMENT') then 'cash'
						when txn_type in('Add Customer', 'Add Supplier') then 'tacs'
						when txn_type in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE') then 'credit'
					end txn_type_extended, 
					created_datetime
				from tallykhata.tallykhata_fact_info_final  
				where created_datetime=var_date
				) tbl1 
			group by 1
			) tbl1 
			
			inner join 
			
			(select event_date report_date, avg(sec_with_tk)/60.00 avg_mins_spent 
			from tallykhata.daily_times_spent_individual_data
			where event_date=var_date
			group by 1
			) tbl2 using(report_date)
		
			inner join 
		
			(select event_date report_date, count(distinct mobile_no) merchants_app_opened
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name in('app_opened')
				and event_date=var_date
			group by 1
			) tbl3 using(report_date); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.new_modality_cjm_analysis_holistic; 
