/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1YUQhv51vvSZ3P4hR7lmu6Wy62cRPTWUXdGQMD_oqbDI/edit#gid=2082138713
- Table:
- File: 
- Email thread: 
- Notes (if any): 
*/


do $$

declare 
	var_start date:='2021-04-01';
	var_end date:='2021-04-12';
begin 
	-- for txns 
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select mobile_no, txn_type, auto_id, contact
	from tallykhata.tallykhata_fact_info_final
	where 
		created_datetime>=var_start and created_datetime<=var_end
		and txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN', 'CREDIT_PURCHASE', 'Add Supplier', 'Add Customer'); 
	
	-- for events
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select pk_id, mobile_no, event_date, event_name
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date>=var_start and event_date<=var_end
		and event_name='add_customer';

	-- metrics
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as
	select concat(var_start, ' to ', var_end) date_range, *
	from 
		(select count(distinct mobile_no) users_supp_txn
		from data_vajapora.help_a
		where txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN', 'CREDIT_PURCHASE', 'Add Supplier')
		) tbl1,
		
		(select count(distinct contact) supp_added
		from data_vajapora.help_a
		where txn_type in('Add Supplier')
		) tbl2,
	
		(select count(auto_id) supp_trt
		from data_vajapora.help_a
		where txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN', 'CREDIT_PURCHASE')
		) tbl3, 
		
		(select count(distinct tbl1.mobile_no) event_but_no_trt_users
		from 
			data_vajapora.help_c tbl1 
			left join 
			data_vajapora.help_a tbl2 using(mobile_no)
		where tbl2.mobile_no is null
		) tbl4, 
		
		(select count(distinct tbl1.pk_id) supp_events
		from 
			data_vajapora.help_c tbl1 
			left join 
			(select mobile_no 
			from data_vajapora.help_a
			where txn_type in('Add Customer', 'Add Supplier')
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null
		) tbl5, 
		
		(select count(mobile_no) users_fst_supp_txn 
		from 
			(select mobile_no, min(created_datetime) fst_supp_txn
	 		from tallykhata.tallykhata_fact_info_final 
			where txn_type in('CASH_PURCHASE', 'CREDIT_PURCHASE_RETURN', 'CREDIT_PURCHASE', 'Add Supplier')
			group by 1 
			having min(created_datetime)>=var_start and min(created_datetime)<=var_end
			) tbl1
			
			inner join 
			
			(select mobile mobile_no, registration_date reg_date 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 using(mobile_no)
		where fst_supp_txn-reg_date>=30
		) tbl6; 
end $$; 

select *
from data_vajapora.help_b; 

