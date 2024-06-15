/*
- Viz: 308.png
- Data: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit#gid=1910209108
- Function: 
- Table:
- File: 
- Email thread: 
- Notes (if any): 
*/


do $$

declare 
	var_date date:='2021-05-10'; 
begin

	if var_date>='2021-05-09' then
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select mobile_no
			from data_vajapora.version_wise_days
			where 
				app_version_name='3.0.0'
				and date(update_or_reg_datetime)=var_date
				and date(update_or_reg_datetime)=date(reg_datetime)
			) tbl1 
			
			inner join 
			
			(select mobile_no, entry_type, txn_type
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl2 using(mobile_no); 
	else 
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select mobile mobile_no
			from tallykhata.tallykhata_user_personal_info
			where registration_date=var_date 
			) tbl1 
			
			inner join 
			
			(select mobile_no, entry_type, txn_type
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl2 using(mobile_no); 	
	end if; 
	
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select 
		var_date reg_plus_txn_date,
		sum(case when entry_type=1 then 1 else 0 end)*1.00/count(distinct case when entry_type=1 then mobile_no else null end) trt_rate,
		sum(case when txn_type='Add Customer' then 1 else 0 end)*1.00/count(distinct case when txn_type='Add Customer' then mobile_no else null end) tac_rate,
		sum(case when txn_type='Add Supplier' then 1 else 0 end)*1.00/count(distinct case when txn_type='Add Supplier' then mobile_no else null end) tas_rate
	from data_vajapora.help_a; 

end $$; 

select *
from data_vajapora.help_b;