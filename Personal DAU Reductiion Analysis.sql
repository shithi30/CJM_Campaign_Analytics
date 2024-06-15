/*
- Viz: https://docs.google.com/spreadsheets/d/1sxB47kgTp2T1W8JDBt46KFsG6BgC-5utdoox1W7T_vQ/edit#gid=1507613640
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
	var_date date:=current_date-45;
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.personal_dau_analysis
		where report_date=var_date; 
	
		insert into data_vajapora.personal_dau_analysis
		select 
			var_date report_date, 
			count(mobile_no) dau,
			count(case when bi_business_type='Grocery Business' then mobile_no else null end) grocery_dau,
			count(case when bi_business_type='Personal purpose' then mobile_no else null end) personal_use_dau,
			count(case when bi_business_type='Pharmacy Business' then mobile_no else null end) pharmacy_dau,
			count(case when bi_business_type like '%recharge%' then mobile_no else null end) recharge_dau,
			count(case when bi_business_type not in('Grocery Business', 'Personal purpose', 'Pharmacy Business') and bi_business_type not like '%recharge%' then mobile_no else null end) others_dau
		from 
			(select mobile_no
			from tallykhata.tallykhata_sync_event_fact_final
			where created_date=var_date
		
			union 
			
			select mobile_no 
			from tallykhata.tallykhata_fact_info_final
			where created_datetime=var_date
			) tbl1 
			
			left join 
			
			(select mobile mobile_no, bi_business_type 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 using(mobile_no); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.personal_dau_analysis;

