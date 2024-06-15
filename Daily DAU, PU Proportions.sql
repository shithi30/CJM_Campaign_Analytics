do $$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Examine users both in PU and DAU, on a daily basis
Auxiliary data table(s) : data_vajapora.day_dau, data_vajapora.day_pu
Target data table       : data_vajapora.daily_dau_pu_ratio
*/

declare
	var_date date:=current_date-3; -- change
begin 
	
	-- deleting backdated info
	delete from data_vajapora.daily_dau_pu_ratio
	where report_date>=var_date; 
	
	raise notice 'New data is being generated.'; 
	
	loop 
		-- DAUs of the date
		drop table if exists data_vajapora.day_dau; 
		create table data_vajapora.day_dau as
		select distinct mobile_no 
		from tallykhata.event_transacting_fact 
		where event_date=var_date; 
		
		-- PUs of the date
		drop table if exists data_vajapora.day_pu; 
		create table data_vajapora.day_pu as
		select distinct mobile_no 
		from tallykhata.tk_power_users_10 
		where report_date=var_date;
		
		-- DAU and PU intersections of the date
		insert into data_vajapora.daily_dau_pu_ratio
		select 
			var_date::date report_date,
			(select count(mobile_no) from data_vajapora.day_dau) dau,
			(select count(mobile_no) from data_vajapora.day_pu) pu,
			count(mobile_no) dau_and_pu, 
			count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.day_dau) dau_and_pu_to_dau_pct,
			count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.day_pu) dau_and_pu_to_pu_pct
		from 
			data_vajapora.day_dau tbl1 
			inner join 
			data_vajapora.day_pu tbl2 using(mobile_no);
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
	
	-- dropping auxiliary tables 
	drop table if exists data_vajapora.day_dau; 
	drop table if exists data_vajapora.day_pu; 

end $$; 

/*
select *
from data_vajapora.daily_dau_pu_ratio; 
*/
