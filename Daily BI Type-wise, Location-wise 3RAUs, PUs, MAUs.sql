CREATE OR REPLACE FUNCTION tallykhata.fn_daily_pu_rau_3_dau_mau_loc_bi_type_distribution()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
	Authored by             : Shithi Maitra
	Supervised by           : Md. Nazrul Islam
	Purpose                 : Location-wise, BI Type-wise analysis of daily PUs, 3RAUs, MAUs, DAUs
	Auxiliary data table(s) : none
	Target data table(s)    : 
							  - tallykhata.rau_3_loc_bi_type
							  - tallykhata.pu_loc_bi_type
							  - tallykhata.mau_loc_bi_type
							  - tallykhata.dau_loc_bi_type
*/

declare
	var_date date:=current_date-10;
begin
	
	-- deleting backdated info.
	delete from tallykhata.rau_3_loc_bi_type where report_date>=var_date;
	delete from tallykhata.pu_loc_bi_type where report_date>=var_date; 
	delete from tallykhata.mau_loc_bi_type where report_date>=var_date; 
	delete from tallykhata.dau_loc_bi_type where report_date>=var_date; 

	-- inserting renewed data till date
	raise notice 'Data insertion started';

	loop 
		-- for 3RAU
		insert into tallykhata.rau_3_loc_bi_type
		select report_date, tbl1.mobile_no, new_bi_business_type, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
		from 
			(select distinct mobile_no, report_date::date
			from tallykhata.regular_active_user_event as s 
			where 
				rau_category=3 
				and report_date::date=var_date
			) tbl1 
			
			left join 
			
			(select mobile mobile_no, new_bi_business_type 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile mobile_no, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
			from tallykhata.tallykhata_clients_location_info 
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
		
		-- for PU
		insert into tallykhata.pu_loc_bi_type
		select report_date, tbl1.mobile_no, new_bi_business_type, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
		from 
			(select distinct mobile_no, report_date 
			from tallykhata.tk_power_users_10
			where report_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile mobile_no, new_bi_business_type 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile mobile_no, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
			from tallykhata.tallykhata_clients_location_info 
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
		
		-- for MAU
		insert into tallykhata.mau_loc_bi_type
		select var_date report_date, tbl1.mobile_no, new_bi_business_type, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
		from 
			(select distinct mobile_no
			from tallykhata.tallykhata_user_date_sequence_final
			where event_date>var_date-30 and event_date<=var_date
			) tbl1 
			
			left join 
			
			(select mobile mobile_no, new_bi_business_type 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile mobile_no, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
			from tallykhata.tallykhata_clients_location_info 
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
		
		-- for DAU
		insert into tallykhata.dau_loc_bi_type
		select report_date, tbl1.mobile_no, new_bi_business_type, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
		from 
			(select mobile_no, event_date report_date
			from tallykhata.tallykhata_user_date_sequence_final
			where event_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile mobile_no, new_bi_business_type 
			from tallykhata.tallykhata_user_personal_info
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile mobile_no, division_name, district_name, upazilla_name, union_name, city_corporation_name, area_type
			from tallykhata.tallykhata_clients_location_info 
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no);
		
		raise notice 'Data generated for: %', var_date;
		
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop;

END;
$function$
;

/*
select tallykhata.fn_daily_pu_rau_3_dau_mau_loc_bi_type_distribution(); 

select report_date, count(mobile_no) rau_3s
from tallykhata.rau_3_loc_bi_type
group by 1
order by 1 desc;

select report_date, count(mobile_no) pus
from tallykhata.pu_loc_bi_type
group by 1
order by 1 desc;

select report_date, count(mobile_no) maus
from tallykhata.mau_loc_bi_type
group by 1
order by 1 desc;

select report_date, count(mobile_no) daus
from tallykhata.dau_loc_bi_type
group by 1
order by 1 desc;
*/
