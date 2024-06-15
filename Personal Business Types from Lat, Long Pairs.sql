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

/*
drop function if exists data_vajapora.lat_long_dist_meters(in lat1 numeric, lon1 numeric, lat2 numeric, lon2 numeric); 
create or replace function data_vajapora.lat_long_dist_meters(in lat1 numeric, lon1 numeric, lat2 numeric, lon2 numeric)
 returns numeric
 language plpgsql
as $function$

declare
	r numeric:=6371000; 	
	dlat numeric;
	dlon numeric;
	a numeric; 
	dist_meters numeric;
begin
	dlat:=lat2*pi()/180-lat1*pi()/180;
	dlon:=lon2*pi()/180-lon1*pi()/180;
	a:=sin(dlat/2)*sin(dlat/2)+cos(lat1*pi()/180)*cos(lat2 *pi()/180)*sin(dlon/2)*sin(dlon/2); 
	dist_meters=2*atan2(sqrt(a), sqrt(1-a))*r; 
	
	return dist_meters; 
end;
$function$
;

select data_vajapora.lat_long_dist_meters(25.267440, 89.009279, 25.270367, 89.010054); -- 334.667373766144
*/

-- 7 days to collect lat-long from 
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as 
select mobile_no, max(event_date)-6 from_loc_date, max(event_date) to_loc_date
from tallykhata.tallykhata_user_date_sequence_final
group by 1; 
	
-- collected lat-long pairs
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as 
select tbl2.tallykhata_user_id, mobile_no, from_loc_date, to_loc_date, lat, long, created_at, create_date
from 
	(select distinct mobile_no
	from test.txn_info_dau_last_45_days
	) tbl1 
	
	inner join 
	
	(select tallykhata_user_id, mobile_number mobile_no
	from public.register_usermobile 
	) tbl2 using(mobile_no)
	
	inner join 
	
	data_vajapora.help_a tbl3 using(mobile_no)
	
	inner join 
	
	(select tallykhata_user_id, lat, long, created_at, date(created_at) create_date
	from public.locations
	) tbl4 on(tbl2.tallykhata_user_id=tbl4.tallykhata_user_id and create_date>=from_loc_date and create_date<=to_loc_date); 

-- for weekdays
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as 
select 
	tbl1.mobile_no, 
	lat_long_pair_weekdays_entries,
	mode_lat_long_pair_weekdays, 
	mode_lat_long_pair_weekdays_entries
from 
	(select 
		mobile_no, 
		count(concat(lat::text, ', ', long::text)) lat_long_pair_weekdays_entries
	from data_vajapora.help_b
	where 
		extract(dow from create_date)!=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1
	) tbl3

	inner join 

	(select 
		mobile_no, 
		mode() within group (order by concat(lat::text, ', ', long::text)) mode_lat_long_pair_weekdays
	from data_vajapora.help_b
	where 
		extract(dow from create_date)!=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1
	) tbl1 using(mobile_no)
	
	inner join 
	
	(select 
		mobile_no, 
		concat(lat::text, ', ' , long::text) lat_long_pair_weekdays,
		count(*) mode_lat_long_pair_weekdays_entries
	from data_vajapora.help_b
	where 
		extract(dow from create_date)!=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1, 2
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.mode_lat_long_pair_weekdays=tbl2.lat_long_pair_weekdays); 

-- for weekends
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as 
select 
	tbl1.mobile_no, 
	lat_long_pair_weekends_entries,
	mode_lat_long_pair_weekends, 
	mode_lat_long_pair_weekends_entries
from 
	(select 
		mobile_no, 
		count(concat(lat::text, ', ', long::text)) lat_long_pair_weekends_entries
	from data_vajapora.help_b
	where 
		extract(dow from create_date)=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1
	) tbl3

	inner join 

	(select 
		mobile_no, 
		mode() within group (order by concat(lat::text, ', ', long::text)) mode_lat_long_pair_weekends
	from data_vajapora.help_b
	where 
		extract(dow from create_date)=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1
	) tbl1 using(mobile_no)
	
	inner join 
	
	(select 
		mobile_no, 
		concat(lat::text, ', ' , long::text) lat_long_pair_weekends,
		count(*) mode_lat_long_pair_weekends_entries
	from data_vajapora.help_b
	where 
		extract(dow from create_date)=5
		and date_part('hour', created_at)>=8 and date_part('hour', created_at)<=19
	group by 1, 2
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.mode_lat_long_pair_weekends=tbl2.lat_long_pair_weekends); 

-- for nights
drop table if exists data_vajapora.help_e;
create table data_vajapora.help_e as 
select 
	tbl1.mobile_no, 
	lat_long_pair_nights_entries,
	mode_lat_long_pair_nights, 
	mode_lat_long_pair_nights_entries
from 
	(select 
		mobile_no, 
		count(concat(lat::text, ', ', long::text)) lat_long_pair_nights_entries
	from data_vajapora.help_b
	where date_part('hour', created_at) in(21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7)
	group by 1
	) tbl3

	inner join 

	(select 
		mobile_no, 
		mode() within group (order by concat(lat::text, ', ', long::text)) mode_lat_long_pair_nights
	from data_vajapora.help_b
	where date_part('hour', created_at) in(21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7)
	group by 1
	) tbl1 using(mobile_no)
	
	inner join 
	
	(select 
		mobile_no, 
		concat(lat::text, ', ' , long::text) lat_long_pair_nights,
		count(*) mode_lat_long_pair_nights_entries
	from data_vajapora.help_b
	where date_part('hour', created_at) in(21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7)
	group by 1, 2
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.mode_lat_long_pair_nights=tbl2.lat_long_pair_nights); 

-- all data together
drop table if exists data_vajapora.personal_from_lat_long_help;
create table data_vajapora.personal_from_lat_long_help as
select 
	*,
	left(mode_lat_long_pair_weekdays, strpos(mode_lat_long_pair_weekdays, ', ') - 1)::numeric lat1,
	right(mode_lat_long_pair_weekdays, strpos(mode_lat_long_pair_weekdays, ', ') - 1)::numeric lon1,
	
	left(mode_lat_long_pair_weekends, strpos(mode_lat_long_pair_weekends, ', ') - 1)::numeric lat2,
	right(mode_lat_long_pair_weekends, strpos(mode_lat_long_pair_weekends, ', ') - 1)::numeric lon2,
	
	left(mode_lat_long_pair_nights, strpos(mode_lat_long_pair_nights, ', ') - 1)::numeric lat3,
	right(mode_lat_long_pair_nights, strpos(mode_lat_long_pair_nights, ', ') - 1)::numeric lon3
from 	
	test.txn_info_dau_last_45_days tbl0
	left join
	(select mobile mobile_no, bi_business_type, shop_name
	from tallykhata.tallykhata_user_personal_info
	) tbl1 using(mobile_no)
	left join
	data_vajapora.help_c tbl2 using(mobile_no)
	left join 
	data_vajapora.help_d tbl3 using(mobile_no)
	left join 
	data_vajapora.help_e tbl4 using(mobile_no); 

-- with predictions generated
drop table if exists data_vajapora.personal_from_lat_long_help_2;
create table data_vajapora.personal_from_lat_long_help_2 as
select 
	mobile_no, 
	shop_name,
	
	lat_long_pair_weekdays_entries, 
	mode_lat_long_pair_weekdays, 
	mode_lat_long_pair_weekdays_entries, 
	lat_long_pair_weekends_entries, 
	mode_lat_long_pair_weekends, 
	mode_lat_long_pair_weekends_entries, 
	lat_long_pair_nights_entries, 
	mode_lat_long_pair_nights, 
	mode_lat_long_pair_nights_entries, 
	
	dist_meters_modes_weekdays_to_weekends,
	
	total_customer_added, 
	total_supplier_added, 
	cash_sale_trv, 
	credit_sale_trv, 
	credit_sale_return_trv, 
	tagada_message_used, 
	
	bi_business_type,
	case when 
		total_customer_added=0 
		and total_supplier_added=0 
		and credit_sale_return_trv=0 
		and tagada_message_used=0 
		and 
			((dist_meters_modes_weekdays_to_weekends<10 or dist_meters_modes_weekdays_to_weekends is null)
			or
			(dist_meters_modes_weekdays_to_nights<10 or dist_meters_modes_weekdays_to_nights is null))
	then 'Personal purpose' else null 
	end predicted_business_type
from 
	(select 
		*, 
		data_vajapora.lat_long_dist_meters(lat1, lon1, lat2, lon2) dist_meters_modes_weekdays_to_weekends,
		data_vajapora.lat_long_dist_meters(lat1, lon1, lat3, lon3) dist_meters_modes_weekdays_to_nights
	from data_vajapora.personal_from_lat_long_help
	) tbl1; 

select *
from data_vajapora.personal_from_lat_long_help_2;

-- see shop names and descriptions of personal users
select mobile_no, shop_name, description
from 
	(select *
	from data_vajapora.personal_from_lat_long_help_2
	where 
		predicted_business_type='Personal purpose'
		and shop_name is not null
	) tbl1 
	
	inner join 
	
	(select mobile_no, description
	from public.journal
	where description is not null and description!=''
	) tbl2 using(mobile_no); 

-- contribution of personal types in DAUs 
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
			
			/*count(case when finalized_business_type='Grocery Business' then mobile_no else null end) grocery_dau,
			count(case when finalized_business_type='Personal purpose' then mobile_no else null end) personal_use_dau,
			count(case when finalized_business_type='Pharmacy Business' then mobile_no else null end) pharmacy_dau,
			count(case when finalized_business_type like '%recharge%' then mobile_no else null end) recharge_dau,
			count(case when 
				(finalized_business_type not in('Grocery Business', 'Personal purpose', 'Pharmacy Business') and finalized_business_type not like '%recharge%')
				or finalized_business_type is null
			then mobile_no else null end) others_dau*/
			
			0 grocery_dau,
			count(case when predicted_business_type is not null then mobile_no else null end) personal_use_dau,
			0 pharmacy_dau,
			0 recharge_dau,
			count(case when predicted_business_type is null then mobile_no else null end) others_dau
		from 
			(select distinct mobile_no 
			from tallykhata.tallykhata_fact_info_final
			where created_datetime=var_date
			) tbl1 
			
			left join 
			
			(select 
				mobile_no, 
				bi_business_type, 
				predicted_business_type,
				case 
					when predicted_business_type is null then bi_business_type
					else predicted_business_type
				end finalized_business_type
			from data_vajapora.personal_from_lat_long_help_2
			) tbl2 using(mobile_no); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.personal_dau_analysis;








