/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1YuCSAAwhmdtc3gWcUk9b3NW6RxxN8mXxHr46_NjbJ8k/edit#gid=1293725545
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: SUs Around Mosques
- Notes (if any): 
*/

-- mosque locations
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a(mosque_lat numeric, mosque_lng numeric); 
insert into data_vajapora.help_a values 
(23.773982681166856, 90.3559741527739), 
(23.770680130551046, 90.36239770227928), 
(23.76813710944044, 90.36755819429764), 
(23.768071056281723, 90.36560947703197), 
(23.768064807337534, 90.35990767477993), 
(23.76654557535326, 90.36023246104513), 
(23.765774483120687, 90.35553896083063), 
(23.764296709689024, 90.35316377785111), 
(23.76300057568362, 90.35792738075035), 
(23.762922021692603, 90.3590860949691), 
(23.760879601283264, 90.35385042331407), 
(23.759936934853453, 90.3586140267768), 
(23.76119382184852, 90.35947233360551), 
(23.762686359389544, 90.36213308477446), 
(23.762411419549732, 90.36457925923621), 
(23.763079129581623, 90.36655336494222), 
(23.761547319128873, 90.36788374052668), 
(23.760447546659105, 90.36552339674778), 
(23.760054768524864, 90.364064275139), 
(23.75789446759892, 90.36423593650474); 

-- SU info.
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select * 
from 
	(select distinct mobile_no
	from cjm_segmentation.retained_users 
	where 
		report_date=current_date-1
		and tg like '%SPU%'
	) tbl1 
	
	left join 
	
	(select mobile mobile_no, max(coalesce(shop_name, business_name, name, merchant_name)) shop_name 
	from tallykhata.tallykhata_user_personal_info
	group by 1
	) tbl3 using(mobile_no)
	
	inner join 
		
	(select mobile mobile_no, lat::numeric lat_su, lng::numeric lng_su
	from tallykhata.tallykhata_clients_location_info
	) tbl2 using(mobile_no); 

-- SUs within radius
select 
	mosque_id, mosque_lat, mosque_lng,
	mobile_no, shop_name, 
	data_vajapora.lat_long_dist_meters(mosque_lat, mosque_lng, lat_su, lng_su) dist_meters, 
	row_number() over(partition by mosque_id order by data_vajapora.lat_long_dist_meters(mosque_lat, mosque_lng, lat_su, lng_su) asc) proximity_serial, 
	concat('https://maps.google.com/?q=', lat_su, ',', lng_su) location_url
from 
	(select *, row_number() over() mosque_id
	from data_vajapora.help_a
	) tbl1, 
	data_vajapora.help_b tbl2 
where data_vajapora.lat_long_dist_meters(mosque_lat, mosque_lng, lat_su, lng_su)<=100
order by 1, 7; 