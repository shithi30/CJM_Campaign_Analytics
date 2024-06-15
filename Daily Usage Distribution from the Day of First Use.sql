/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=333719855
- Data: 
- Function: 
- Table: data_vajapora.usage_dist_init 
- File: 
- Path: 
- Presentation: 
- Email thread: 
- Notes (if any): 

	"Credit scoring.
	% of days used.
	At least 30 days used.
	havey user or not regular user.
	Try to understnad regular user
	use started at least 30 days ago.
	Trend will show churning behaviour.
	Slop of this graph is very interesting.
	Report on total users. -- Stacked Chart.
	Churning Flag.
	Db record trend follow usingscript."
	req. sheet (11 no.): https://docs.google.com/spreadsheets/d/1ha3F8XpWvAAsLvOCokY1ykgZfAEdZgm8B3JbI2kr7gw/edit?pli=1#gid=537331623

*/

-- prepopulating data_vajapora.user_date_seq
drop table if exists data_vajapora.user_date_seq;
create table data_vajapora.user_date_seq as
select *, row_number() over(partition by mobile_no order by event_date) user_date_seq
from 
	(select distinct mobile_no, event_date
	from tallykhata.event_transacting_fact
	where event_name='app_opened'
	) tbl1; 

-- generating daily data
do $$

declare
	var_date date:='2021-02-15';
begin

	delete from data_vajapora.usage_dist_init 
	where "date">=var_date; 

	loop
		insert into data_vajapora.usage_dist_init
		select 
			event_date "date", 
			
			count(mobile_no) "DAUs registered >=30 days back",
			
			count(case when usage_dist_cat='active for 0%-20% days' then mobile_no else null end) "active for 0%-20% days",
			count(case when usage_dist_cat='active for 20%-40% days' then mobile_no else null end) "active for 20%-40% days",
			count(case when usage_dist_cat='active for 40%-60% days' then mobile_no else null end) "active for 40%-60% days",
			count(case when usage_dist_cat='active for 60%-80% days' then mobile_no else null end) "active for 60%-80% days",
			count(case when usage_dist_cat='active for 80%-100% days' then mobile_no else null end) "active for 80%-100% days",
			
			count(case when usage_dist_cat='active for 0%-20% days' then mobile_no else null end)*1.00/count(mobile_no) "active for 0%-20% days pct",
			count(case when usage_dist_cat='active for 20%-40% days' then mobile_no else null end)*1.00/count(mobile_no) "active for 20%-40% days pct",
			count(case when usage_dist_cat='active for 40%-60% days' then mobile_no else null end)*1.00/count(mobile_no) "active for 40%-60% days pct",
			count(case when usage_dist_cat='active for 60%-80% days' then mobile_no else null end)*1.00/count(mobile_no) "active for 60%-80% days pct",
			count(case when usage_dist_cat='active for 80%-100% days' then mobile_no else null end)*1.00/count(mobile_no) "active for 80%-100% days pct"
		from 
			(select 
				*, user_date_seq*1.00/(event_date-first_use_date+1) usage_dist_ratio,
				case 
					when user_date_seq*1.00/(event_date-first_use_date+1)>=0 and user_date_seq*1.00/(event_date-first_use_date+1)<=0.2 then 'active for 0%-20% days' 
					when user_date_seq*1.00/(event_date-first_use_date+1)>0.2 and user_date_seq*1.00/(event_date-first_use_date+1)<=0.4 then 'active for 20%-40% days' 
					when user_date_seq*1.00/(event_date-first_use_date+1)>0.4 and user_date_seq*1.00/(event_date-first_use_date+1)<=0.6 then 'active for 40%-60% days' 
					when user_date_seq*1.00/(event_date-first_use_date+1)>0.6 and user_date_seq*1.00/(event_date-first_use_date+1)<=0.8 then 'active for 60%-80% days' 
					when user_date_seq*1.00/(event_date-first_use_date+1)>0.8 and user_date_seq*1.00/(event_date-first_use_date+1)<=1.0 then 'active for 80%-100% days' 
				end usage_dist_cat
			from 
				(select *
				from data_vajapora.user_date_seq 
				where event_date=var_date -- DAUs of the date (with the latest day-sequence till then)
				) tbl1
				
				inner join 
				
				(select mobile_number mobile_no
				from public.register_usermobile
				where date(created_at)<var_date-30 -- at least 30 days passed after reg. 
				) tbl2 using(mobile_no)
				
				inner join 
				
				(select mobile_no, event_date first_use_date 
				from data_vajapora.user_date_seq 
				where user_date_seq=1 -- first active day
				) tbl3 using(mobile_no)
			) tbl1 
		group by 1;
	
		raise notice 'Data inserted for: %', var_date; 
	
		var_date=var_date+1;
		if(var_date=current_date) then exit;
		end if; 
	end loop; 

end $$; 

/*
truncate table data_vajapora.usage_dist_init; 
select *
from data_vajapora.usage_dist_init; 
*/