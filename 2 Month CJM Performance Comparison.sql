/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=249877441
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Discussion Notes & Requirements!
- Notes (if any): 
*/

-- May
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as 
select mobile_no, count(id) inbox_visit_times
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_name in('inbox_message_open')
	and event_date>='2022-05-01' and event_date<='2022-05-31'
group by 1; 
	
-- June
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as 
select mobile_no, count(id) inbox_visit_times
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_name in('inbox_message_open')
	and event_date>='2022-06-01' and event_date<='2022-06-30'
group by 1; 
	
-- summary
select
	tg, 
	count(tbl1.mobile_no) merchants_visited_inbox_may, 
	count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) merchants_did_not_visit_inbox_june
from 
	data_vajapora.help_a tbl1 
	
	left join 
	
	data_vajapora.help_b tbl2 using(mobile_no) 
	
	left join 
		
	(select 
		mobile_no,
		max(
			case 
				when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
				when tg in('LTUCb','LTUTa') then 'LTU'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('NT--') then 'NT'
				when tg in('PSU') then 'PSU'
				when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
				when tg in('SPU') then 'SU'
				when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
				else null
			end
		) tg
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl3 using(mobile_no)
group by 1; 

-- May ranges
select inbox_visit_times_cat, count(mobile_no) merchants 
from 
	(select 
		*, 
		case 
			when inbox_visit_times<=5 then '01-05'
			when inbox_visit_times<=10 then '06-10'
			when inbox_visit_times<=20 then '11-20'
			when inbox_visit_times<=50 then '21-50'
			when inbox_visit_times>50 then '50+'
		end inbox_visit_times_cat
	from data_vajapora.help_a
	) tbl1 
group by 1 
order by 1; 

select inbox_visit_times, count(mobile_no) merchants 
from data_vajapora.help_a
group by 1 
order by 1; 

-- May-June ranges
select inbox_visit_times_cat, count(mobile_no) merchants 
from 
	(select 
		tbl1.mobile_no, 
		case 
			when tbl1.inbox_visit_times<=5 then '01-05'
			when tbl1.inbox_visit_times<=10 then '06-10'
			when tbl1.inbox_visit_times<=20 then '11-20'
			when tbl1.inbox_visit_times<=50 then '21-50'
			when tbl1.inbox_visit_times>50 then '50+'
		end inbox_visit_times_cat
	from 
		data_vajapora.help_a tbl1 
		left join 
		data_vajapora.help_b tbl2 using(mobile_no)  
	where tbl2.mobile_no is null
	) tbl1 
group by 1 
order by 1; 

select tbl1.inbox_visit_times, count(mobile_no) merchants 
from 
	data_vajapora.help_a tbl1 
	left join 
	data_vajapora.help_b tbl2 using(mobile_no)  
where tbl2.mobile_no is null
group by 1 
order by 1; 

-- June ranges
select inbox_visit_times_cat, count(mobile_no) merchants 
from 
	(select 
		*, 
		case 
			when inbox_visit_times<=5 then '01-05'
			when inbox_visit_times<=10 then '06-10'
			when inbox_visit_times<=20 then '11-20'
			when inbox_visit_times<=50 then '21-50'
			when inbox_visit_times>50 then '50+'
		end inbox_visit_times_cat
	from data_vajapora.help_b
	) tbl1 
group by 1 
order by 1; 

select inbox_visit_times, count(mobile_no) merchants 
from data_vajapora.help_b
group by 1 
order by 1; 

-- type wise distributions
do $$ 

declare 
	var_date date:='2022-05-01'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.help_c 
		where event_date=var_date; 
	
		insert into data_vajapora.help_c 
		select * 
		from 
			(select mobile_no, notification_id message_id, event_date, id
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_name in('inbox_message_open')
				and event_date=var_date
			) tbl1 
			
			left join 
					
			(select distinct message_id, tag
			from data_vajapora.msg_types
			where remarks='inbox'
			) tbl2 using(message_id); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	to_char(event_date, 'YYYY-MM') year_month, 
	tag, 
	count(distinct mobile_no) merchants_opened, 
	count(id) merchants_opened_times
from data_vajapora.help_c tbl1 
group by 1, 2
order by 1, 2; 
