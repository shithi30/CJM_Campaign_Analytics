/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2010136287
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1912163392
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

-- message-wise results
select 
	year_month, 
	notification_id, 
	case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end title, 
	count(tbl1.mobile_no) inactive_mau_winbacks
from 
	(select 
		left(report_date::text, 7) year_month, 
		mobile_no, 
		min(report_date) first_apprered_on_month_through_inbox
	from data_vajapora.mom_cjm_performance_detailed
	where 
		id is not null
		and report_date>'2021-12-31'::date
		and notification_id in(2672, 2681, 2682, 2666, 2672, 2668, 2681, 2666, 2662, 2672, 2668, 2681, 2694, 2682, 2667, 2574, 2687, 2745, 2745, 2693, 2666, 2573, 2662, 2681, 2694, 2745, 2666, 2687, 2667, 2666, 2662, 2694, 2693, 2745, 2682, 2745, 2667, 2694, 2666, 2745, 2745, 2694, 2687, 2745, 2667)
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select mobile_no, report_date, notification_id
	from data_vajapora.mom_cjm_performance_detailed
	where id is not null
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.first_apprered_on_month_through_inbox=tbl2.report_date)
	
	left join 
	
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl3 using(notification_id)
group by 1, 2, 3
order by 1 asc, 4 desc; 

-- summary
select 
	year_month, 
	count(tbl1.mobile_no) inactive_mau_winbacks
from 
	(select 
		left(report_date::text, 7) year_month, 
		mobile_no, 
		min(report_date) first_apprered_on_month_through_inbox
	from data_vajapora.mom_cjm_performance_detailed
	where 
		id is not null
		and report_date>'2021-12-31'::date
		and notification_id in(2672, 2681, 2682, 2666, 2672, 2668, 2681, 2666, 2662, 2672, 2668, 2681, 2694, 2682, 2667, 2574, 2687, 2745, 2745, 2693, 2666, 2573, 2662, 2681, 2694, 2745, 2666, 2687, 2667, 2666, 2662, 2694, 2693, 2745, 2682, 2745, 2667, 2694, 2666, 2745, 2745, 2694, 2687, 2745, 2667)
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select mobile_no, report_date, notification_id
	from data_vajapora.mom_cjm_performance_detailed
	where id is not null
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.first_apprered_on_month_through_inbox=tbl2.report_date)
	
	left join 
	
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl3 using(notification_id)
group by 1
order by 1; 

-- summary (till this point of month)
select 
	year_month, 
	count(tbl1.mobile_no) inactive_mau_winbacks
from 
	(select 
		left(report_date::text, 7) year_month, 
		mobile_no, 
		min(report_date) first_apprered_on_month_through_inbox
	from data_vajapora.mom_cjm_performance_detailed
	where 
		id is not null
		and report_date>'2021-12-31'::date 
		and date_part('day', report_date)<date_part('day', current_date) -- till this point of month
		and notification_id in(2672, 2681, 2682, 2666, 2672, 2668, 2681, 2666, 2662, 2672, 2668, 2681, 2694, 2682, 2667, 2574, 2687, 2745, 2745, 2693, 2666, 2573, 2662, 2681, 2694, 2745, 2666, 2687, 2667, 2666, 2662, 2694, 2693, 2745, 2682, 2745, 2667, 2694, 2666, 2745, 2745, 2694, 2687, 2745, 2667)
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select mobile_no, report_date, notification_id
	from data_vajapora.mom_cjm_performance_detailed
	where id is not null
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.first_apprered_on_month_through_inbox=tbl2.report_date)
	
	left join 
	
	(select id notification_id, title, summary
	from public.notification_pushmessage
	) tbl3 using(notification_id)
group by 1
order by 1; 

-- trends
do $$

declare 
	var_date date:=current_date-3; 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.inactive_mau_analysis
		where report_date=var_date;
	
		-- DAUs
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
			
		/*select mobile_no
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union 
		
		select mobile_no 
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received','inbox_message_received')
			
		union 
			
		select ss.mobile_number mobile_no
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date;*/ 
		
		select mobile_no 
		from tallykhata.tallykhata_user_date_sequence_final 
		where event_date=var_date; 
		
		-- activated MAUs
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select tbl1.mobile_no
		from 
			(select mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl1 
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>concat(left(var_date::text, 7), '-01')::date-1 and event_date<var_date
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
		
		-- first open through inbox (via campaigns or not)
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select * 
		from 
			(select mobile_no, notification_id, bulk_notification_id request_id
			from data_vajapora.mom_cjm_performance_detailed
			where 
				id is not null 
				and report_date=var_date
			) tbl1 
			
			left join 
			
			(select id request_id, title campaign_id
			from public.notification_bulknotificationrequest
			where title in('IM221226-02', 'IM221228-01', 'IM221228-02', 'IM221229-01', 'IM221229-02', 'IM220130-02', 'IM221231-01', 'IM221231-02', 'IM220211-01', 'IM220211-02', 'IM220212-01', 'IM220212-02', 'IM220213-01', 'IM220214-01', 'IM220214-02', 'IM220215-01', 'IM220215-02', 'IM220216-01', 'IM220216-01', 'IM220217-01', 'IM220217-02', 'IM220218-01', 'IM220218-02', 'IM220219-01', 'IM220220-01', 'IM220221-01', 'IM220222-01', 'IM220223-01', 'IM220223-02', 'IM220224-01', 'IM220224-02', 'IM220225-01', 'IM220225-02', 'IM220226-01', 'IM220226-02', 'IM220227-01', 'IM220227-02', 'IM220227-01', 'IM220228-01', 'IM220228-02', 'IM220304-01', 'IM220308-01', 'IM220309-01', 'IM220311-01', 'IM220312-01')
			) tbl2 using(request_id); 
		
		-- metrics
		insert into data_vajapora.inactive_mau_analysis
		select 
			var_date report_date, 
			count(tbl1.mobile_no) daus, 
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) activated_maus, 
			count(case when tbl2.mobile_no is not null and tbl3.mobile_no is null then tbl1.mobile_no else null end) activated_maus_organic, 
			count(case when tbl2.mobile_no is not null and tbl3.mobile_no is not null and campaign_id is not null then tbl1.mobile_no else null end) activated_maus_target_campaign, 
			count(case when tbl2.mobile_no is not null and tbl3.mobile_no is not null and campaign_id is null then tbl1.mobile_no else null end) activated_maus_other_campaign
		from 
			data_vajapora.help_a tbl1 
			left join 
			data_vajapora.help_b tbl2 using(mobile_no)
			left join 
			data_vajapora.help_c tbl3 using(mobile_no);
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.inactive_mau_analysis
order by 1; 









