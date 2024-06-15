/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=448566064
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Request for Analysis Report!
- Notes (if any): 
	- full DEC te: unique open by notification (first and overall), kotojon notun PU te dhuklo, PU contribution, winback, dhore rakha
	- personalized e: category-wise dec er ta ano
	- impulse: 
		- impulse dekhe kara first ashlo (daily)
		- MAU te
*/

do $$ 

declare 
	var_date date:='2021-12-01'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.messaging_contribution_to_pu
		where report_date=var_date; 
	
		-- sequenced events of merchants, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			case 
				when notification_id in(select "Testimonials Msg IDs" from data_vajapora.message_categories where "Testimonials Msg IDs" is not null) then 'testimonial'
				when notification_id in(select "Add Hoc Msg IDs" from data_vajapora.message_categories where "Add Hoc Msg IDs" is not null) then 'adhoc'
				when notification_id in(select "Impulse Msg IDs" from data_vajapora.message_categories where "Impulse Msg IDs" is not null) then 'impulse'
				when notification_id in(select "Personalized Msg IDs" from data_vajapora.message_categories where "Personalized Msg IDs" is not null) then 'personalized'
				when notification_id in(select "Regular Msg IDs" from data_vajapora.message_categories where "Regular Msg IDs" is not null) then 'cjm'
				else 'others'
			end msg_category
		from 
			(select id, mobile_no, event_timestamp, event_name, notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('inbox_message_open', 'app_opened'); 
		
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl1.msg_category, tbl3.id
		from 
			data_vajapora.help_a tbl1
			inner join 
			data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from data_vajapora.help_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened';
		
		insert into data_vajapora.messaging_contribution_to_pu
		
		(-- DAUs via messaging
		select var_date report_date, msg_category, mobile_no 
		from data_vajapora.help_b
		where id is not null
		) 
		
		union all
		
		(-- all DAUs
		select var_date report_date, 'dau' msg_category, mobile_no 
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union 
		
		select var_date report_date, 'dau' msg_category, mobile_no 
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received','inbox_message_received')
			
		union 
			
		select var_date report_date, 'dau' msg_category, ss.mobile_number mobile_no
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date
		); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

-- PUs with months
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, string_agg(distinct left(report_date::text, 7), ', ') pu_months, count(distinct left(report_date::text, 7)) pu_months_count
from tallykhata.tk_power_users_10 
group by 1; 

-- necessary statistics
select 
	left(report_date::text, 7) "month",
	count(distinct mobile_no) mau,
	
	count(distinct case when msg_category not in('dau') then mobile_no else null end) first_open_through_inbox_mau, 
	count(distinct case when msg_category not in('dau') and mobile_no in(select mobile_no from tallykhata.tk_power_users_10 where report_date=current_date-1) then mobile_no else null end) first_open_through_inbox_mau_current_pu, 
	count(distinct case when msg_category not in('dau') and mobile_no not in(select mobile_no from tallykhata.tk_power_users_10 where report_date=current_date-1) then mobile_no else null end) first_open_through_inbox_mau_current_non_pu, 
	
	count(distinct case when msg_category='cjm' then mobile_no else null end) first_open_through_inbox_mau_cjm, 
	count(distinct case when msg_category='personalized' then mobile_no else null end) first_open_through_inbox_mau_personalized, 
	count(distinct case when msg_category='impulse' then mobile_no else null end) first_open_through_inbox_mau_impulse, 
	count(distinct case when msg_category='testimonial' then mobile_no else null end) first_open_through_inbox_mau_testimonial, 
	count(distinct case when msg_category='others' then mobile_no else null end) first_open_through_inbox_mau_others, 
	
	count(distinct case when 
		msg_category not in('dau') 
		and pu_months like concat('%', left(report_date::text, 7), '%') 
	then mobile_no else null end) first_open_through_inbox_pus,
	
	count(distinct case when 
		msg_category not in('dau') 
		and pu_months like concat('%', left(report_date::text, 7), '%') 
		and pu_months_count=1 
	then mobile_no else null end) first_open_through_inbox_first_time_pus,
	
	count(distinct case when 
		msg_category not in('dau') 
		and pu_months like concat('%', left(report_date::text, 7), '%') 
		and pu_months_count>1 
		and pu_months like concat('%', left((report_date-interval '1 month')::text, 7), '%') 
	then mobile_no else null end) first_open_through_inbox_continued_pus,
	
	count(distinct case when 
		msg_category not in('dau') 
		and pu_months like concat('%', left(report_date::text, 7), '%') 
		and pu_months_count>1 
		and pu_months not like concat('%', left((report_date-interval '1 month')::text, 7), '%') 
	then mobile_no else null end) first_open_through_inbox_wonback_pus
from 
	data_vajapora.messaging_contribution_to_pu tbl1 
	left join 
	data_vajapora.help_a tbl2 using(mobile_no)
group by 1; 

/*
select *, dau-dashboard_dau diff
from 
	(select report_date, count(distinct mobile_no) dau
	from data_vajapora.messaging_contribution_to_pu 
	where msg_category='dau'
	group by 1
	) tbl1 
	
	inner join
		
	(-- dashboard DAU
	select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event dashboard_dau
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event_date','Non Verified')
		group by d.report_date
		) as tbl_1 
	) tbl2 using(report_date)
order by 1 desc; 
*/
