/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=33531831
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
	Ad hoc messages are showing promise in this regard. 
	Traction due to this type of messaging has been contributing 3k to 5k DAUs for the last 3 days, which used to be ~1.3k during earlier times. 
*/

-- open through inbox stats
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	report_date, 
	
	count(distinct mobile_no) open_through_notification, 
	count(distinct case when id is not null then mobile_no else null end) first_open_through_notification, 
	
	count(distinct case when campaign_type='personalized' then mobile_no else null end) open_through_notification_personalized, 
	count(distinct case when campaign_type='personalized' and id is not null then mobile_no else null end) first_open_through_notification_personalized,
	
	count(distinct case when campaign_type='ad hoc' then mobile_no else null end) open_through_notification_adhoc, 
	count(distinct case when campaign_type='ad hoc' and id is not null then mobile_no else null end) first_open_through_notification_adhoc,
	
	count(distinct case when campaign_type='regular' then mobile_no else null end) open_through_notification_regular, 
	count(distinct case when campaign_type='regular' and id is not null then mobile_no else null end) first_open_through_notification_regular
from 
	(select 
		*, 
		case 
			when (notification_id, bulk_notification_id) in(select message_id, bulk_notification_id from data_vajapora.all_sch_stats where campaign_id in('PM220801-30-01', 'PM220801-30-02', 'PM220801-30-02', 'PM220801-30-02', 'PM220801-30-03', 'PM220801-30-04', 'PM220801-30-04', 'PM220801-30-04', 'PM220801-30-05', 'PM220801-30-05', 'PM220801-30-06', 'PM220801-30-06', 'PM220801-30-07', 'PM220801-30-08', 'PM220801-30-08', 'PM220801-30-08', 'PM220801-30-09', 'PM220801-30-09', 'PM220801-30-09', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-11', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-13', 'PM220801-30-14', 'PM220801-30-15', 'PM220801-30-15', 'PM220801-30-15', 'PM220801-30-16', 'PM220801-30-17', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-19', 'PM220801-30-19', 'PM220801-30-19', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20')) then 'personalized' 
			when (notification_id, bulk_notification_id) in(select message_id, bulk_notification_id from data_vajapora.all_sch_stats where campaign_id in('MG220801-01', 'TM220802-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'RB220803-05', 'RB220803-06', 'RB220803-07', 'RB220803-08', 'FBG220804-01', 'UGC220804-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'IM220806-01', 'UGC220807-01', 'MR220729-01', 'IM220810-01', 'IM220810-01', 'IM220811-01', 'IM220812-01', 'IM220813-01', 'IM220814-01', 'IM220815-01', 'IM220816-01', 'IM220817-01', 'IM220818-01', 'IM220811-01', 'IM220814-01', 'IM220817-01', 'IM220819-01', 'IM220820-01', 'IM220821-01', 'IM220817-01s', 'IM220818-01', 'RM220818-01', 'FBG220818-01', 'IM220819-01', 'MR220821-01', 'IM220822-01', 'IM220722-01', 'IM220823-01', 'IM220824-01', 'IM220825-02', 'IM220826-03', 'IM220827-04', 'IM220828-05', 'IM220823-02', 'IM220824-01', 'TM220825-01', 'MR220825-01', 'IM220827-01', 'IM220828-01', 'MR220829-01', 'MR220829-02', 'IM220829-01', 'MR220830-01', 'IM220830-01', 'IM220830-02', 'MR220831-01', 'MR220831-02', 'IM220831-01', 'MG220901-01')) then 'ad hoc' 
			else 'regular'
		end campaign_type
	from tallykhata.mom_cjm_performance_detailed
	where report_date>=current_date-30 and report_date<current_date
	) tbl1 
group by 1; 

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.msg_type_stats 
		where report_date=var_date; 
	
		-- inbox open stats
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as 
		select 
			mobile_no, 
			case 
				when (notification_id, bulk_notification_id) in(select message_id, bulk_notification_id from data_vajapora.all_sch_stats where campaign_id in('PM220801-30-01', 'PM220801-30-02', 'PM220801-30-02', 'PM220801-30-02', 'PM220801-30-03', 'PM220801-30-04', 'PM220801-30-04', 'PM220801-30-04', 'PM220801-30-05', 'PM220801-30-05', 'PM220801-30-06', 'PM220801-30-06', 'PM220801-30-07', 'PM220801-30-08', 'PM220801-30-08', 'PM220801-30-08', 'PM220801-30-09', 'PM220801-30-09', 'PM220801-30-09', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-10', 'PM220801-30-11', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-12', 'PM220801-30-13', 'PM220801-30-14', 'PM220801-30-15', 'PM220801-30-15', 'PM220801-30-15', 'PM220801-30-16', 'PM220801-30-17', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-18', 'PM220801-30-19', 'PM220801-30-19', 'PM220801-30-19', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20', 'PM220801-30-20')) then 'personalized' 
				when (notification_id, bulk_notification_id) in(select message_id, bulk_notification_id from data_vajapora.all_sch_stats where campaign_id in('MG220801-01', 'TM220802-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'RB220803-05', 'RB220803-06', 'RB220803-07', 'RB220803-08', 'FBG220804-01', 'UGC220804-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'IM220806-01', 'UGC220807-01', 'MR220729-01', 'IM220810-01', 'IM220810-01', 'IM220811-01', 'IM220812-01', 'IM220813-01', 'IM220814-01', 'IM220815-01', 'IM220816-01', 'IM220817-01', 'IM220818-01', 'IM220811-01', 'IM220814-01', 'IM220817-01', 'IM220819-01', 'IM220820-01', 'IM220821-01', 'IM220817-01s', 'IM220818-01', 'RM220818-01', 'FBG220818-01', 'IM220819-01', 'MR220821-01', 'IM220822-01', 'IM220722-01', 'IM220823-01', 'IM220824-01', 'IM220825-02', 'IM220826-03', 'IM220827-04', 'IM220828-05', 'IM220823-02', 'IM220824-01', 'TM220825-01', 'MR220825-01', 'IM220827-01', 'IM220828-01', 'MR220829-01', 'MR220829-02', 'IM220829-01', 'MR220830-01', 'IM220830-01', 'IM220830-02', 'MR220831-01', 'MR220831-02', 'IM220831-01', 'MG220901-01')) then 'ad hoc' 
				else 'regular'
			end campaign_type
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name='inbox_message_open'; 
			
		insert into data_vajapora.msg_type_stats
		select 
			var_date report_date, 
			count(distinct mobile_no) view_message, 
			count(distinct case when campaign_type='personalized' then mobile_no else null end) view_message_personalized, 
			count(distinct case when campaign_type='ad hoc' then mobile_no else null end) view_message_adhoc, 
			count(distinct case when campaign_type='regular' then mobile_no else null end) view_message_regular
		from data_vajapora.help_b; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
	
end $$; 

-- all stats combined
select 
	report_date, 
	view_message, open_through_notification, first_open_through_notification,
	view_message_personalized, open_through_notification_personalized, first_open_through_notification_personalized, 
	view_message_adhoc, open_through_notification_adhoc, first_open_through_notification_adhoc,
	view_message_regular, open_through_notification_regular, first_open_through_notification_regular
from
	data_vajapora.msg_type_stats tbl1 
	inner join 
	data_vajapora.help_a tbl2 using(report_date)
order by 1; 

-- district-wise inv.
select 
	report_date, 
	case when district_name is null then 'others' else district_name end district_name, 
	count(distinct mobile_no) first_open_through_notification_adhoc
from 
	(select report_date, mobile_no
	from tallykhata.mom_cjm_performance_detailed
	where 
		report_date>=current_date-7 and report_date<current_date-0
		and (notification_id, bulk_notification_id) in(select message_id, bulk_notification_id from data_vajapora.all_sch_stats where campaign_id in('MG220801-01', 'TM220802-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'RB220803-05', 'RB220803-06', 'RB220803-07', 'RB220803-08', 'FBG220804-01', 'UGC220804-01', 'RB220803-01', 'RB220803-02', 'RB220803-03', 'RB220803-04', 'IM220806-01', 'UGC220807-01', 'MR220729-01', 'IM220810-01', 'IM220810-01', 'IM220811-01', 'IM220812-01', 'IM220813-01', 'IM220814-01', 'IM220815-01', 'IM220816-01', 'IM220817-01', 'IM220818-01', 'IM220811-01', 'IM220814-01', 'IM220817-01', 'IM220819-01', 'IM220820-01', 'IM220821-01', 'IM220817-01s', 'IM220818-01', 'RM220818-01', 'FBG220818-01', 'IM220819-01', 'MR220821-01', 'IM220822-01', 'IM220722-01', 'IM220823-01', 'IM220824-01', 'IM220825-02', 'IM220826-03', 'IM220827-04', 'IM220828-05', 'IM220823-02', 'IM220824-01', 'TM220825-01', 'MR220825-01', 'IM220827-01', 'IM220828-01', 'MR220829-01', 'MR220829-02', 'IM220829-01', 'MR220830-01', 'IM220830-01', 'IM220830-02', 'MR220831-01', 'MR220831-02', 'IM220831-01', 'MG220901-01')) 
		and id is not null
	) tbl1 
		
	left join 
	
	(select mobile mobile_no, max(district_name) district_name
	from tallykhata.tallykhata_clients_location_info
	group by 1
	) tbl2 using(mobile_no) 
group by 1, 2 
order by 1, 2; 
