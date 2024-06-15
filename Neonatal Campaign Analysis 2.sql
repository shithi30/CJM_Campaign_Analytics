/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1899460863
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

-- time spent
do $$ 

declare 
	var_date date:=(select max(report_date)-7 from data_vajapora.reg_day_time_spent); 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.reg_day_time_spent 
		where report_date=var_date; 
	
		-- sequenced terminal events on reg date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name in('app_in_background', 'app_opened', 'app_launched', 'app_closed')
				and event_date=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl2 using(mobile_no); 
		
		-- avg. mins spent on reg date
		insert into data_vajapora.reg_day_time_spent
		select 
			var_date report_date,
			
			count(tbl1.mobile_no) reg_merchants_recorded_time, 
			sum(min_spent) reg_merchant_total_min_spent, 
			avg(min_spent) reg_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) reg_txn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_total_min_spent,
			avg(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) reg_nontxn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_total_min_spent, 
			avg(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_avg_min_spent
		from 
			(select mobile_no, sum(sec_spent)/60.00 min_spent
			from 
				(select 
					tbl1.mobile_no, 
					date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
					+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
					+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					sec_spent
				from 
					data_vajapora.help_a tbl1
					inner join 
					data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				where 
					tbl1.event_name in('app_opened', 'app_launched')
					and tbl2.event_name in('app_in_background', 'app_closed')
				) tbl1 
			group by 1
			) tbl1 
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl2 using(mobile_no); 

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

-- all metrics
do $$ 

declare 
	var_date date:=(select max(report_date)-5 from data_vajapora.neonatal_analysis_2); 
begin 
	raise notice 'New OP goes below:'; 

	-- neonatal campaigns
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		schedule_date, 
		case when message_id in(3134, 3137, 3139, 3140, 3143) then 'inbox' else 'inapp' end campaign_type, 
		campaign_id, bulk_notification_id, message_id, 
		message, 
		intended_receiver_count, total_success firebase_success 
	from data_vajapora.all_sch_stats
	where 
		campaign_id like 'NN%'
		and campaign_id not in('NN220606-30-119', 'NN220606-30-113')
		and message_id in
			(-- inbox
			3134,
			3137,
			3139,
			3140,
			3143,
			-- inapp
			3135,
			3138,
			3142)
		and schedule_date>'2022-06-05' and schedule_date<current_date; 

	loop 
		delete from data_vajapora.neonatal_analysis_2
		where report_date=var_date; 
	
		-- necessary metrics
		insert into data_vajapora.neonatal_analysis_2
		select
			var_date report_date, 
			count(tbl1.mobile_no) merchants_reg, 
			count(case when tbl0.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_trt_tacs,
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_trt,
			count(case when tbl3.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_tacs, 
			(select reg_merchant_avg_min_spent from data_vajapora.reg_day_time_spent where report_date=var_date) merchants_reg_avg_time_spent, 
			count(case when tbl4.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_rec_message, 
			count(case when tbl5.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_opened_message,
			count(case when tbl6.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_opened_app_via_notification,
			count(case when tbl7.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_first_opened_app_via_notification, 
			count(case when tbl8.mobile_no is not null then tbl1.mobile_no else null end) merchants_reg_tapped_link, 
			count(case when  tbl9.mobile_no is not null then tbl1.mobile_no else null end) retention_day_01, 
			count(case when tbl10.mobile_no is not null then tbl1.mobile_no else null end) retention_day_02, 
			count(case when tbl11.mobile_no is not null then tbl1.mobile_no else null end) retention_day_03,
			count(case when  tbl9.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_day_01, 
			count(case when tbl10.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_day_02, 
			count(case when tbl11.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_day_03, 
			count(case when tbl81.mobile_no is not null and tbl12.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_txn_day_00, 
			count(case when tbl9.mobile_no  is not null and tbl13.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_txn_day_01, 
			count(case when tbl10.mobile_no is not null and tbl14.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_txn_day_02,
			count(case when tbl11.mobile_no is not null and tbl15.mobile_no is not null and tbl5.mobile_no is not null then tbl1.mobile_no else null end) seen_retention_txn_day_03, 
			count(case when tbl13.mobile_no is not null then tbl1.mobile_no else null end) retention_txn_day_01, 
			count(case when tbl14.mobile_no is not null then tbl1.mobile_no else null end) retention_txn_day_02, 
			count(case when tbl15.mobile_no is not null then tbl1.mobile_no else null end) retention_txn_day_03, 
			count(
				case 
					when (tbl0.mobile_no is not null or tbl13.mobile_no is not null or tbl14.mobile_no is not null or tbl15.mobile_no is not null) and var_date<'2022-06-06' then tbl1.mobile_no                        
					when 
						(
							   (tbl81.mobile_no is not null and tbl12.mobile_no is not null)
							or (tbl9.mobile_no  is not null and tbl13.mobile_no is not null) 
							or (tbl10.mobile_no is not null and tbl14.mobile_no is not null) 
							or (tbl11.mobile_no is not null and tbl15.mobile_no is not null)
						)
						and tbl5.mobile_no is not null 
						and var_date>='2022-06-06'
					then tbl1.mobile_no
				end
			) comparative_retention_txn_day_n
		from 
			(select distinct mobile_number mobile_no 
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl1 
			
			left join 
			
			(select distinct mobile_no 
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date 
			) tbl0 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no 
			from tallykhata.tallykhata_fact_info_final 
			where 
				created_datetime=var_date 
				and entry_type=1
			) tbl2 using(mobile_no) 
			
			left join 
			
			(select distinct mobile_no 
			from tallykhata.tallykhata_fact_info_final 
			where 
				created_datetime=var_date 
				and entry_type=2
			) tbl3 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name like '%_message_received'
				and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a)
			) tbl4 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date
				and event_name like '%_message_open'
				and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a)
			) tbl5 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no
			from data_vajapora.mom_cjm_performance_detailed
			where 
				report_date=var_date
				and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a)
			) tbl6 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no
			from data_vajapora.mom_cjm_performance_detailed
			where 
				report_date=var_date
				and id is not null
				and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a)
			) tbl7 using(mobile_no)
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date
				and event_name='in_app_message_link_tap'
				and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a)
			) tbl8 using(mobile_no)
			
			left join 
			(select mobile_no from tallykhata.tallykhata_user_date_sequence_final where event_date=var_date) tbl81 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_user_date_sequence_final where event_date=var_date+1) tbl9 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_user_date_sequence_final where event_date=var_date+2) tbl10 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_user_date_sequence_final where event_date=var_date+3) tbl11 using(mobile_no)
			
			left join 
			(select mobile_no from tallykhata.tallykhata_transacting_user_date_sequence_final where created_datetime=var_date) tbl12 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_transacting_user_date_sequence_final where created_datetime=var_date+1) tbl13 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_transacting_user_date_sequence_final where created_datetime=var_date+2) tbl14 using(mobile_no)
			left join 
			(select mobile_no from tallykhata.tallykhata_transacting_user_date_sequence_final where created_datetime=var_date+3) tbl15 using(mobile_no); 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

select report_date, merchants_reg, merchants_reg_trt_tacs, merchants_reg_trt, merchants_reg_tacs, merchants_reg_avg_time_spent, merchants_reg_rec_message, merchants_reg_opened_message, merchants_reg_opened_app_via_notification, merchants_reg_first_opened_app_via_notification, merchants_reg_tapped_link, retention_day_01, retention_day_02, retention_day_03, seen_retention_day_01, seen_retention_day_02, seen_retention_day_03, seen_retention_txn_day_00, seen_retention_txn_day_01, seen_retention_txn_day_02, seen_retention_txn_day_03, comparative_retention_txn_day_n, retention_txn_day_01, retention_txn_day_02, retention_txn_day_03                 
from data_vajapora.neonatal_analysis_2
order by 1;
