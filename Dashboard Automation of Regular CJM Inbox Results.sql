/*
- Viz: https://datastudio.google.com/u/0/reporting/72e9308f-7d7e-45e6-931b-8b482fb8aeab/page/p_kptav01cxc
- Data: 
- Function: tallykhata.fn_mom_cjm_performance_detailed(), tallykhata.fn_cjm_daily_performance()
- Table: tallykhata.mom_cjm_performance_detailed, tallykhata.cjm_daily_performance
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- open through inbox
CREATE OR REPLACE FUNCTION tallykhata.fn_mom_cjm_performance_detailed()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	var_date date:=(select max(report_date)-7 from tallykhata.mom_cjm_performance_detailed); 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from tallykhata.mom_cjm_performance_detailed
		where report_date=var_date;

		-- sequenced events of DAUs, filtered for necessary events
		create table tallykhata.mom_cjm_performance_detailed_temp_a as
		select *
		from 
			(select id, mobile_no, event_name, notification_id, bulk_notification_id, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
	
		-- all push-open cases with message ids, with first opens/txns of the day identified
		insert into tallykhata.mom_cjm_performance_detailed
		select var_date report_date, tbl1.mobile_no, tbl1.notification_id, tbl3.id, tbl1.bulk_notification_id, tbl4.if_transacted
		from 
			tallykhata.mom_cjm_performance_detailed_temp_a tbl1
			inner join 
			tallykhata.mom_cjm_performance_detailed_temp_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from tallykhata.mom_cjm_performance_detailed_temp_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
			left join 
			(select mobile_no, 1 if_transacted
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date
			) tbl4 on(tbl2.mobile_no=tbl4.mobile_no)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		
		drop table if exists tallykhata.mom_cjm_performance_detailed_temp_a;
		
		raise notice 'Data generated for: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
END;
$function$
;

select tallykhata.fn_mom_cjm_performance_detailed(); 

select * 
from tallykhata.mom_cjm_performance_detailed; 

-- main table
CREATE OR REPLACE FUNCTION tallykhata.fn_cjm_daily_performance()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare 
	var_date date:=current_date-7; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from tallykhata.cjm_daily_performance 
		where report_date=var_date; 

		insert into tallykhata.cjm_daily_performance 
		select 
			var_date report_date, 
			dau, 
			intended_inbox_tg, 
			inbox_send_events_found_successful,
			inbox_send_events_found_successful*1.00/intended_inbox_tg inbox_send_events_found_successful_pct, 
			inbox_opened_merchants,  
			open_through_inbox_merchants,   
			first_open_through_inbox_merchants,   
			first_open_through_inbox_merchants_txn,   
			first_open_through_inbox_merchants_nontxn
		from 
			(select tbl_1.total_active_user_db_event dau
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
			where tbl_1.report_date=var_date
			) tbl1, 
			
			(select count(mobile_no) intended_inbox_tg
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl2, 
			
			(select count(distinct mobile_no) inbox_send_events_found_successful
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date
				and event_name='inbox_message_received'
			) tbl3, 
			
			(select count(distinct mobile_no) inbox_opened_merchants
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_date=var_date
				and event_name in('inbox_message_open')
			) tbl4, 
			
			(select
				count(distinct mobile_no) open_through_inbox_merchants, 
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants, 
				count(distinct case when id is not null and if_transacted=1 then mobile_no else null end) first_open_through_inbox_merchants_txn, 
				count(distinct case when id is not null and if_transacted is null then mobile_no else null end) first_open_through_inbox_merchants_nontxn
			from tallykhata.mom_cjm_performance_detailed
			where report_date=var_date
			) tbl5; 
			
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
END;
$function$
;

select tallykhata.fn_cjm_daily_performance(); 

select *
from tallykhata.cjm_daily_performance
order by 1; 