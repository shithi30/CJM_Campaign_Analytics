/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=532956315
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	How much contribution we get in DAU and MAU from Messaging overall and from different category for messages? 
	For example, how many merchant open the app due to personalized messages.
*/

do $$

declare 
	var_date date:='2021-12-01'::date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.personalized_msg_impact_1
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
			where created_date=var_date
			) tbl1 
		where event_name in('inbox_message_open', 'app_opened'); 
		
		-- all push-open cases, with first opens of the day and transaction-day identified 
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl1.msg_category, tbl3.id, tbl4.if_transacted
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
			left join 
			(select mobile_no, 1 if_transacted
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date
			) tbl4 on(tbl2.mobile_no=tbl4.mobile_no)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened';
		
		-- necessary statistics
		insert into data_vajapora.personalized_msg_impact_1
		select
			var_date report_date,	
			-- testimonial
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='testimonial') testimonial_inbox_opened_merchants,
			count(distinct case when msg_category='testimonial' then mobile_no else null end) open_through_testimonial_inbox_merchants, 
			count(distinct case when id is not null and msg_category='testimonial' then mobile_no else null end) first_open_through_testimonial_inbox_merchants, 
			count(distinct case when id is not null and msg_category='testimonial' and if_transacted=1 then mobile_no else null end) first_open_through_testimonial_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='testimonial' and if_transacted is null then mobile_no else null end) first_open_through_testimonial_inbox_merchants_nontxn, 	
			-- adhoc
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='adhoc') adhoc_inbox_opened_merchants,
			count(distinct case when msg_category='adhoc' then mobile_no else null end) open_through_adhoc_inbox_merchants, 
			count(distinct case when id is not null and msg_category='adhoc' then mobile_no else null end) first_open_through_adhoc_inbox_merchants, 
			count(distinct case when id is not null and msg_category='adhoc' and if_transacted=1 then mobile_no else null end) first_open_through_adhoc_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='adhoc' and if_transacted is null then mobile_no else null end) first_open_through_adhoc_inbox_merchants_nontxn, 	
			-- impulse
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='impulse') impulse_inbox_opened_merchants,
			count(distinct case when msg_category='impulse' then mobile_no else null end) open_through_impulse_inbox_merchants, 
			count(distinct case when id is not null and msg_category='impulse' then mobile_no else null end) first_open_through_impulse_inbox_merchants, 
			count(distinct case when id is not null and msg_category='impulse' and if_transacted=1 then mobile_no else null end) first_open_through_impulse_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='impulse' and if_transacted is null then mobile_no else null end) first_open_through_impulse_inbox_merchants_nontxn, 
			-- personalized
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='personalized') personalized_inbox_opened_merchants,
			count(distinct case when msg_category='personalized' then mobile_no else null end) open_through_personalized_inbox_merchants, 
			count(distinct case when id is not null and msg_category='personalized' then mobile_no else null end) first_open_through_personalized_inbox_merchants, 
			count(distinct case when id is not null and msg_category='personalized' and if_transacted=1 then mobile_no else null end) first_open_through_personalized_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='personalized' and if_transacted is null then mobile_no else null end) first_open_through_personalized_inbox_merchants_nontxn, 
			-- cjm
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='cjm') cjm_inbox_opened_merchants,
			count(distinct case when msg_category='cjm' then mobile_no else null end) open_through_cjm_inbox_merchants, 
			count(distinct case when id is not null and msg_category='cjm' then mobile_no else null end) first_open_through_cjm_inbox_merchants, 
			count(distinct case when id is not null and msg_category='cjm' and if_transacted=1 then mobile_no else null end) first_open_through_cjm_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='cjm' and if_transacted is null then mobile_no else null end) first_open_through_cjm_inbox_merchants_nontxn, 
			-- others
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open' and msg_category='others') others_inbox_opened_merchants,
			count(distinct case when msg_category='others' then mobile_no else null end) open_through_others_inbox_merchants, 
			count(distinct case when id is not null and msg_category='others' then mobile_no else null end) first_open_through_others_inbox_merchants, 
			count(distinct case when id is not null and msg_category='others' and if_transacted=1 then mobile_no else null end) first_open_through_others_inbox_merchants_txn, 
			count(distinct case when id is not null and msg_category='others' and if_transacted is null then mobile_no else null end) first_open_through_others_inbox_merchants_nontxn, 
			-- all
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open') inbox_opened_merchants,
			count(distinct mobile_no) open_through_inbox_merchants, 
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants, 
			count(distinct case when id is not null and if_transacted=1 then mobile_no else null end) first_open_through_inbox_merchants_txn, 
			count(distinct case when id is not null and if_transacted is null then mobile_no else null end) first_open_through_inbox_merchants_nontxn
		from data_vajapora.help_b; 
		commit; 
	
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	dau, 
	-- all
	inbox_opened_merchants,  
	open_through_inbox_merchants,   
	first_open_through_inbox_merchants,   
	first_open_through_inbox_merchants_txn,   
	first_open_through_inbox_merchants_nontxn, 
	-- testimonial
	testimonial_inbox_opened_merchants,
	open_through_testimonial_inbox_merchants, 
	first_open_through_testimonial_inbox_merchants, 
	first_open_through_testimonial_inbox_merchants_txn, 
	first_open_through_testimonial_inbox_merchants_nontxn, 	
	-- adhoc
	adhoc_inbox_opened_merchants,  
	open_through_adhoc_inbox_merchants,   
	first_open_through_adhoc_inbox_merchants,   
	first_open_through_adhoc_inbox_merchants_txn,   
	first_open_through_adhoc_inbox_merchants_nontxn,   	
	-- impulse
	impulse_inbox_opened_merchants,  
	open_through_impulse_inbox_merchants,   
	first_open_through_impulse_inbox_merchants,   
	first_open_through_impulse_inbox_merchants_txn,   
	first_open_through_impulse_inbox_merchants_nontxn,   
	-- personalized
	personalized_inbox_opened_merchants,  
	open_through_personalized_inbox_merchants,   
	first_open_through_personalized_inbox_merchants,   
	first_open_through_personalized_inbox_merchants_txn,   
	first_open_through_personalized_inbox_merchants_nontxn,   
	-- cjm
	cjm_inbox_opened_merchants,  
	open_through_cjm_inbox_merchants,   
	first_open_through_cjm_inbox_merchants,   
	first_open_through_cjm_inbox_merchants_txn,   
	first_open_through_cjm_inbox_merchants_nontxn, 
	-- others
	others_inbox_opened_merchants,  
	open_through_others_inbox_merchants,   
	first_open_through_others_inbox_merchants,   
	first_open_through_others_inbox_merchants_txn,
	first_open_through_others_inbox_merchants_nontxn
from 
	data_vajapora.personalized_msg_impact_1 tbl1
	
	inner join
		
	(-- dashboard DAU
	select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event dau
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event','Non Verified')
		group by d.report_date
		) as tbl_1 
	) tbl2 using(report_date)
order by 1; 

/*
-- sanity check
select 
	report_date, 
	
	first_open_through_inbox_merchants,   
	
	 first_open_through_testimonial_inbox_merchants
	+first_open_through_adhoc_inbox_merchants
	+first_open_through_impulse_inbox_merchants    
	+first_open_through_personalized_inbox_merchants    
	+first_open_through_cjm_inbox_merchants
	+first_open_through_others_inbox_merchants
	
	first_open_through_inbox_merchants_summed
from data_vajapora.personalized_msg_impact_1;
*/

