/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1096394270
- Data: 
- Function: 
- Table:
- Instructions: https://docs.google.com/spreadsheets/d/140lLAWQoqbAfqvrrxnHPyHdyo_A9Dciyvvh0-x-SRg8/edit#gid=0 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

do $$ 

declare 
	var_date date:='2021-12-01'::date; 
begin 
	raise notice 'New OP goes below: '; 

	-- message types
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select "Testimonials Msg IDs" notification_id, 'testimonial' msg_type from data_vajapora.message_categories where "Testimonials Msg IDs" is not null
	union all
	select "Ad Hoc Msg IDs" notification_id, 'ad hoc' msg_type from data_vajapora.message_categories where "Ad Hoc Msg IDs" is not null
	union all
	select "Impulse Msg IDs" notification_id, 'impulse' msg_type from data_vajapora.message_categories where "Impulse Msg IDs" is not null
	union all
	select "Personalized Msg IDs" notification_id, 'personalized' msg_type from data_vajapora.message_categories where "Personalized Msg IDs" is not null
	union all
	select "Regular Msg IDs" notification_id, 'regular' msg_type from data_vajapora.message_categories where "Regular Msg IDs" is not null; 

	loop
		delete from data_vajapora.opened_inbox_and_transacted 
		where txn_date=var_date; 
		delete from data_vajapora.opened_inbox_and_nontransacted 
		where txn_date=var_date; 

		-- merchants opened inbox message
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select mobile_no, notification_id 
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name='inbox_message_open'; 
			
		-- merchants transacted
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select distinct mobile_no 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		where created_datetime=var_date; 
			
		-- merchants opened inbox message and transacted
		insert into data_vajapora.opened_inbox_and_transacted
		select
			var_date txn_date, 
			mobile_no, 
			count(distinct case when msg_type='personalized' then notification_id else null end) visited_personalized_message, 
			count(distinct case when msg_type in('impulse', 'ad hoc') then notification_id else null end) visited_adhoc_message, 
			count(distinct case when msg_type in('testimonial', 'regular') or msg_type is null then (case when notification_id is null then -1 else notification_id end) else null end) visited_regular_message    
		from 
			data_vajapora.help_b tbl1 
			left join 
			data_vajapora.help_c tbl2 using(mobile_no)
			left join 
			data_vajapora.help_a tbl3 using(notification_id)
		group by 1, 2;
	
		-- merchants opened inbox message but did not transact
		insert into data_vajapora.opened_inbox_and_nontransacted
		select
			var_date txn_date, 
			mobile_no, 
			count(distinct case when msg_type='personalized' then notification_id else null end) visited_personalized_message, 
			count(distinct case when msg_type in('impulse', 'ad hoc') then notification_id else null end) visited_adhoc_message, 
			count(distinct case when msg_type in('testimonial', 'regular') or msg_type is null then (case when notification_id is null then -1 else notification_id end) else null end) visited_regular_message    
		from 
			data_vajapora.help_b tbl1 
			left join 
			data_vajapora.help_c tbl2 using(mobile_no)
			left join 
			data_vajapora.help_a tbl3 using(notification_id)
		where tbl2.mobile_no is null
		group by 1, 2; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-12-01'::date+5 then exit; 
		end if; 
	end loop; 
end $$; 

-- comparative 
select *
from 
	(select txn_date, count(mobile_no) merchants_opened_inbox_and_transacted
	from data_vajapora.opened_inbox_and_transacted
	group by 1
	) tbl1 
	
	left join 
	
	(select txn_date, count(mobile_no) merchants_opened_inbox_and_nontransacted
	from data_vajapora.opened_inbox_and_nontransacted
	group by 1
	) tbl2 using(txn_date)
order by 1; 

select *
from 
	/* merchants opened inbox message and transacted */
	(-- overall
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_message_and_transacted_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_message_and_transacted_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_message_and_transacted_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_transacted 
		group by 1, 2
		) tbl1 
	group by 1
	) tbl0 
	
	left join 

	(-- personalized
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_personalized_and_transacted_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_personalized_and_transacted_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_personalized_and_transacted_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_transacted 
		where visited_personalized_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl1 using(txn_days)
	
	left join 
	
	(-- adhoc
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_adhoc_and_transacted_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_adhoc_and_transacted_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_adhoc_and_transacted_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_transacted 
		where visited_adhoc_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl2 using(txn_days)
	
	left join 
	
	(-- regular
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_regular_and_transacted_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_regular_and_transacted_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_regular_and_transacted_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_transacted 
		where visited_regular_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl3 using(txn_days)
	
	left join
	
	/* merchants opened inbox message but did not transact */
	(-- overall
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_message_and_did_not_transact_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_message_and_did_not_transact_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_message_and_did_not_transact_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_nontransacted 
		group by 1, 2
		) tbl1 
	group by 1
	) tbl4 using(txn_days)
	
	left join 

	(-- personalized
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_personalized_and_did_not_transact_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_personalized_and_did_not_transact_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_personalized_and_did_not_transact_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_nontransacted 
		where visited_personalized_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl5 using(txn_days)
	
	left join 
	
	(-- adhoc
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_adhoc_and_did_not_transact_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_adhoc_and_did_not_transact_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_adhoc_and_did_not_transact_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_nontransacted 
		where visited_adhoc_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl6 using(txn_days)
	
	left join 
	
	(-- regular
	select 
		txn_days, 
		count(case when txn_year_month='2021-12' then mobile_no else null end) merchants_saw_regular_and_did_not_transact_in_dec, 
		count(case when txn_year_month='2022-01' then mobile_no else null end) merchants_saw_regular_and_did_not_transact_in_jan,
		count(case when txn_year_month='2022-02' then mobile_no else null end) merchants_saw_regular_and_did_not_transact_in_feb
	from 
		(select left(txn_date::text, 7) txn_year_month, mobile_no, count(txn_date) txn_days 
		from data_vajapora.opened_inbox_and_nontransacted 
		where visited_regular_message!=0
		group by 1, 2
		) tbl1 
	group by 1
	) tbl7 using(txn_days)
order by 1; 
