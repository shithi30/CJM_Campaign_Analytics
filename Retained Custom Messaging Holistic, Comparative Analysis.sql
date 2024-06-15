/*
- Viz: https://docs.google.com/spreadsheets/d/1SWUuc0jI6_34f62X5tgB5L8vpA9J762KU1mxzlL_CtQ/edit#gid=172949561
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Template: https://docs.google.com/spreadsheets/d/1SWUuc0jI6_34f62X5tgB5L8vpA9J762KU1mxzlL_CtQ/edit#gid=0
- Email thread: 
- Notes (if any): 
*/


-- noncampaign period: '2021-05-23' to '2021-06-08'
-- noncampaign period: '2021-06-23' to '2021-07-08'
-- noncampaign period: '2021-07-23' to '2021-08-08'
-- campaign period: '2021-08-23' to '2021-09-08'


-- with events: green tables
do $$

declare
	var_date date:='2021-08-23'::date; 

begin
	
	raise notice 'New OP goes below:';

	loop 
		delete from data_vajapora.retained_campaign_dau_inv 
		where dau_date=var_date; 
		
		insert into data_vajapora.retained_campaign_dau_inv
		select
			var_date dau_date,
			count(tbl1.mobile_no) dau,
			count(case when max_last_event_date is null and reg_date=event_date then tbl1.mobile_no else null end) first_dau_on_reg_date,
			count(case when max_last_event_date is null and reg_date!=event_date then tbl1.mobile_no else null end) first_dau_on_other_date,
			count(case when max_last_event_date is null and reg_date is null then tbl1.mobile_no else null end) first_dau_unverified,
			count(case when var_date-max_last_event_date=1 then tbl1.mobile_no else null end) cont_dau,
			count(case when var_date-max_last_event_date>=2 and var_date-max_last_event_date<=7 then tbl1.mobile_no else null end) activated_2_to_7_days_later,
			count(case when var_date-max_last_event_date>=8 and var_date-max_last_event_date<=15 then tbl1.mobile_no else null end) activated_8_to_15_days_later,
			count(case when var_date-max_last_event_date>=16 and var_date-max_last_event_date<=30 then tbl1.mobile_no else null end) activated_16_to_30_days_later,      
			count(case when var_date-max_last_event_date>30 then tbl1.mobile_no else null end) activated_more_than_30_days_later
		from 
			(select mobile_no, event_date
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile_no, max(event_date) max_last_event_date
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date<var_date
			group by 1
			) tbl4 on(tbl1.mobile_no=tbl4.mobile_no); 
		
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date='2021-09-08'::date then exit;
		end if; 
	
	end loop; 

end $$; 

select *
from data_vajapora.retained_campaign_dau_inv
order by 1; 


-- without events: blue tables
do $$

declare
	var_date date:='2021-08-23'::date; 

begin
	
	raise notice 'New OP goes below:';

	loop 
		delete from data_vajapora.retained_campaign_dau_inv_txn
		where dau_date=var_date; 
		
		insert into data_vajapora.retained_campaign_dau_inv_txn
		select
			var_date dau_date,
			count(tbl1.mobile_no) dau,
			count(case when max_last_event_date is null and reg_date=event_date then tbl1.mobile_no else null end) first_dau_on_reg_date,
			count(case when max_last_event_date is null and reg_date!=event_date then tbl1.mobile_no else null end) first_dau_on_other_date,
			count(case when max_last_event_date is null and reg_date is null then tbl1.mobile_no else null end) first_dau_unverified,
			count(case when var_date-max_last_event_date=1 then tbl1.mobile_no else null end) cont_dau,
			count(case when var_date-max_last_event_date>=2 and var_date-max_last_event_date<=7 then tbl1.mobile_no else null end) activated_2_to_7_days_later,
			count(case when var_date-max_last_event_date>=8 and var_date-max_last_event_date<=15 then tbl1.mobile_no else null end) activated_8_to_15_days_later,
			count(case when var_date-max_last_event_date>=16 and var_date-max_last_event_date<=30 then tbl1.mobile_no else null end) activated_16_to_30_days_later,      
			count(case when var_date-max_last_event_date>30 then tbl1.mobile_no else null end) activated_more_than_30_days_later
		from 
			(select mobile_no, created_datetime event_date
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime=var_date
			) tbl1 
			
			left join 
			
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile 
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 
			
			(select mobile_no, max(created_datetime) max_last_event_date
			from tallykhata.tallykhata_transacting_user_date_sequence_final 
			where created_datetime<var_date
			group by 1
			) tbl4 on(tbl1.mobile_no=tbl4.mobile_no); 
		
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date='2021-09-08'::date then exit;
		end if; 
	
	end loop; 

end $$; 

select *
from data_vajapora.retained_campaign_dau_inv_txn
order by 1; 


-- table-01: churn after winback
do $$

declare
	var_start_date date:='2021-08-23'::date;
begin 

	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select tbl1.mobile_no, tbl2.event_date-tbl1.event_date winback_after_days
	from 
		(select mobile_no, event_date, date_sequence 
		from tallykhata.tallykhata_user_date_sequence_final 
		) tbl1 
		
		inner join 
		
		(select mobile_no, min(event_date) event_date, min(date_sequence) date_sequence
		from tallykhata.tallykhata_user_date_sequence_final 
		where event_date>=var_start_date+0 and event_date<=var_start_date+7 -- first half: 8 days
		group by 1  
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	where tbl2.event_date-tbl1.event_date>=30; 

	insert into data_vajapora.retained_campaign_churn_after_winback
	select 
		var_start_date+0 first_half_start_date,
		var_start_date+7 first_half_end_date,
		var_start_date+8 second_half_start_date,
		var_start_date+15 second_half_end_date,
		*
	from 
		(select count(distinct mobile_no) winback_first_half	
		from data_vajapora.help_a
		) tbl1,
		
		(select count(distinct mobile_no) churned_after_winback_second_half
		from 
			data_vajapora.help_a tbl1
			left join 
			(select distinct mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>=var_start_date+8 and event_date<=var_start_date+15 -- second half: 8 days
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null
		) tbl2; 

end $$; 
	
select *
from data_vajapora.retained_campaign_churn_after_winback; 


-- table-02: overall metrics
do $$

declare
	var_start_date date:='2021-07-23'::date;
begin 

	drop table if exists data_vajapora.help_a;
	create table data_vajapora.help_a as
	select tbl1.mobile_no, tbl2.event_date-tbl1.event_date winback_after_days
	from 
		(select mobile_no, event_date, date_sequence 
		from tallykhata.tallykhata_user_date_sequence_final 
		) tbl1 
		
		inner join 
		
		(select mobile_no, min(event_date) event_date, min(date_sequence) date_sequence
		from tallykhata.tallykhata_user_date_sequence_final 
		where event_date>=var_start_date and event_date<=var_start_date+14
		group by 1  
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.date_sequence=tbl2.date_sequence-1)
	where tbl2.event_date-tbl1.event_date>=30; 

	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select count(distinct tbl1.mobile_no) reg_to_first_dau
	from 
		(select created_datetime, mobile_no
		from tallykhata.tallykhata_transacting_user_date_sequence_final
		where created_datetime>=var_start_date and created_datetime<=var_start_date+14
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, date(created_at) reg_date
		from public.register_usermobile 
		where date(created_at)>=var_start_date and date(created_at)<=var_start_date+14
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.created_datetime=tbl2.reg_date);
		
	drop table if exists data_vajapora.help_c;
	create table data_vajapora.help_c as
	select count(distinct tbl1.mobile_no) reg_to_new_user
	from 
		(select mobile_no
		from tallykhata.tallykhata_transacting_user_date_sequence_final
		where 
			date_sequence=1
			and created_datetime>=var_start_date and created_datetime<=var_start_date+14
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no
		from public.register_usermobile 
		where date(created_at)<var_start_date
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no);
	
	drop table if exists data_vajapora.help_d;
	create table data_vajapora.help_d as
	select count(distinct tbl1.mobile_no) reg_to_3rau
	from 
		(select mobile_no
		from tallykhata.regular_active_user_event
		where 
			report_date::date>=var_start_date and report_date::date<=var_start_date+14
			and rau_category=3
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, date(created_at) reg_date
		from public.register_usermobile 
		where date(created_at)>=var_start_date and date(created_at)<=var_start_date+14
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no);
	
	-- bringing all metrics together
	insert into data_vajapora.retained_campaign_overall_metrics
	select 
		var_start_date start_date,
		var_start_date+14 end_date,
		*
	from 
		(select count(distinct mobile_no) inactive_to_active
		from data_vajapora.help_a
		) tbl1,
	
		(select reg_to_first_dau
		from data_vajapora.help_b
		) tbl2,
	
		(select reg_to_new_user
		from data_vajapora.help_c
		) tbl3,
	
		(select reg_to_3rau
		from data_vajapora.help_d
		) tbl4;

end $$; 

select *
from data_vajapora.retained_campaign_overall_metrics
order by 1; 

-- for additional column
select 
	count(mobile_number) users_registered, 
	-- to match dates
	min(date(created_at)), 
	max(date(created_at))
from public.register_usermobile 
where date(created_at)>='2021-08-23'::date and date(created_at)<='2021-08-23'::date+14; -- plug in right ranges

