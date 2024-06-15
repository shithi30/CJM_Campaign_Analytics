/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
	- play around with generated data: https://docs.google.com/document/d/1Hmg2zXO4PwiuQHPZoCJ3_2JRBgZVYlVad7I5nHHarFQ/edit
	- our response: https://docs.google.com/spreadsheets/d/1OUp5jYnkGKikO-VG5hdcEHPNYaKsK4QBwlrHTJRL8Pg/edit#gid=465737476
- Email thread: 
- Notes (if any): 
*/

-- merchants' daily TRT, TACS: generate this first to populate all tables
drop table if exists data_vajapora.daily_trt_tacs; 
create table data_vajapora.daily_trt_tacs as
select 
	mobile_no, 
	created_datetime,
	count(case when entry_type=1 then auto_id else null end) lft_trt,
	count(case when entry_type=2 then auto_id else null end) lft_tacs
from tallykhata.tallykhata_fact_info_final 
group by 1, 2; 

-- generate daily data: data_vajapora.usage_dist_age_greater_than_30_days
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	
	delete from data_vajapora.usage_dist_age_greater_than_30_days
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select var_date report_date, mobile_no, reg_date, var_date-reg_date+1 days_with_tk 
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile
			) tbl1 
			
			inner join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>var_date-30 and event_date<=var_date
			) tbl2 using(mobile_no)
		where var_date-reg_date+1>30;
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			-- calculation of scores
			total_active_days*1.00/days_with_tk pct_of_active_days_lft_score, 
			active_days_last_30_days/30.00 pct_of_active_days_last_30_days_score, 
			case when lft_trt!=0 then last_30_days_trt*1.00/lft_trt else 0 end pct_of_trt_last_30_days_score,
			case when lft_tacs!=0 then last_30_days_tacs*1.00/lft_tacs else 0 end pct_of_tacs_last_30_days_score
		from 	
			(select 
				-- general metrics
				tbl1.mobile_no, report_date, reg_date, days_with_tk, total_active_days, 
				case when lft_trt is null then 0 else lft_trt end lft_trt, 
				case when lft_tacs is null then 0 else lft_tacs end lft_tacs, 
				case when active_days_last_30_days is null then 0 else active_days_last_30_days end active_days_last_30_days, 
				case when last_30_days_trt is null then 0 else last_30_days_trt end last_30_days_trt, 
				case when last_30_days_tacs is null then 0 else last_30_days_tacs end last_30_days_tacs 
			from 
				data_vajapora.help_b tbl1 
				
				left join 
				
				(select mobile_no, max(date_sequence) total_active_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date<=var_date
				group by 1
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) lft_trt, sum(lft_tacs) lft_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime<=var_date
				group by 1
				) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
				
				left join 
				
				(select mobile_no, count(date_sequence) active_days_last_30_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>var_date-30 and event_date<=var_date
				group by 1
				) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) last_30_days_trt, sum(lft_tacs) last_30_days_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime>var_date-30 and created_datetime<=var_date
				group by 1
				) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
			) tbl1; 
		
		insert into data_vajapora.usage_dist_age_greater_than_30_days
		select *
		from data_vajapora.help_a; 
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_greater_than_30_days; 

select *
from data_vajapora.usage_dist_age_greater_than_30_days
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_greater_than_30_days; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_greater_than_30_days 
group by 1
order by 1 desc;
*/

-- generate daily data: data_vajapora.usage_dist_age_equal_to_30_days
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	
	delete from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select var_date report_date, mobile_no, reg_date, var_date-reg_date+1 days_with_tk 
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile
			) tbl1 
			
			inner join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date>var_date-30 and event_date<=var_date
			) tbl2 using(mobile_no)
		where var_date-reg_date+1=30;
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			-- calculation of scores
			total_active_days*1.00/days_with_tk pct_of_active_days_lft_score, 
			active_days_last_30_days/30.00 pct_of_active_days_last_30_days_score, 
			case when lft_trt!=0 then last_30_days_trt*1.00/lft_trt else 0 end pct_of_trt_last_30_days_score,
			case when lft_tacs!=0 then last_30_days_tacs*1.00/lft_tacs else 0 end pct_of_tacs_last_30_days_score
		from 	
			(select 
				-- general metrics
				tbl1.mobile_no, report_date, reg_date, days_with_tk, total_active_days, 
				case when lft_trt is null then 0 else lft_trt end lft_trt, 
				case when lft_tacs is null then 0 else lft_tacs end lft_tacs, 
				case when active_days_last_30_days is null then 0 else active_days_last_30_days end active_days_last_30_days, 
				case when last_30_days_trt is null then 0 else last_30_days_trt end last_30_days_trt, 
				case when last_30_days_tacs is null then 0 else last_30_days_tacs end last_30_days_tacs 
			from 
				data_vajapora.help_b tbl1 
				
				left join 
				
				(select mobile_no, max(date_sequence) total_active_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date<=var_date
				group by 1
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) lft_trt, sum(lft_tacs) lft_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime<=var_date
				group by 1
				) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
				
				left join 
				
				(select mobile_no, count(date_sequence) active_days_last_30_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>var_date-30 and event_date<=var_date
				group by 1
				) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) last_30_days_trt, sum(lft_tacs) last_30_days_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime>var_date-30 and created_datetime<=var_date
				group by 1
				) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
			) tbl1; 
		
		insert into data_vajapora.usage_dist_age_equal_to_30_days
		select *
		from data_vajapora.help_a; 
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_equal_to_30_days; 

select *
from data_vajapora.usage_dist_age_equal_to_30_days
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_equal_to_30_days; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_equal_to_30_days 
group by 1
order by 1 desc;
*/

-- generate daily data: data_vajapora.usage_dist_age_equal_to_1_day
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	
	delete from data_vajapora.usage_dist_age_equal_to_1_day
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			tbl1.mobile_no, var_date report_date, reg_date, days_with_tk,
			case when total_active_days is null then 0 else total_active_days end total_active_days,
			case when total_trt_tacs is null then 0 else total_trt_tacs end total_trt_tacs,
			case when total_trt_tacs is null then 0 else 1 end score
		from 
			(select var_date report_date, mobile_number mobile_no, date(created_at) reg_date, var_date-date(created_at)+1 days_with_tk 
			from public.register_usermobile
			where date(created_at)=var_date
			) tbl1
			
			left join 
							
			(select mobile_no, 1 total_active_days 
			from tallykhata.tallykhata_user_date_sequence_final 
			where event_date=var_date
			group by 1
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
			
			left join 	
						
			(select mobile_no, sum(lft_trt)+sum(lft_tacs) total_trt_tacs
			from data_vajapora.daily_trt_tacs
			where created_datetime=var_date
			group by 1
			) tbl3 on(tbl1.mobile_no=tbl3.mobile_no); 
		
		insert into data_vajapora.usage_dist_age_equal_to_1_day
		select *
		from data_vajapora.help_a; 
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_equal_to_1_day; 

select *
from data_vajapora.usage_dist_age_equal_to_1_day
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_equal_to_1_day; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_equal_to_1_day 
group by 1
order by 1 desc;
*/

-- generate daily data: data_vajapora.usage_dist_age_between_2_to_7_days
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	
	delete from data_vajapora.usage_dist_age_between_2_to_7_days
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select var_date report_date, mobile_no, reg_date, var_date-reg_date+1 days_with_tk 
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile
			) tbl1 
		where var_date-reg_date+1>=2 and var_date-reg_date+1<=7;
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			-- calculation of scores
			total_active_days*1.00/7 pct_of_active_days_lft_score, 
			active_days_last_7_days*1.00/7 pct_of_active_days_last_7_days_score, 
			case when lft_trt!=0 then last_7_days_trt*1.00/lft_trt else 0 end pct_of_trt_last_7_days_score,
			case when lft_tacs!=0 then last_7_days_tacs*1.00/lft_tacs else 0 end pct_of_tacs_last_7_days_score
		from 	
			(select 
				-- general metrics
				tbl1.mobile_no, report_date, reg_date, days_with_tk, total_active_days, 
				case when lft_trt is null then 0 else lft_trt end lft_trt, 
				case when lft_tacs is null then 0 else lft_tacs end lft_tacs, 
				case when active_days_last_7_days is null then 0 else active_days_last_7_days end active_days_last_7_days, 
				case when last_7_days_trt is null then 0 else last_7_days_trt end last_7_days_trt, 
				case when last_7_days_tacs is null then 0 else last_7_days_tacs end last_7_days_tacs 
			from 
				data_vajapora.help_b tbl1 
				
				left join 
				
				(select mobile_no, max(date_sequence) total_active_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date<=var_date
				group by 1
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) lft_trt, sum(lft_tacs) lft_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime<=var_date
				group by 1
				) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
				
				left join 
				
				(select mobile_no, count(date_sequence) active_days_last_7_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>var_date-7 and event_date<=var_date
				group by 1
				) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) last_7_days_trt, sum(lft_tacs) last_7_days_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime>var_date-7 and created_datetime<=var_date
				group by 1
				) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
			) tbl1; 

			insert into data_vajapora.usage_dist_age_between_2_to_7_days
			select *
			from data_vajapora.help_a;
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_between_2_to_7_days; 

select *
from data_vajapora.usage_dist_age_between_2_to_7_days
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_between_2_to_7_days; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_between_2_to_7_days 
group by 1
order by 1 desc;
*/

-- generate daily data: data_vajapora.usage_dist_age_between_8_to_15_days
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	
	delete from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select var_date report_date, mobile_no, reg_date, var_date-reg_date+1 days_with_tk 
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile
			) tbl1 
		where var_date-reg_date+1>=8 and var_date-reg_date+1<=15;
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			-- calculation of scores
			total_active_days*1.00/15 pct_of_active_days_lft_score, 
			active_days_last_15_days*1.00/15 pct_of_active_days_last_15_days_score, 
			case when lft_trt!=0 then last_15_days_trt*1.00/lft_trt else 0 end pct_of_trt_last_15_days_score,
			case when lft_tacs!=0 then last_15_days_tacs*1.00/lft_tacs else 0 end pct_of_tacs_last_15_days_score
		from 	
			(select 
				-- general metrics
				tbl1.mobile_no, report_date, reg_date, days_with_tk, total_active_days, 
				case when lft_trt is null then 0 else lft_trt end lft_trt, 
				case when lft_tacs is null then 0 else lft_tacs end lft_tacs, 
				case when active_days_last_15_days is null then 0 else active_days_last_15_days end active_days_last_15_days, 
				case when last_15_days_trt is null then 0 else last_15_days_trt end last_15_days_trt, 
				case when last_15_days_tacs is null then 0 else last_15_days_tacs end last_15_days_tacs 
			from 
				data_vajapora.help_b tbl1 
				
				left join 
				
				(select mobile_no, max(date_sequence) total_active_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date<=var_date
				group by 1
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) lft_trt, sum(lft_tacs) lft_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime<=var_date
				group by 1
				) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
				
				left join 
				
				(select mobile_no, count(date_sequence) active_days_last_15_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>var_date-15 and event_date<=var_date
				group by 1
				) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) last_15_days_trt, sum(lft_tacs) last_15_days_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime>var_date-15 and created_datetime<=var_date
				group by 1
				) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
			) tbl1; 

			insert into data_vajapora.usage_dist_age_between_8_to_15_days
			select *
			from data_vajapora.help_a;
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_between_8_to_15_days; 

select *
from data_vajapora.usage_dist_age_between_8_to_15_days
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_between_8_to_15_days; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_between_8_to_15_days 
group by 1
order by 1 desc;
*/

-- generate daily data: data_vajapora.usage_dist_age_between_16_to_29_days
do $$ 

declare 
	var_date date:='2021-06-01'; 
begin 
	
	delete from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date>=var_date;
	
	raise notice 'New OP goes below:'; 
	
	loop
	
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select var_date report_date, mobile_no, reg_date, var_date-reg_date+1 days_with_tk 
		from 
			(select mobile_number mobile_no, date(created_at) reg_date
			from public.register_usermobile
			) tbl1 
		where var_date-reg_date+1>=16 and var_date-reg_date+1<=29;
	
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select 
			*,
			-- calculation of scores
			total_active_days*1.00/29 pct_of_active_days_lft_score, 
			active_days_last_29_days*1.00/29 pct_of_active_days_last_29_days_score, 
			case when lft_trt!=0 then last_29_days_trt*1.00/lft_trt else 0 end pct_of_trt_last_29_days_score,
			case when lft_tacs!=0 then last_29_days_tacs*1.00/lft_tacs else 0 end pct_of_tacs_last_29_days_score
		from 	
			(select 
				-- general metrics
				tbl1.mobile_no, report_date, reg_date, days_with_tk, total_active_days, 
				case when lft_trt is null then 0 else lft_trt end lft_trt, 
				case when lft_tacs is null then 0 else lft_tacs end lft_tacs, 
				case when active_days_last_29_days is null then 0 else active_days_last_29_days end active_days_last_29_days, 
				case when last_29_days_trt is null then 0 else last_29_days_trt end last_29_days_trt, 
				case when last_29_days_tacs is null then 0 else last_29_days_tacs end last_29_days_tacs 
			from 
				data_vajapora.help_b tbl1 
				
				left join 
				
				(select mobile_no, max(date_sequence) total_active_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date<=var_date
				group by 1
				) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) lft_trt, sum(lft_tacs) lft_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime<=var_date
				group by 1
				) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
				
				left join 
				
				(select mobile_no, count(date_sequence) active_days_last_29_days 
				from tallykhata.tallykhata_user_date_sequence_final 
				where event_date>var_date-29 and event_date<=var_date
				group by 1
				) tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
				
				left join 	
				
				(select mobile_no, sum(lft_trt) last_29_days_trt, sum(lft_tacs) last_29_days_tacs
				from data_vajapora.daily_trt_tacs
				where created_datetime>var_date-29 and created_datetime<=var_date
				group by 1
				) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
			) tbl1; 

			insert into data_vajapora.usage_dist_age_between_16_to_29_days
			select *
			from data_vajapora.help_a;
		
		raise notice 'Data generated for: %', var_date; 
		
		var_date:=var_date+1; 
		if var_date=current_date then exit;
		end if; 
	
	end loop; 

	drop table if exists data_vajapora.help_a;
	drop table if exists data_vajapora.help_b;
	
end $$; 

/*
truncate table data_vajapora.usage_dist_age_between_16_to_29_days; 

select *
from data_vajapora.usage_dist_age_between_16_to_29_days
limit 1000; 

select max(report_date) max_date
from data_vajapora.usage_dist_age_between_16_to_29_days; 

select report_date, count(mobile_no) users_tracked
from data_vajapora.usage_dist_age_between_16_to_29_days 
group by 1
order by 1 desc;
*/

-- see the data generated 
select * from data_vajapora.usage_dist_age_greater_than_30_days limit 10000;
select * from data_vajapora.usage_dist_age_equal_to_30_days limit 10000;

select * from data_vajapora.usage_dist_age_equal_to_1_day limit 10000;

select * from data_vajapora.usage_dist_age_between_2_to_7_days limit 10000;
select * from data_vajapora.usage_dist_age_between_8_to_15_days limit 10000;
select * from data_vajapora.usage_dist_age_between_16_to_29_days limit 10000; 

-- make necessary updates
ALTER TABLE data_vajapora.usage_dist_age_between_2_to_7_days ALTER COLUMN pct_of_active_days_last_7_days_score TYPE float USING pct_of_active_days_last_7_days_score::float;
ALTER TABLE data_vajapora.usage_dist_age_between_8_to_15_days ALTER COLUMN pct_of_active_days_last_15_days_score TYPE float USING pct_of_active_days_last_15_days_score::float;
ALTER TABLE data_vajapora.usage_dist_age_between_16_to_29_days ALTER COLUMN pct_of_active_days_last_29_days_score TYPE float USING pct_of_active_days_last_29_days_score::float;


update data_vajapora.usage_dist_age_between_2_to_7_days
set total_active_days=0 
where total_active_days is null;

update data_vajapora.usage_dist_age_between_8_to_15_days
set total_active_days=0 
where total_active_days is null;

update data_vajapora.usage_dist_age_between_16_to_29_days
set total_active_days=0 
where total_active_days is null;


update data_vajapora.usage_dist_age_between_2_to_7_days
set pct_of_active_days_last_7_days_score=active_days_last_7_days/days_with_tk::float;

update data_vajapora.usage_dist_age_between_8_to_15_days
set pct_of_active_days_last_15_days_score=active_days_last_15_days/days_with_tk::float;

update data_vajapora.usage_dist_age_between_16_to_29_days
set pct_of_active_days_last_29_days_score=active_days_last_29_days/days_with_tk::float;

/* count of merchants against different blocks */

-- block 04
/*
Conditions: 
Age =30, no activity T/TACS recorded till now.
if usage < 17% days they are in low useage pattern
if usage > 18% and < 33% days the are potential and passed (3RAU)
if usage >=33% days they are power user
if usage >66% days then are Super power user (SPU)
*/

select count(mobile_no) users 
from data_vajapora.usage_dist_age_equal_to_30_days
where 
	report_date='2021-07-06'
	and lft_trt+lft_tacs=0;

select count(mobile_no) users
from data_vajapora.usage_dist_age_equal_to_30_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_30_days_score<=0.17; 

select count(mobile_no) users
from data_vajapora.usage_dist_age_equal_to_30_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_30_days_score>0.17 and pct_of_active_days_last_30_days_score<0.33; 

select count(mobile_no) users
from data_vajapora.usage_dist_age_equal_to_30_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_30_days_score>=0.33; 

select count(mobile_no) users
from data_vajapora.usage_dist_age_equal_to_30_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_30_days_score>0.66; 

-- block 02
/*
Conditions:
Age 2 to 7 days
if usage < 43% days they are in low useage pattern
if usage >43% days they are potential and passed 3RAU
*/

select count(mobile_no) users
from data_vajapora.usage_dist_age_between_2_to_7_days
where 
	report_date='2021-07-06'
	-- and lft_trt+lft_tacs=0; 

select count(mobile_no) users
from data_vajapora.usage_dist_age_between_2_to_7_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_7_days_score<0.43
	and lft_trt+lft_tacs>0; 

select count(mobile_no) users
from data_vajapora.usage_dist_age_between_2_to_7_days
where 
	report_date='2021-07-06'
	and pct_of_active_days_last_7_days_score>=0.43; 

-- block 05
/*
Conditions:
Last 30 days activity on any given day [ >30 days age user ]
Age >=31, no activity T/TACS recorded till now.
*/

select count(mobile_no) users
from data_vajapora.usage_dist_age_greater_than_30_days
where 
	report_date='2021-07-06'
	and lft_trt+lft_tacs=0;

/* mine out rules for face yellow-marked block */

-- Age =30, no activity T/TACS recorded till now.
select 
	count(distinct mobile_no) pus
from 
	(select * 
	from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date='2021-07-06'
	
	union all
	
	select * 
	from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date='2021-07-06'
	
	union all
	
	select *
	from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date='2021-07-06'
	) tbl1 
where lft_trt+lft_tacs=0; 

-- if usage < 17% days they are in low useage pattern
select 
	count(distinct mobile_no) pus
from 
	(select * 
	from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date='2021-07-06'
	
	union all
	
	select * 
	from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date='2021-07-06'
	
	union all
	
	select *
	from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date='2021-07-06'
	) tbl1 
where 
	pct_of_active_days_last_15_days_score<0.17
	and total_active_days>0; 

-- if usage > 18% and < 33% days the are potential and passed (3RAU)
select 
	count(distinct mobile_no) pus
from 
	(select * 
	from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date='2021-07-06'
	
	union all
	
	select * 
	from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date='2021-07-06'
	
	union all
	
	select *
	from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date='2021-07-06'
	) tbl1 
where 
	pct_of_active_days_last_15_days_score>=0.17 and pct_of_active_days_last_15_days_score<0.33
	and total_active_days>0; 

-- if usage >=33% days they are power user
select 
	count(distinct mobile_no) pus
from 
	(select * 
	from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date='2021-07-06'
	
	union all
	
	select * 
	from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date='2021-07-06'
	
	union all
	
	select *
	from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date='2021-07-06'
	) tbl1 
where 
	pct_of_active_days_last_15_days_score>=0.33
	and total_active_days>=10; 

-- if usage >66% days then are Super power user (SPU)
select 
	count(distinct mobile_no) pus
from 
	(select * 
	from data_vajapora.usage_dist_age_between_8_to_15_days
	where report_date='2021-07-06'
	
	union all
	
	select * 
	from data_vajapora.usage_dist_age_between_16_to_29_days
	where report_date='2021-07-06'
	
	union all
	
	select *
	from data_vajapora.usage_dist_age_equal_to_30_days
	where report_date='2021-07-06'
	) tbl1 
where 
	pct_of_active_days_last_15_days_score>=0.66
	and total_active_days>=20; 



