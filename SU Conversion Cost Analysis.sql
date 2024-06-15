/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=302507610
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

-- analysis-01
do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	-- 30-day specifics initial
	drop table if exists data_vajapora.last_30_day_specs; 
	create table data_vajapora.last_30_day_specs(mobile_no text, days_active int); 

	loop
		-- DAU
		drop table if exists data_vajapora.temp_a; 
		create table data_vajapora.temp_a as
		
		select mobile_no
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union
		
		select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received', 'inbox_message_received')
			
		union
			
		select ss.mobile_number mobile_no
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date; 
		
		-- 30-day specifics final
		update data_vajapora.last_30_day_specs 
		set days_active=days_active+1 
		where mobile_no in 
			(select mobile_no
			from 
				data_vajapora.temp_a tbl1
				inner join 
				data_vajapora.last_30_day_specs tbl2 using(mobile_no)
			); 
		insert into data_vajapora.last_30_day_specs 
		select mobile_no, 1 days_active
		from 
			data_vajapora.temp_a tbl1
			left join 
			data_vajapora.last_30_day_specs tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 

		commit; 
		raise notice 'MAU generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if;
	end loop;
end $$; 

select 
	days_cat, 
	count(tbl1.mobile_no) opened_last_30_days, 
	count(tbl2.mobile_no) dau_16_may_2022
from 
	(select 
		*,
		case 
			when days_active<3 then '01-02 days'
			when days_active<5 then '03-04 days'
			when days_active<10 then '05-09 days'
			when days_active<15 then '10-14 days'
			when days_active<20 then '11-19 days'
			when days_active<25 then '20-24 days'
			else '25 or more days'
		end days_cat
	from data_vajapora.last_30_day_specs
	) tbl1 
	
	left join 
	
	(select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime='2022-05-16'::date
	
	union
	
	select mobile_no
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date='2022-05-16'::date
		and event_name not in ('in_app_message_received', 'inbox_message_received')
		
	union
		
	select ss.mobile_number mobile_no
	from 
		public.user_summary as ss 
		left join 
		public.register_usermobile as i on ss.mobile_number = i.mobile_number
	where 
		i.mobile_number is null 
		and ss.created_at::date='2022-05-16'::date
	) tbl2 using(mobile_no)
group by 1 
order by 1; 

-- analysis-02
do $$ 

declare 
	var_month text:='2022-05'; -- change
	var_date date:=concat(var_month, '-01')::date; 
	var_date_start date:=var_date-1; 
	var_date_end date:= concat(left((var_date+35)::text, 7), '-01')::date; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- MAU initial
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b(mobile_no text); 
	
	loop
		-- DAU
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		
		select mobile_no
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union
		
		select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received', 'inbox_message_received')
			
		union
			
		select ss.mobile_number mobile_no
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date; 
		
		-- MAU incremental
		insert into data_vajapora.help_b 
		select mobile_no 
		from 
			data_vajapora.help_a tbl1
			left join 
			data_vajapora.help_b tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
	
		commit; 
		raise notice 'MAU generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=var_date_end then exit; 
		end if;
	end loop;
	raise notice 'MAU generated for: %', var_month; 

	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as 
	select
		tbl1.mobile_no, 
		case
			when tg in('SPU') or tbl1.mobile_no in(select mobile_no from tallykhata.tk_spu_aspu_data where pu_type in('SPU') and report_date=var_date_start) then 'SU'                      
			when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
			when tg in('LTUCb','LTUTa') then 'LTU'
			when tg in('NB0','NN1','NN2-6') then 'NN'
			when tg in('NT--') then 'NT'
			when tg in('PSU') then 'PSU'
			when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
			when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
			else null
		end tg, 
		tbl3.mobile_no su_mobile_no
	from 
		data_vajapora.help_b tbl1
		
		left join 
		
		(select mobile_no, max(tg) tg 
		from cjm_segmentation.retained_users 
		where report_date=var_date_start
		group by 1
		) tbl2 using(mobile_no)
		
		left join
		
		(select distinct mobile_no 
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type in('SPU')
			and report_date>current_date-16
		) tbl3 using(mobile_no); 
	
	delete from data_vajapora.mau_distrib_last_month
	where month_year=var_month; 
	insert into data_vajapora.mau_distrib_last_month
	select 
		var_month month_year, 
		
		count(mobile_no) mau, 
		count(case when tg='3RAU' then mobile_no else null end) "3RAU", 
		count(case when tg='LTU' then mobile_no else null end) "LTU", 
		count(case when tg='NN' then mobile_no else null end) "NN", 
		count(case when tg='NT' then mobile_no else null end) "NT", 
		count(case when tg='PSU' then mobile_no else null end) "PSU", 
		count(case when tg='PU' then mobile_no else null end) "PU", 
		count(case when tg='SU' then mobile_no else null end) "SU", 
		count(case when tg='Zombie' then mobile_no else null end) "Zombie", 
		count(case when tg is null then mobile_no else null end) "other",
		
		count(su_mobile_no) "SU last 15 days", 
		count(case when tg='3RAU' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days 3RAU", 
		count(case when tg='LTU' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days LTU", 
		count(case when tg='NN' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days NN", 
		count(case when tg='NT' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days NT", 
		count(case when tg='PSU' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days PSU", 
		count(case when tg='PU' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days PU", 
		count(case when tg='SU' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days SU", 
		count(case when tg='Zombie' and su_mobile_no is not null then mobile_no else null end) "SU last 15 days Zombie", 
		count(case when tg is null and su_mobile_no is not null then mobile_no else null end) "SU last 15 days other"
	from data_vajapora.help_c;

	delete from data_vajapora.mau_distrib_last_month_2
	where month_year=var_month; 
	insert into data_vajapora.mau_distrib_last_month_2
	select var_month month_year, mobile_no, tg 
	from data_vajapora.help_c
	where su_mobile_no is not null;

end $$; 

select * 
from data_vajapora.mau_distrib_last_month
order by 1; 

select
	month_year, 
	case when tg is null then 'other' else tg end tg, 
	count(mobile_no) kowm, 
	count(case when first_su_date>=current_date-15 then mobile_no else null end) first_time_su, 
	count(case when first_su_date<current_date-15 then mobile_no else null end) su_previously
from 
	data_vajapora.mau_distrib_last_month_2 tbl1 
	
	left join 
	
	(select mobile_no, min(report_date) first_su_date 
	from tallykhata.tk_spu_aspu_data 
	where pu_type in('SPU')
	group by 1
	) tbl2 using(mobile_no)
group by 1, 2
order by 1, 2;
