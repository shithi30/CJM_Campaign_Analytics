/*
- Viz: 
- Data: 
- Function: data_vajapora.fn_merchant_base_in_20_segments()
- Table: data_vajapora.merchant_base_in_20_segments
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: CJM Data testing
- Notes (if any): 
	- Mahmud's auto email script will shoot the data. 
	- in date wise folders: https://drive.google.com/drive/folders/1-1gzTf6QQhzv5zHBPLpb6pwSSEURn4f2 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_merchant_base_in_20_segments()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

declare
	var_date date:=current_date-1; 
begin
	-- generate today's data afresh
	delete from data_vajapora.merchant_base_in_20_segments
	where date(data_generation_time)=current_date; 

	-- retained merchants of today: use Python for this
	drop table if exists data_vajapora.seg_help_d;
	create table data_vajapora.seg_help_d as 
	select tbl2.mobile_no, reg_date, var_date::date-reg_date+1 days_with_tk
	from 
		(select concat('0', mobile_no) mobile_no
		from data_vajapora.retained_today
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile
		where date(created_at)<=var_date::date
		) tbl2 using(mobile_no); 
	
	-- 3RAU statistics
	drop table if exists data_vajapora.seg_help_a;
	create table data_vajapora.seg_help_a as
	select mobile_no, max(rau_date) max_3_rau_date, count(rau_date) rau_days
	from tallykhata.tallykhata_regular_active_user
	where 
		rau_category=3
		and rau_date<=var_date::date
	group by 1; 
	
	-- PU statistics
	drop table if exists data_vajapora.seg_help_c;
	create table data_vajapora.seg_help_c as
	select mobile_no, max(report_date) max_pu_date, count(distinct report_date) pu_days
	from tallykhata.tk_power_users_10
	where report_date<=var_date::date
	group by 1; 
	
	-- all features combined
	drop table if exists data_vajapora.seg_help_b;
	create table data_vajapora.seg_help_b as
	select 
		tbl1.*, 
		case when tbl2.created_datetime is null then 0 else 1 end if_first_day_active,
		case when tbl3.txn_days is null then 0 else tbl3.txn_days end txn_days,
		case when tbl4.rau_days is null then 0 else tbl4.rau_days end days_in_3_rau,
		tbl4.max_3_rau_date, 
		var_date::date-tbl4.max_3_rau_date days_after_last_3_rau,
		case when tbl5.mobile_no is null then 0 else 1 end if_pu_yesterday,
		case when tbl7.mobile_no is null then 0 else 1 end if_3_rau_yesterday, 
		case when active_last_30_days is null then 0 else active_last_30_days end active_last_30_days,
		case when tbl8.pu_days is null then 0 else tbl8.pu_days end days_in_pu,
		tbl8.max_pu_date
	from 
		data_vajapora.seg_help_d tbl1 
		
		left join 
		
		(select mobile_no, created_datetime 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.reg_date=tbl2.created_datetime)
		
		left join 
		
		(select mobile_no, count(created_datetime) txn_days 
		from tallykhata.tallykhata_transacting_user_date_sequence_final  
		where created_datetime<=var_date::date
		group by 1 
		) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
		
		left join 
		
		data_vajapora.seg_help_a tbl4 on(tbl1.mobile_no=tbl4.mobile_no)
		
		left join 
		
		(select distinct mobile_no
		from tallykhata.tk_power_users_10
		where report_date=var_date::date-1
		) tbl5 on(tbl1.mobile_no=tbl5.mobile_no)
		
		left join 
		
		(select mobile_no, count(created_datetime) active_last_30_days
		from tallykhata.tallykhata_transacting_user_date_sequence_final
		where created_datetime>=var_date::date-30 and created_datetime<var_date::date 
		group by 1
		) tbl6 on(tbl1.mobile_no=tbl6.mobile_no)
		
		left join 
		
		(select distinct mobile_no
		from tallykhata.tallykhata_regular_active_user
		where 
			rau_category=3
			and rau_date=var_date::date-1
		) tbl7 on(tbl1.mobile_no=tbl7.mobile_no)
		
		left join 
		
		data_vajapora.seg_help_c tbl8 on(tbl1.mobile_no=tbl8.mobile_no); 
	
	-- segmented user-base
	insert into data_vajapora.merchant_base_in_20_segments
	select 
		mobile_no,
		case
			when days_with_tk=1 and if_first_day_active=0 then 'S01 age 1: day-01 not transacted' -- S1
			when days_with_tk=1 and if_first_day_active=1 then 'S02 age 1: day-01 transacted' -- S2
			
			when days_with_tk>=2 and days_with_tk<=7 and txn_days=0 then 'S03 age 2-7: no transaction' -- S3
			when days_with_tk>=2 and days_with_tk<=7 and days_in_3_rau=0 then 'S04 age 2-7: did not enter 3RAU' -- S4
			when days_with_tk>=2 and days_with_tk<=7 and days_in_3_rau!=0 then 'S05 age 2-7: entered 3RAU' -- S5
			
			when days_with_tk>=8 and days_with_tk<=29 and if_pu_yesterday=1 then 'S12(1) age 8-29: PU yesterday' -- S12(1)
			when days_with_tk>=8 and days_with_tk<=29 and txn_days=0 then 'S08 age 8-29: no txn till now' -- S8
			when days_with_tk>=8 and days_with_tk<=29 and var_date::date-max_3_rau_date=0 and days_in_3_rau=1 then 'S09 age 8-29: today 3RAU 1st' -- S9
			when days_with_tk>=8 and days_with_tk<=29 and var_date::date-max_3_rau_date=0 and days_in_3_rau>1 then 'S10 age 8-29: today 3RAU cont.' -- S10
			when days_with_tk>=8 and days_with_tk<=29 and var_date::date-max_3_rau_date=1 then 'S06 age 8-29: was 3RAU yesterday' -- S6
			when days_with_tk>=8 and days_with_tk<=29 and var_date::date-max_3_rau_date>1 then 'S07 age 8-29: gaps after 3RAU' -- S7
			when days_with_tk>=8 and days_with_tk<=29 and max_3_rau_date is null then 'S11 age 8-29: low usage' -- S11
			
			when days_with_tk>29 and txn_days=0 then 'S13 age >29: no transaction' -- S13
			when days_with_tk>29 and active_last_30_days=0 then 'S14 age >29: inactive last 29 days' -- S14
			when days_with_tk>29 and if_pu_yesterday=1 and active_last_30_days<20 then 'S12(2) age >29: was PU yesterday' -- S12(2)
			when days_with_tk>29 and if_pu_yesterday=1 and active_last_30_days>=20 then 'S15 age >29: was SPU yesterday' -- S15
			when days_with_tk>29 and days_in_3_rau=0 then 'S16 age >29: not entered 3RAU till now' -- S16
			when days_with_tk>29 and if_3_rau_yesterday=1 then 'S17 age >29: 3RAU yesterday' -- S17
			
			-- remaining
			when days_with_tk>29 and days_in_pu=0 then 'S18 age >29: never became PU' -- S18
			when days_with_tk>29 and days_in_pu>0 and var_date::date-max_pu_date!=0 then 'S19 age >29: once PU currently not' -- S19
			when days_with_tk>29 and days_in_pu>0 and var_date::date-max_pu_date=0 then 'S21? age >29: once PU currently PU' -- S21?
			
			else 'unknown'
		end cjm_segment,
		now() data_generation_time
	from data_vajapora.seg_help_b; 
	
	-- drop auxiliary tables 
	drop table if exists data_vajapora.seg_help_a; 
	drop table if exists data_vajapora.seg_help_b; 
	drop table if exists data_vajapora.seg_help_c; 
	drop table if exists data_vajapora.seg_help_d; 
	
END;
$function$
;

/*
select data_vajapora.fn_merchant_base_in_20_segments(); 

-- distribution of segments
select *
from 
	(select left(cjm_segment, 3) cjm_segment, count(*) merchants
	from data_vajapora.merchant_base_in_20_segments 
	group by 1 
	) tbl1
where cjm_segment!='S21'
order by 1;

select mobile_no, left(cjm_segment, 3) cjm_segment, data_generation_time
from data_vajapora.merchant_base_in_20_segments
where date(data_generation_time)=current_date; 
*/
