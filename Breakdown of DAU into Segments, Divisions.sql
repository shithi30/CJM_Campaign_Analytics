/*
- Viz: 
	- Definition of Segments: https://docs.google.com/spreadsheets/d/1Z-SM02m_s6FR6-UBioYp-mUcZ4-GTnr2a3SOxh9Y6lQ/edit#gid=183262388
	- Segment-wise distribution: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1458453804
	- Segment-wise distribution 2: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1743558155
	- Division-wise distribution: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1299589660
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

-- breakdown of DAUs into segments
do $$ 

declare 
	var_date date:=current_date-7; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_breakdown_to_segments
		where report_date=var_date; 
	
		insert into data_vajapora.dau_breakdown_to_segments 
		select
			var_date report_date, 
			count(mobile_no) dau,
			count(case when tg_shrunk='PUAll' then mobile_no else null end) dau_puall,
			count(case when tg_shrunk='3RAUAll' then mobile_no else null end) dau_3rauall,
			count(case when tg_shrunk='LTUAll' then mobile_no else null end) dau_ltuall,
			count(case when tg_shrunk='ZAll' then mobile_no else null end) dau_zall,
			count(case when tg_shrunk='PSU' then mobile_no else null end) dau_psu,
			count(case when tg_shrunk='NN2-6' then mobile_no else null end) dau_nn26,
			count(case when tg_shrunk='NN1' then mobile_no else null end) dau_nn1,
			count(case when tg_shrunk='NT' then mobile_no else null end) dau_nt,
			count(case when tg_shrunk is null then mobile_no else null end) dau_rest
		from 
			(-- DAUs
			select mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			
			union 
			
			select mobile_no 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				date(created_date)=var_date
				and event_name not in ('in_app_message_received','inbox_message_received')
				
			union 
				
			select ss.mobile_number mobile_no
			from 
				public.user_summary as ss 
				left join 
				public.register_usermobile as i on ss.mobile_number = i.mobile_number
			where 
				i.mobile_number is null 
				and ss.created_at::date=var_date
			) tbl1 
			
			left join 
			
			(-- TG
			select 
				mobile_no, 
				tg, 
				case 
					when tg ilike 'pu%' then 'PUAll'
					when tg ilike '3rau%'  then '3RAUAll'
					when tg ilike 'ltu%' then 'LTUAll'
					when tg ilike 'z%' then 'ZAll'
					when tg ilike 'psu%' then 'PSU'
					when tg ilike '%NN2-6%' then 'NN2-6'
					when tg ilike '%NN1%' then 'NN1'
				else 'NT' end tg_shrunk
			from cjm_segmentation.retained_users 
			where report_date=var_date -- change here
			) tbl2 using(mobile_no);
		
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.dau_breakdown_to_segments
order by 1; 

-- DAU breakdown into detailed segments 
do $$ 

declare 
	var_date date:=current_date-10; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.dau_breakdown_to_segments_detailed 
		where report_date=var_date; 
	
		insert into data_vajapora.dau_breakdown_to_segments_detailed
		select
			var_date report_date, 
			count(mobile_no) dau,
			count(case when tg='PU Set-A' then mobile_no else null end) dau_PU_Set_A,
			count(case when tg='3RAU Set-C' then mobile_no else null end) dau_3RAU_Set_C,
			count(case when tg='3RAU Set-A' then mobile_no else null end) dau_3RAU_Set_A,
			count(case when tg='3RAUTa' then mobile_no else null end) dau_3RAUTa,
			count(case when tg='3RAUTacs' then mobile_no else null end) dau_3RAUTacs,
			count(case when tg='NN1' then mobile_no else null end) dau_NN1,
			count(case when tg='NT--' then mobile_no else null end) dau_NT,
			count(case when tg='PUCb' then mobile_no else null end) dau_PUCb,
			count(case when tg='ZTa+Cb' then mobile_no else null end) dau_ZTa_Cb,
			count(case when tg='LTUTa' then mobile_no else null end) dau_LTUTa,
			count(case when tg='NN2-6' then mobile_no else null end) dau_NN2_6,
			count(case when tg='PUTa' then mobile_no else null end) dau_PUTa,
			count(case when tg='PUTacs' then mobile_no else null end) dau_PUTacs,
			count(case when tg='ZTa' then mobile_no else null end) dau_ZTa,
			count(case when tg='PSU' then mobile_no else null end) dau_PSU,
			count(case when tg='3RAUCb' then mobile_no else null end) dau_3RAUCb,
			count(case when tg='3RAUTa+Cb' then mobile_no else null end) dau_3RAUTa_Cb,
			count(case when tg='PUTa+Cb' then mobile_no else null end) dau_PUTa_Cb,
			count(case when tg='PU Set-B' then mobile_no else null end) dau_PU_Set_B,
			count(case when tg='3RAU Set-B' then mobile_no else null end) dau_3RAU_Set_B,
			count(case when tg='ZCb' then mobile_no else null end) dau_ZCb,
			count(case when tg='PU Set-C' then mobile_no else null end) dau_PU_Set_C,
			count(case when tg='LTUCb' then mobile_no else null end) dau_LTUCb,
			count(case when tg is null then mobile_no else null end) dau_rest
		from 
			(-- DAUs
			select mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			
			union 
			
			select mobile_no 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				date(created_date)=var_date
				and event_name not in ('in_app_message_received','inbox_message_received')
				
			union 
				
			select ss.mobile_number mobile_no
			from 
				public.user_summary as ss 
				left join 
				public.register_usermobile as i on ss.mobile_number = i.mobile_number
			where 
				i.mobile_number is null 
				and ss.created_at::date=var_date
			) tbl1 
			
			left join 
			
			(-- TG
			select mobile_no, tg 
			from cjm_segmentation.retained_users 
			where report_date=var_date -- change here
			) tbl2 using(mobile_no);
			
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-5 then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.dau_breakdown_to_segments_detailed
order by 1; 

-- dig down for a single day
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select '2021-12-03'::date report_date, *
from 
	(-- DAUs
	select mobile_no
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime='2021-12-03'::date
	
	union 
	
	select mobile_no 
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		date(created_date)='2021-12-03'::date
		and event_name not in ('in_app_message_received','inbox_message_received')
		
	union 
		
	select ss.mobile_number mobile_no
	from 
		public.user_summary as ss 
		left join 
		public.register_usermobile as i on ss.mobile_number = i.mobile_number
	where 
		i.mobile_number is null 
		and ss.created_at::date='2021-12-03'::date
	) tbl1 
	
	left join 
	
	(-- TG
	select mobile_no, tg 
	from cjm_segmentation.retained_users 
	where report_date='2021-12-03'::date -- change here
	) tbl2 using(mobile_no)
	
	left join 

	(-- reg date
	select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	) tbl3 using(mobile_no)
where tbl2.mobile_no is null; 

select 
	count(mobile_no) dau_absent_in_segment,
	count(case when report_date=reg_date then mobile_no else null end) reg_on_dau_date, 
	count(case when report_date>reg_date then mobile_no else null end) reinstalled, 
	count(case when report_date<reg_date then mobile_no else null end) unverified_got_verified, 
	count(case when reg_date is null then mobile_no else null end) still_unverified
from data_vajapora.help_a; 

-- breakdown of DAUs into divisions
do $$ 

declare 
	var_date date:='2021-11-20'::date; 
begin
	raise notice 'New OP goes below:'; 

	-- merchants with divisions
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as 
	select mobile mobile_no, division_name
	from tallykhata.tallykhata_clients_location_info; 

	loop
		delete from data_vajapora.dau_breakdown_to_divisions
		where report_date=var_date; 
	
		insert into data_vajapora.dau_breakdown_to_divisions
		select
			var_date report_date, 
			count(mobile_no) dau,
			count(case when division_name='Khulna' then mobile_no else null end) dau_khulna,
			count(case when division_name='Barisal' then mobile_no else null end) dau_barisal,
			count(case when division_name='Dhaka' then mobile_no else null end) dau_dhaka,
			count(case when division_name='Rangpur' then mobile_no else null end) dau_rangpur,
			count(case when division_name='Chittagong' then mobile_no else null end) dau_chittagong,
			count(case when division_name='Sylhet' then mobile_no else null end) dau_sylhet,
			count(case when division_name='Mymensingh' then mobile_no else null end) dau_mymensingh,
			count(case when division_name='Rajshahi' then mobile_no else null end) dau_rajshahi
		from 
			(-- DAUs
			select mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			
			union 
			
			select mobile_no 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				date(created_date)=var_date
				and event_name not in ('in_app_message_received','inbox_message_received')
				
			union 
				
			select ss.mobile_number mobile_no
			from 
				public.user_summary as ss 
				left join 
				public.register_usermobile as i on ss.mobile_number = i.mobile_number
			where 
				i.mobile_number is null 
				and ss.created_at::date=var_date
			) tbl1 
			
			left join 
			
			data_vajapora.help_a tbl2 using(mobile_no);
			
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.dau_breakdown_to_divisions; 
   