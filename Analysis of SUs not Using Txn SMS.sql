/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=250211188
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): In monthly SMS report, it was found that around 20k SUs sent no txn SMS. They mostly fulfilled SU-criteria in previous month. 
*/

do $$ 

declare 
	var_seq int:=14; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_sms_month asc) seq 
	from 
		(select left(txn_sms_date::text, 7) txn_sms_month, max(txn_sms_date) txn_sms_month_last_date 
		from 
			(select generate_series(0, current_date-'2021-07-01'::date, 1)+'2021-07-01'::date txn_sms_date
			) tbl1 
		group by 1
		) tbl1; 
	
	loop 
		-- tagada/txn SMS of month
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select 
			id, 
			translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
			case when message_body like '%অনুগ্রহ করে%' then 'tagada' else 'txn' end sms_type
		from public.t_scsms_message_archive_v2 as s
		where
			upper(s.channel) in('TALLYKHATA_TXN') 
			and upper(trim(s.bank_name)) = 'SURECASH'
			and lower(s.message_body) not like '%verification code%'
			and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
			and upper(s.message_status) in ('SUCCESS', '0')
			and left(s.request_time::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		
		-- SPUs of month
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select distinct mobile_no, (select txn_sms_month from data_vajapora.help_c where seq=var_seq) txn_sms_month
		from tallykhata.tk_spu_aspu_data 
		where 
			pu_type='SPU'
			and left(report_date::text, 7)=(select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
	
		-- txn SMS: merchants
		insert into data_vajapora.sus_notusing_txn_sms
		select *
		from 
			data_vajapora.help_b tbl1
			
			left join 
			
			(select distinct mobile_no
			from data_vajapora.help_a 
			where sms_type='txn'
			) tbl2 using(mobile_no)
		where tbl2.mobile_no is null; 
		
		commit; 
		raise notice 'Data generated for: %', (select txn_sms_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.sus_notusing_txn_sms; 

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, left(report_date::text, 7) txn_sms_month, min(report_date) first_su_date 
from tallykhata.tk_spu_aspu_data 
where 
	pu_type='SPU'
	and report_date>='2022-08-01' and report_date<current_date 
group by 1, 2; 

-- SUs not using txn SMS mostly fulfilled criteria in previous month
select 
	txn_sms_month, 
	count(case when date_part('day', first_su_date)>=1 and date_part('day', first_su_date)<=10 then mobile_no else null end) sus_first_10_days, 
	count(case when date_part('day', first_su_date)>=11 and date_part('day', first_su_date)<=20 then mobile_no else null end) sus_second_10_days, 
	count(case when date_part('day', first_su_date)>20 then mobile_no else null end) sus_third_10_days
from 
	data_vajapora.sus_notusing_txn_sms tbl1 
	left join 
	data_vajapora.help_a tbl2 using(mobile_no, txn_sms_month) 
group by 1 
order by 1; 
