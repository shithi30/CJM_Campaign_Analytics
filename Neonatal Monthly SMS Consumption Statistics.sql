/*
- Viz: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1765400061
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

do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by sms_month asc) seq 
	from 
		(select left(txn_sms_date::text, 7) sms_month, max(txn_sms_date) sms_month_last_date 
		from 
			(select generate_series(0, current_date-'2022-07-01'::date, 1)+'2022-07-01'::date txn_sms_date
			) tbl1 
		group by 1
		) tbl1; 
	
	raise notice 'Calander generated.'; 
	
	-- tagada/txn SMS of months
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select 
		id, 
		translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') mobile_no, 
		case 
			when message_body like '%অনুগ্রহ করে%' then 'tagada'
			else 'txn'
		end sms_type, 
		date(request_time) sms_date 
	from public.t_scsms_message_archive_v2 as s
	where
		upper(s.channel) in('TALLYKHATA_TXN') 
		and upper(trim(s.bank_name)) = 'SURECASH'
		and lower(s.message_body) not like '%verification code%'
		and s.telco_identifier_id in(66, 64, 61, 62, 49, 67) 
		and upper(s.message_status) in ('SUCCESS', '0')
		and left(s.request_time::text, 7) in(select sms_month from data_vajapora.help_c);
	
	raise notice 'SMS table generated.'; 
	
	loop 
		delete from data_vajapora.reg_txn_sms_monthly_distributions
		where year_month=(select sms_month from data_vajapora.help_c where seq=var_seq); 
		delete from data_vajapora.reg_txn_sms_monthly_distributions_2
		where year_month=(select sms_month from data_vajapora.help_c where seq=var_seq);  
	
		delete from data_vajapora.reg_tagada_sms_monthly_distributions
		where year_month=(select sms_month from data_vajapora.help_c where seq=var_seq); 
		delete from data_vajapora.reg_tagada_sms_monthly_distributions_2
		where year_month=(select sms_month from data_vajapora.help_c where seq=var_seq);  
		
		-- regs of month
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as		
		select distinct mobile_number mobile_no
		from public.register_usermobile
		where left(created_at::text, 7)=(select sms_month from data_vajapora.help_c where seq=var_seq);
		
		-- txn SMS: merchants
		insert into data_vajapora.reg_txn_sms_monthly_distributions
		select 
			(select sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			count(tbl1.mobile_no) merchants_consumed_txn_sms, 
			count(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_1_to_10, 
			count(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_11_to_20, 
			count(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_21_to_50, 
			count(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_51_to_100, 
			count(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_101_to_150, 
			count(case when txn_sms_consumed>150 then tbl1.mobile_no else null end) merchants_consumed_txn_sms_more_than_150
		from 
			(select mobile_no, count(id) txn_sms_consumed
			from data_vajapora.help_a 
			where 
				sms_type!='tagada'
				and left(sms_date::text, 7)=(select sms_month from data_vajapora.help_c where seq=var_seq)
			group by 1 
			) tbl1 
			
			inner join 
						
			data_vajapora.help_b tbl2 using(mobile_no); 
		
		-- txn SMS: messages
		insert into data_vajapora.reg_txn_sms_monthly_distributions_2 
		select 
			(select sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			sum(txn_sms_consumed) txn_sms_consumed, 
			sum(case when txn_sms_consumed>=1 and txn_sms_consumed<=10 then txn_sms_consumed else 0 end) txn_sms_consumed_1_to_10, 
			sum(case when txn_sms_consumed>10 and txn_sms_consumed<=20 then txn_sms_consumed else 0 end) txn_sms_consumed_11_to_20, 
			sum(case when txn_sms_consumed>20 and txn_sms_consumed<=50 then txn_sms_consumed else 0 end) txn_sms_consumed_21_to_50, 
			sum(case when txn_sms_consumed>50 and txn_sms_consumed<=100 then txn_sms_consumed else 0 end) txn_sms_consumed_51_to_100, 
			sum(case when txn_sms_consumed>100 and txn_sms_consumed<=150 then txn_sms_consumed else 0 end) txn_sms_consumed_101_to_150, 
			sum(case when txn_sms_consumed>150 then txn_sms_consumed else 0 end) txn_sms_consumed_more_than_150
		from 
			(select mobile_no, count(id) txn_sms_consumed
			from data_vajapora.help_a 
			where 
				sms_type!='tagada'
				and left(sms_date::text, 7)=(select sms_month from data_vajapora.help_c where seq=var_seq)
			group by 1 
			) tbl1 
			
			inner join 
						
			data_vajapora.help_b tbl2 using(mobile_no);
		
		-- tagada SMS: merchants
		insert into data_vajapora.reg_tagada_sms_monthly_distributions
		select 
			(select sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			count(tbl1.mobile_no) merchants_consumed_tagada_sms, 
			count(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_1_to_10, 
			count(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_11_to_20, 
			count(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_21_to_50, 
			count(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_51_to_100, 
			count(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_101_to_150, 
			count(case when tagada_sms_consumed>150 then tbl1.mobile_no else null end) merchants_consumed_tagada_sms_more_than_150
		from 
			(select mobile_no, count(id) tagada_sms_consumed
			from data_vajapora.help_a 
			where 
				sms_type='tagada'
				and left(sms_date::text, 7)=(select sms_month from data_vajapora.help_c where seq=var_seq)
			group by 1 
			) tbl1 
			
			inner join 
						
			data_vajapora.help_b tbl2 using(mobile_no); 
		
		-- tagada SMS: messages
		insert into data_vajapora.reg_tagada_sms_monthly_distributions_2 
		select 
			(select sms_month from data_vajapora.help_c where seq=var_seq) year_month, 
			sum(tagada_sms_consumed) tagada_sms_consumed, 
			sum(case when tagada_sms_consumed>=1 and tagada_sms_consumed<=10 then tagada_sms_consumed else 0 end) tagada_sms_consumed_1_to_10, 
			sum(case when tagada_sms_consumed>10 and tagada_sms_consumed<=20 then tagada_sms_consumed else 0 end) tagada_sms_consumed_11_to_20, 
			sum(case when tagada_sms_consumed>20 and tagada_sms_consumed<=50 then tagada_sms_consumed else 0 end) tagada_sms_consumed_21_to_50, 
			sum(case when tagada_sms_consumed>50 and tagada_sms_consumed<=100 then tagada_sms_consumed else 0 end) tagada_sms_consumed_51_to_100, 
			sum(case when tagada_sms_consumed>100 and tagada_sms_consumed<=150 then tagada_sms_consumed else 0 end) tagada_sms_consumed_101_to_150, 
			sum(case when tagada_sms_consumed>150 then tagada_sms_consumed else 0 end) tagada_sms_consumed_more_than_150
		from 
			(select mobile_no, count(id) tagada_sms_consumed
			from data_vajapora.help_a 
			where 
				sms_type='tagada'
				and left(sms_date::text, 7)=(select sms_month from data_vajapora.help_c where seq=var_seq)
			group by 1 
			) tbl1 
			
			inner join 
						
			data_vajapora.help_b tbl2 using(mobile_no);
		
		commit; 
		raise notice 'Data generated for: %', (select sms_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c) then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.reg_txn_sms_monthly_distributions
order by 1; 

select * 
from data_vajapora.reg_txn_sms_monthly_distributions_2
order by 1;

select * 
from data_vajapora.reg_tagada_sms_monthly_distributions
order by 1; 

select * 
from data_vajapora.reg_tagada_sms_monthly_distributions_2
order by 1;