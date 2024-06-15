/*
- Viz: https://docs.google.com/spreadsheets/d/1NQ17yJjfX3j9ubvatCGh_JWl4urwwGtQBH4xmIoga0I/edit?pli=1#gid=1858248188
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): About 20% users are deprived of Aha Moment, daily
*/

do $$

declare
	var_date date;
begin
	-- sequence Jer customers against each merchant
	drop table if exists data_vajapora.jer_cust_seq; 
	create table data_vajapora.jer_cust_seq as
	select 
		mobile_no merchant_mobile, contact jer_cust_contact, create_date::timestamp jer_cust_add_datetime, start_balance jer,
		row_number() over(partition by mobile_no order by create_date::timestamp asc) jer_cust_seq
	from public.account 
	where 
		type=2
		and start_balance!=0
		and left(contact, 3) not in('010', '011', '012');
	
	-- bring details of registrtation
	drop table if exists data_vajapora.reg_details;
	create table data_vajapora.reg_details as
	select *
	from 
		(select mobile mobile_no, shop_name 
		from tallykhata.tallykhata_user_personal_info
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, created_at reg_datetime
		from public.register_usermobile 
		) tbl2 using(mobile_no); 
				
	-- dates to calculate Aha-SMS metrics for
	if current_date-7<'2021-05-12' then var_date:='2021-05-12';
	else var_date=current_date-7;
	end if; 

	-- detete if Aha-SMS metrics already exist for said dates
	delete from data_vajapora.daily_aha_sms_info 
	where date(reg_datetime)>=var_date; 

	-- generate Aha-SMS metrics for said dates
	raise notice 'New OP goes below:'; 
	loop
		raise notice 'Generating data for: %', var_date;
	
		insert into data_vajapora.daily_aha_sms_info 
		select 
			reg_datetime, mobile_no, shop_name, 
			jer_cust_seq, jer_cust_contact, jer, jer_cust_add_datetime, 
			aha_sms_datetime, aha_sms_merchant_mobile, aha_sms_id, aha_sms, length(aha_sms) aha_sms_length
		from 
			(select reg_datetime, mobile_no, shop_name 
			from data_vajapora.reg_details
			where date(reg_datetime)=var_date
			) tbl1
			
			left join 
			
			(select *
			from data_vajapora.jer_cust_seq 
			where jer_cust_seq in(1, 2, 3, 4, 5)
			) tbl3 on(tbl1.mobile_no=tbl3.merchant_mobile)
		
			left join 
			
			(select mobile_no cust_contact, request_time aha_sms_datetime, id aha_sms_id, message_body aha_sms, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') aha_sms_merchant_mobile
			from public.t_scsms_message_archive_v2
			where 
				left(message_body, 28)='প্রিয় গ্রাহক, আপনার মোট বাকি'
				and message_status not in('FAILED')
			) tbl4 on(tbl3.jer_cust_contact=tbl4.cust_contact and tbl3.merchant_mobile=tbl4.aha_sms_merchant_mobile);
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

	-- drop auxiliary table(s)
	drop table if exists data_vajapora.jer_cust_seq; 	
	drop table if exists data_vajapora.reg_details; 
end $$; 

-- mother table for Aha-SMS metrics
select *
from data_vajapora.daily_aha_sms_info; 

-- day-to-day Aha-SMS metrics
select *
from 
	(select date(reg_datetime) reg_date, count(distinct mobile_no) regs
	from data_vajapora.daily_aha_sms_info
	group by 1
	) tbl1 
	
	inner join 
	
	(select 
		date(reg_datetime) reg_date, 
		count(distinct case when jer_cust_contact is not null then mobile_no else null end) fst_jer_cust_users,
		count(distinct case when jer_cust_contact is not null and aha_sms_datetime is not null then mobile_no else null end) fst_jer_cust_users_got_aha,
		count(distinct case when jer_cust_contact is not null and aha_sms_datetime is null then mobile_no else null end) fst_jer_cust_users_got_no_aha,
		count(distinct case when jer_cust_contact is not null and aha_sms_datetime is null then mobile_no else null end)*1.00/
		count(distinct case when jer_cust_contact is not null then mobile_no else null end) fst_jer_cust_users_got_no_aha_pct
		/*count(distinct jer_cust_contact) fst_jer_custs, 
		count(distinct case when aha_sms_datetime is not null then jer_cust_contact else null end) fst_jer_custs_got_aha_sms,
		count(distinct case when aha_sms_datetime is null then jer_cust_contact else null end) fst_jer_custs_got_no_aha_sms,
		count(distinct case when aha_sms_datetime is null then jer_cust_contact else null end)*1.00/
		count(distinct jer_cust_contact) jer_custs_got_no_aha_sms_pct,*/
	from data_vajapora.daily_aha_sms_info
	where jer_cust_seq=1
	group by 1
	) tbl2 using(reg_date)
	
	inner join 
	
	(select 
		date(aha_sms_datetime) reg_date, 
		count(aha_sms_id) aha_sms_sent,
		ceil(avg(aha_sms_length)) avg_aha_sms_length
	from data_vajapora.daily_aha_sms_info
	where jer_cust_seq=1
	group by 1
	) tbl3 using(reg_date)
order by 1 desc; 

-- cases where Aha-SMS were not sent
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and jer_cust_contact is not null 
	and aha_sms_datetime is null;

-- verify that, cases where Aha-SMS were not sent, have been identified correctly
select *
from 
	(-- cases where Aha-SMS were not sent
	select *
	from data_vajapora.daily_aha_sms_info
	where 
		jer_cust_seq=1
		and jer_cust_contact is not null 
		and aha_sms_datetime is null
	) tbl1 
	
	inner join 
	
	(select mobile_no jer_cust_contact, translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') aha_sms_merchant_mobile
	from public.t_scsms_message_archive_v2
	where 
		left(message_body, 28)='প্রিয় গ্রাহক, আপনার মোট বাকি'
		and message_status not in('FAILED')
	) tbl2 using(jer_cust_contact, aha_sms_merchant_mobile); 

-- cases where Aha-SMS were not sent: most cases exhibit adding cust. prior to reg. 
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and reg_datetime>jer_cust_add_datetime
	and jer_cust_contact is not null 
	and aha_sms_datetime is null;

-- cases where Aha-SMS were sent
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and jer_cust_contact is not null 
	and aha_sms_datetime is not null; 

-- cases where Aha-SMS were sent, but not on the day of reg. (for users who added customers on a day other than their reg. day)
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and date(reg_datetime)<date(aha_sms_datetime); 

-- cases where Aha-SMS were sent, but before the customer was added (DB latency)
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and jer_cust_add_datetime>aha_sms_datetime; 

-- cases where customers received multiple Aha-SMS from the same merchant (possible reasons: user removed+readded/edited Jer, SMS sent multiple times, SMS failed) 
select tbl1.*, aha_sms_sent
from 
	(select *
	from data_vajapora.daily_aha_sms_info
	where 
		jer_cust_seq=1
		and jer_cust_contact is not null 
		and aha_sms_datetime is not null
	) tbl1 
	
	inner join 
		
	(select mobile_no, jer_cust_contact, count(aha_sms_datetime) aha_sms_sent 
	from data_vajapora.daily_aha_sms_info
	where 
		jer_cust_seq=1
		and jer_cust_contact is not null 
		and aha_sms_datetime is not null
	group by 1, 2
	having count(aha_sms_datetime)>1
	) tbl2 using(mobile_no, jer_cust_contact)
order by jer_cust_contact, aha_sms_datetime; 

-- cases where Aha-SMS were experimentally sent to self
select *
from data_vajapora.daily_aha_sms_info
where 
	jer_cust_seq=1
	and jer_cust_contact is not null 
	and aha_sms_datetime is not null
	and mobile_no=jer_cust_contact; 

select 
	(-- self
	select count(distinct mobile_no) 
	from data_vajapora.daily_aha_sms_info
	where 
		jer_cust_seq=1
		and jer_cust_contact is not null 
		and aha_sms_datetime is not null
		and mobile_no=jer_cust_contact
	)*1.00/
	(-- all (other custs+self)
	select count(distinct mobile_no) 
	from data_vajapora.daily_aha_sms_info
	where 
		jer_cust_seq=1
		and jer_cust_contact is not null 
		and aha_sms_datetime is not null
	) aha_merchant_self_pct; 

-- cases where the same custs were added multiple times, causing mother-table to show n-th customers getting Aha-SMS as well
select *
from 
	(select *
	from data_vajapora.daily_aha_sms_info
	) tbl1 
	
	inner join 
	
	(select aha_sms_id, count(aha_sms_id) entries
	from data_vajapora.daily_aha_sms_info
	group by 1
	having count(aha_sms_id)>1
	) tbl2 using(aha_sms_id) 
order by aha_sms_id, jer_cust_seq, aha_sms_datetime; 

-- cases where a single cust. was added by multiple merchants as first Jer cust.
select tbl1.*
from 
	(select *
	from data_vajapora.daily_aha_sms_info
	where jer_cust_seq=1
	) tbl1
	
	inner join 
	
	(select jer_cust_contact, count(mobile_no) merchants
	from data_vajapora.daily_aha_sms_info
	where jer_cust_seq=1
	group by 1 
	having count(distinct mobile_no)>1
	) tbl2 using(jer_cust_contact)
order by jer_cust_contact; 

/*
Additional Analytical Scopes:
- if updated people too are getting Aha-moments
- how much time it takes to give Aha-moment
*/

