/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1873182741
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

-- all Aha SMS shot
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no cust_contact, 
	request_time aha_sms_datetime, 
	id aha_sms_id, 
	message_body aha_sms, 
	split_part(translate(message_body, '০১২৩৪৫৬৭৮৯,', '0123456789'), ' ', 6)::numeric aha_amount, 
	translate(substring(message_body, length(message_body)-10, 12), '০১২৩৪৫৬৭৮৯', '0123456789') aha_sms_merchant_mobile
from public.t_scsms_message_archive_v2
where 
	left(message_body, 28)='প্রিয় গ্রাহক, আপনার মোট বাকি'
	and message_status not in('FAILED'); 
select *
from data_vajapora.help_a; 

-- edit Jer impact
select *
from 
	(select 
		aha_sms_date, 
		count(distinct aha_sms_merchant_mobile) edit_jer_merchants, 
		count(distinct cust_contact) edit_jer_custs,
		sum(jer_edits)-count(*) additional_aha_sms_sent_due_to_edit_jer
	from 
		(-- date on which customer received first Aha SMS
		select aha_sms_merchant_mobile, cust_contact, date(min(aha_sms_datetime)) aha_sms_date 
		from data_vajapora.help_a
		group by 1, 2
		) tbl1 
		
		inner join 
		
		(-- cases where multiple Jers got detected
		select aha_sms_merchant_mobile, cust_contact, count(distinct aha_amount) jer_edits 
		from data_vajapora.help_a
		group by 1, 2
		having count(distinct aha_amount)>1
		) tbl2 using(aha_sms_merchant_mobile, cust_contact)
	group by 1
	) tbl1 
	
	inner join 
	
	(-- total SMS shot on the day
	select date(aha_sms_datetime) aha_sms_date, count(distinct aha_sms_id) total_aha_sms_shot 
	from data_vajapora.help_a 
	group by 1
	) tbl2 using(aha_sms_date) 
order by 1 asc;
