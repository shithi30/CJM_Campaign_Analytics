/*
1.
We have started a referral campaign on yesterday through In-App. Could you update on us? Here is the campaign details:

In-App Message ID: 2726 
CTA Link: com.progoti.tallykhata.v2.activities.ReferralActivity
Referral Link: Automatically generated from TK App
TG: 538K (Lifetime PU & Transacting Users)

Required Updates:
1. How many delivered?
2. How many displayed?
3. How many CTA clicked?
4. How many users referral link shared?(From App)
5. How many referred?
6. How many users app installed through referral link?

Please note the campaign message IDs;
7-Feb-2022 In-App Message ID: 2726
8-Feb-2022 In-App Message ID: 2727

2.
Referral Campaign Message Details:

7 Feb 2022
RL220206-01	
Message ID: 2726	
টালিখাতা অ্যাপ!📱⚡️	
আপনার বন্ধুদের এই অ্যাপে হিসাব রাখতে পরামর্শ দিন।🤝

8 Feb 2022
RL220208-01		
2727	
টালিখাতা অ্যাপ!📱⚡️	
আপনার ব্যবসায়ী বন্ধুদের এই অ্যাপে হিসাব রাখতে পরামর্শ দিন।🤝

9 Feb 2022
RL220209-01
2739	
টালিখাতা অ্যাপ!📱⚡️	
আপনার ব্যবসায়ী বন্ধুদের এই অ্যাপে হিসাব রাখতে পরামর্শ দিন।🤝

3.
in_app_message_open
in_app_message_close
in_app_message_link_tap

refer_button_pressed
copy_refer_link
refer
*/

select *
from 
	public.sync_appevent tbl1
	
	left join 
	
	(select id notification_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message                 
	from public.notification_pushmessage
	) tbl2 using(notification_id)
where 
	tallykhata_user_id in(select tallykhata_user_id from public.register_usermobile where mobile_number='01684311672')
	and date(created_at)>=current_date-10 and date(created_at)<current_date
	and bulk_notification_id in 
		(select request_id
		from 
			(select request_id, schedule_time start_datetime, date(schedule_time) start_date
		    from public.notification_bulknotificationsendrequest
		    ) tbl1 
		
		    inner join 
		
		    (select id request_id, title campaign_id
		    from public.notification_bulknotificationrequest
		    ) tbl2 using(request_id) 
		where campaign_id in('RL220206-01', 'RL220208-01', 'RL220209-01')
		) 
order by created_at desc; 

select *
from tallykhata.tallykhata_sync_event_fact_final 
where 
	notification_id in(2726, 2727, 2739)
	and event_date>=current_date-10 and event_date<current_date; 

/*
select *
from 
	(select id notification_id, case when title is null then '[message shot in version < 4.0.1]' else regexp_replace(concat(title, ' ', summary), E'[\\n\\r]+', ' ', 'g' ) end message                 
	from public.notification_pushmessage
	) tbl2 
where message like '%বন্ধুদের%';
*/ 
