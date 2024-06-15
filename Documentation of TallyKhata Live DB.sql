/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: https://docs.google.com/spreadsheets/d/1xtJ1aSdvhQGK9ClWiawdaypYSGMY1VVMdTFRGd5VF8o/edit#gid=0
- Email thread: 
- Notes (if any): 

Did not get permission to:
	- public.django_celery_beat_clockedschedule
	- public.django_celery_beat_crontabschedule
	- public.django_celery_beat_intervalschedule
	- public.django_celery_beat_periodictask
	- public.django_celery_beat_periodictasks
	- public.django_celery_beat_solarschedule
	- public.google_ad_id
	- public.notification_bulknotificationfile
	- public.notification_bulknotificationreceiver
	- public.notification_bulknotificationrequest
	- public.notification_bulknotificationrequest_receiving_tags
	- public.notification_bulknotificationschedule
	- public.notification_bulknotificationsendcount
	- public.notification_bulknotificationsendrequest
	- public.notification_popupmessage
	- public.notification_tagadasms
	- public.notification_uninstalltracking
	- public.register_referral
	- public.register_smsquota
	- public.register_tag
	- public.register_tallykhataaccount
	- public.register_tallykhatauser_tag_list
	- public.register_unverifieduserapp
	- public.sync_appevent
	- public.sync_appeventunverified
	- public.sync_resynctrigger
	- public.sync_unverifieduseraccount
	- public.sync_unverifieduserjournal
*/

-- for live
select * from
public.v1_data_recovery_from_request_log -- drag repeatedly here
order by random()
limit 1000; 
