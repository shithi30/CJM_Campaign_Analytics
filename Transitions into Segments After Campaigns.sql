/*
- Viz: 
	- pct: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1693909689
	- numbers: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=304281899
	- gross: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=661623368
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

-- segment to segment transitions
select before_campaign_tg, after_campaign_tg, merchants_migrated, merchants_migrated*1.00/merchants_in_tg_before_campaign merchants_migrated_pct
from 
	(select 
		before_campaign_tg, 
		case when after_campaign_tg is null then 'uninstalled' else after_campaign_tg end after_campaign_tg, 
		count(mobile_no) merchants_migrated
	from 
		(select mobile_no, tg before_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-02'
		) tbl1 
		
		left join 
		
		(select mobile_no, tg after_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-09'
		) tbl2 using(mobile_no)
	group by 1, 2
	order by 1, 2
	) tbl1 
	
	inner join 
	
	(select tg before_campaign_tg, count(*) merchants_in_tg_before_campaign
	from cjm_segmentation.retained_users 
	where report_date='2021-11-02'
	group by 1
	) tbl2 using(before_campaign_tg); 

-- before count = after count + uninstalled
select *
from 
	(-- before
	select tg, count(mobile_no) merchants_before
	from cjm_segmentation.retained_users 
	where report_date='2021-11-02'
	group by 1
	) tbl1 
	
	left join 
		
	(-- after
	select tg, count(tbl2.mobile_no) merchants_after
	from 
		(select mobile_no
		from cjm_segmentation.retained_users 
		where report_date='2021-11-02'
		) tbl1 
		
		left join 
		
		(select mobile_no, tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-09'
		) tbl2 using(mobile_no)
	group by 1
	) tbl2 using(tg); 

-- gross numbers non-campaign
select before_campaign_tg, after_campaign_tg, merchants_migrated, merchants_migrated*1.00/merchants_in_tg_before_campaign merchants_migrated_pct
from 
	(select 
		before_campaign_tg, 
		case when after_campaign_tg is null then 'uninstalled' else after_campaign_tg end after_campaign_tg, 
		count(mobile_no) merchants_migrated
	from 
		(select mobile_no, tg before_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-02'
		) tbl1 
		
		left join 
		
		(select mobile_no, tg after_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-09'
		) tbl2 using(mobile_no)
	group by 1, 2
	order by 1, 2
	) tbl1 
	
	inner join 
	
	(select tg before_campaign_tg, count(*) merchants_in_tg_before_campaign
	from cjm_segmentation.retained_users 
	where report_date='2021-11-02'
	group by 1
	) tbl2 using(before_campaign_tg)
where merchants_migrated*1.00/merchants_in_tg_before_campaign>0.05; 

-- gross numbers campaign
select before_campaign_tg, after_campaign_tg, merchants_migrated, merchants_migrated*1.00/merchants_in_tg_before_campaign merchants_migrated_pct
from 
	(select 
		before_campaign_tg, 
		case when after_campaign_tg is null then 'uninstalled' else after_campaign_tg end after_campaign_tg, 
		count(mobile_no) merchants_migrated
	from 
		(select mobile_no, tg before_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-10'
		) tbl1 
		
		left join 
		
		(select mobile_no, tg after_campaign_tg
		from cjm_segmentation.retained_users 
		where report_date='2021-11-17'
		) tbl2 using(mobile_no)
	group by 1, 2
	order by 1, 2
	) tbl1 
	
	inner join 
	
	(select tg before_campaign_tg, count(*) merchants_in_tg_before_campaign
	from cjm_segmentation.retained_users 
	where report_date='2021-11-10'
	group by 1
	) tbl2 using(before_campaign_tg)
where merchants_migrated*1.00/merchants_in_tg_before_campaign>0.05; 

