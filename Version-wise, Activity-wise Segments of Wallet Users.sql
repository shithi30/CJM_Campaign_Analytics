/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1efq81FGu_VhhHQMANk_gR5vJNjtVZ38CNCzXtSfPSHc/edit#gid=0
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Version-05 Segments and Activities
- Notes (if any): 
*/

-- bring from live data_vajapora.version_info
select mobile_no, app_version_number, app_version_name 
from 
	(select mobile mobile_no, max(id) id 
	from public.registered_users
	group by 1 
	) tbl1 
	
	inner join 
	
	(select id, app_version_name, app_version_number
	from public.registered_users
	) tbl2 using(id) 
where app_version_number>105; 

-- bring from NP DWH nobopay_dwh data_vajapora.act_info_1
-- add money 
select distinct mobile_no, 'add money' act_type
from 
	backend_db.np_txn_log tbl1 
	inner join 
	(select id to_id, owner_name mobile_no 
	from nobopay_core.accounts 
	) tbl2 using(to_id)
where 
	txn_type in ('CASH_IN_FROM_BANK', 'CASH_IN_FROM_CARD')
	and status='COMPLETE'
	
union all
	
-- payment  
select distinct mobile_no, 'payment' act_type
from 
	backend_db.np_txn_log tbl1 
	inner join 
	(select id from_id, owner_name mobile_no 
	from nobopay_core.accounts 
	) tbl2 using(from_id)
where 
	txn_type in ('PAYMENT', 'IDTP_PAYMENT')
	and status='COMPLETE'
	
union all
	
-- recharge  
select distinct mobile_no, 'mobile recharge' act_type
from 
	backend_db.np_txn_log tbl1 
	inner join 
	(select id to_id, owner_name mobile_no 
	from nobopay_core.accounts 
	) tbl2 using(to_id)
where 
	txn_type in ('MOBILE_RECHARGE')
	and status='COMPLETE'; 

-- bring from NP DWH ods data_vajapora.act_info_2
-- wallet open 
select distinct p.wallet_no mobile_no, 'wallet open' act_type
from ods_tp.backend_db__profile p 
left join ods_tp.backend_db__document as d on p.user_id  = d.user_id 
left join ods_tp.backend_db__bank_account  as a on p.user_id  = a.user_id
left join ods_tp.backend_db__mfs_account as m on p.user_id  = m.user_id
where 1=1
and upper(d.doc_type) ='NID'
and p.created_at::date>='2022-09-21'
and p.bank_account_status = 'VERIFIED'; 

-- combined activities
drop table if exists data_vajapora.act_info; 
create table data_vajapora.act_info as
select * from data_vajapora.act_info_1 
union all
select * from data_vajapora.act_info_2;

-- version wise segments
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select *
from 
	data_vajapora.version_info tbl1 
	
	left join 

	(select 
		mobile_no, 
		max(
			case 
				when tg like '3RAU%' then '3RAU'
				when tg like 'LTU%' then 'LTU'
				when tg like 'PU%' then 'PU'
				when tg like 'Z%' then 'Zombie' 
				when tg in('NT--') then 'NT'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('PSU') then 'PSU'
				when tg in('SPU') then 'SU'
				else null
			end
		) segment
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl2 using(mobile_no); 

select 
	app_version_name, 
	case when segment is null then 'uninstalled' else segment end segment, 
	count(mobile_no) merchants
from data_vajapora.help_b 
group by 1, 2; 

-- version-01: activity wise segments
select 
	case when segment is null then 'uninstalled' else segment end segment,
	count(case when act_wallet_open is not null then mobile_no else null end) opened_wallet,
	count(case when act_payment is not null then mobile_no else null end) received_payment,
	count(case when act_add_money is not null then mobile_no else null end) added_money,
	count(case when act_mobile_recharge is not null then mobile_no else null end) recharged_mobile 
from 	
	(select mobile_no, act_type act_wallet_open 
	from data_vajapora.act_info 
	where act_type='wallet open'
	) tbl1
	
	left join 
	
	(select mobile_no, act_type act_payment
	from data_vajapora.act_info 
	where act_type='payment'
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile_no, act_type act_add_money
	from data_vajapora.act_info 
	where act_type='add money'
	) tbl3 using(mobile_no)
	
	left join 
	
	(select mobile_no, act_type act_mobile_recharge
	from data_vajapora.act_info 
	where act_type='mobile recharge'
	) tbl4 using(mobile_no)
	
	left join 

	(select 
		mobile_no, 
		max(
			case 
				when tg like '3RAU%' then '3RAU'
				when tg like 'LTU%' then 'LTU'
				when tg like 'PU%' then 'PU'
				when tg like 'Z%' then 'Zombie' 
				when tg in('NT--') then 'NT'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('PSU') then 'PSU'
				when tg in('SPU') then 'SU'
				else null
			end
		) segment
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl5 using(mobile_no) 
group by 1; 

-- version-02: activity wise segments
select 
	case when segment is null then 'uninstalled' else segment end segment, 
	act_type, 
	count(*) txns
from 
	(select 
		wallet_no mobile_no, 
		case 
			when txn_type like 'CASH_IN_%' then 'add money' 
			when txn_type in('CREDIT_COLLECTION') then 'receive payment'
			when txn_type in('MOBILE_RECHARGE') then 'mobile recharge'
			else null
		end act_type 
	from data_vajapora.t_marketing_report -- temp. table by Nazrul Bh. 
	where 
		1=1
		-- and status='COMPLETE'
		and txn_type in('CASH_IN_FROM_BANK', 'CASH_IN_FROM_CARD', 'CREDIT_COLLECTION', 'MOBILE_RECHARGE')
		
	union all 
	
	select mobile_no, 'wallet open' act_type  
	from data_vajapora.act_info 
	where act_type='wallet open'
	) tbl1 
	
	left join 
	
	(select 
		mobile_no, 
		max(
			case 
				when tg like '3RAU%' then '3RAU'
				when tg like 'LTU%' then 'LTU'
				when tg like 'PU%' then 'PU'
				when tg like 'Z%' then 'Zombie' 
				when tg in('NT--') then 'NT'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('PSU') then 'PSU'
				when tg in('SPU') then 'SU'
				else null
			end
		) segment
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl2 using(mobile_no) 
group by 1, 2;  