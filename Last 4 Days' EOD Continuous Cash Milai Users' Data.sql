/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1sxB47kgTp2T1W8JDBt46KFsG6BgC-5utdoox1W7T_vQ/edit#gid=1116830996
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): for survey calls
*/

-- day-to-day merchants using Cash Milai during last hour
do $$

declare
	var_date date:='2021-09-29';
begin 
	raise notice 'New OP goes below:';

	truncate table data_vajapora.eod_cashbox_stats_2; 

	loop
		insert into data_vajapora.eod_cashbox_stats_2 
		select distinct 
			var_date create_date, 
			tbl1.mobile_no cashbox_adj_at_eod_merchants
		from 
			(select mobile_no, create_date::timestamp
			from public.cashbox_adjustment
			where create_date::date=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_no, max(event_timestamp)-interval '1 hour' last_event_time_minus_1_hr, max(event_timestamp) last_event_time
			from tallykhata.tallykhata_sync_event_fact_final 
			where event_date=var_date
			group by 1
			) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and create_date>=last_event_time_minus_1_hr and create_date<=last_event_time); 
		
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

-- merchants using Cash Milai continuously
drop table if exists test.countinuous_cash_adjustment_users; 
create table test.countinuous_cash_adjustment_users as
select cashbox_adj_at_eod_merchants mobile_no
from data_vajapora.eod_cashbox_stats_2
where create_date>=current_date-4 and create_date<current_date
group by 1
having count(distinct create_date)=4;

-- merchants using Cash Milai continuously, with registration app version
drop table if exists test.countinuous_cash_adjustment_users_help; 
create table test.countinuous_cash_adjustment_users_help as
select 
	b.mobile_number,
	b.tallykhata_user_id,
	c.app_version_name
from test.countinuous_cash_adjustment_users a
inner join public.register_usermobile b on a.mobile_no=b.mobile_number 

inner join

(select 
	tbl_1.mobile_no,app_version_name
from 
	(select 
		mobile_no,
		min(app_version_number) as reg_version
	from data_vajapora.version_wise_days
	group by 1
	) tbl_1
inner join data_vajapora.version_wise_days tbl_2 on tbl_1.mobile_no=tbl_2.mobile_no and  tbl_1.reg_version=tbl_2.app_version_number
) c on a.mobile_no=c.mobile_no;

-- merchants using Cash Milai continuously, with other metrics
drop table if exists test.countinuous_cash_adjustment_users_final; 
create table test.countinuous_cash_adjustment_users_final as
select 
	tbl_1.mobile_number,
	tbl_1.app_version_name as registration_app_version_name,
	tbl_2.division,
	tbl_2.district,
	tbl_2.upazilla,
	tbl_2."union",
	tbl_3.bi_business_type,
	case when tbl_1.mobile_number in (select mobile_no from tallykhata.tk_power_users_10 where report_date=current_date-1) then 1 else 0 end as is_PU,
	tbl_4.cash_sale_txn_days,	
	tbl_4.cash_purchase_txn_days,	
	tbl_4.expense_txn_days,	
	tbl_4.malik_nilo_txn_days,	
	tbl_4.malik_dilo_txn_days,
	tbl_5.cash_sale_txn_days_last_4_days,	
	tbl_5.cash_purchase_txn_days_last_4_days,	
	tbl_5.expense_txn_days_last_4_days,	
	tbl_5.malik_nilo_txn_days_last_4_days,	
	tbl_5.malik_dilo_txn_days_last_4_days
from test.countinuous_cash_adjustment_users_help tbl_1
left join tallykhata.tk_user_location_final tbl_2 on tbl_1.tallykhata_user_id=tbl_2.tallykhata_user_id
left join tallykhata.tallykhata_user_personal_info tbl_3 on tbl_1.mobile_number=tbl_3.mobile
left join 
		(select
			tbl_1.mobile_no,
			count(distinct case when txn_type='CASH_SALE' then created_datetime end) as cash_sale_txn_days,
			count(distinct case when txn_type='CASH_PURCHASE' then created_datetime end) as cash_purchase_txn_days,
			count(distinct case when txn_type='EXPENSE' then created_datetime end) as expense_txn_days,
			count(distinct case when txn_type='MALIK_NILO' then created_datetime end) as malik_nilo_txn_days,
			count(distinct case when txn_type='MALIK_DILO' then created_datetime end) as malik_dilo_txn_days
		from tallykhata.tallykhata_fact_info_final tbl_1
		inner join test.countinuous_cash_adjustment_users tbl_2 on tbl_1.mobile_no=tbl_2.mobile_no
		group by 1
		)tbl_4 on tbl_1.mobile_number=tbl_4.mobile_no
left join 
		(select 
			tbl_1.mobile_no,
			count(distinct case when txn_type='CASH_SALE' then created_datetime end) as cash_sale_txn_days_last_4_days,
			count(distinct case when txn_type='CASH_PURCHASE' then created_datetime end) as cash_purchase_txn_days_last_4_days,
			count(distinct case when txn_type='EXPENSE' then created_datetime end) as expense_txn_days_last_4_days,
			count(distinct case when txn_type='MALIK_NILO' then created_datetime end) as malik_nilo_txn_days_last_4_days,
			count(distinct case when txn_type='MALIK_DILO' then created_datetime end) as malik_dilo_txn_days_last_4_days
		from tallykhata.tallykhata_fact_info_final tbl_1
		inner join test.countinuous_cash_adjustment_users tbl_2 on tbl_1.mobile_no=tbl_2.mobile_no
		where created_datetime>=current_date-4 and created_datetime<current_date
		group by 1) tbl_5 on tbl_1.mobile_number=tbl_5.mobile_no; 

-- data to share
select
	mobile_number, 
	registration_app_version_name, 
	merchant_name, 
	shop_name,
	
	division, 
	district, 
	upazilla, 
	"union",
	
	bi_business_type, 
	is_pu, 
	
	cash_sale_txn_days, 
	cash_purchase_txn_days, 
	expense_txn_days, 
	malik_nilo_txn_days, 
	malik_dilo_txn_days,
	
	cash_sale_txn_days_last_4_days, 
	cash_purchase_txn_days_last_4_days, 
	expense_txn_days_last_4_days, 
	malik_nilo_txn_days_last_4_days, 
	malik_dilo_txn_days_last_4_days,
	
	credit_sales_trv_last_4_days, 
	cash_sales_trv_last_4_days, 
	total_sales_trv_last_4_days,
	
	total_sales_trv_last_7_days
from 
	test.countinuous_cash_adjustment_users_final tbl1
	
	left join 

	(select 
		mobile_no mobile_number, 
		sum(case when txn_type in('CREDIT_SALE') then cleaned_amount else 0 end) credit_sales_trv_last_4_days,
		sum(case when txn_type in('CASH_SALE', 'CASH_ADJUSTMENT') then cleaned_amount else 0 end) cash_sales_trv_last_4_days,
		sum(case when txn_type in('CREDIT_SALE', 'CASH_SALE', 'CASH_ADJUSTMENT') then cleaned_amount else 0 end) total_sales_trv_last_4_days
	from tallykhata.tallykhata_fact_info_final
	where 
		mobile_no in(select mobile_no from test.countinuous_cash_adjustment_users)                                                      
		and created_datetime>=current_date-4 and created_datetime<current_date
	group by 1
	order by 1 desc
	) tbl2 using(mobile_number)
	
	left join 

	(select 
		mobile_no mobile_number, 
		sum(case when txn_type in('CREDIT_SALE', 'CASH_SALE', 'CASH_ADJUSTMENT') then cleaned_amount else 0 end) total_sales_trv_last_7_days
	from tallykhata.tallykhata_fact_info_final
	where 
		mobile_no in(select mobile_no from test.countinuous_cash_adjustment_users)                                                      
		and created_datetime>=current_date-7 and created_datetime<current_date
	group by 1
	order by 1 desc
	) tbl4 using(mobile_number)
	
	left join 
	
	(select mobile mobile_number, merchant_name, shop_name
	from tallykhata.tallykhata_user_personal_info
	) tbl3 using(mobile_number);
