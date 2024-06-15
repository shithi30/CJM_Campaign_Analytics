/*
- Viz: 
	- version-01: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=401986140
	- version-02: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=482821243
- Data: 
- Function: 
- Table:
- Instructions: 
	> Number of monthly transacting user
	> Distribute the MTU by their first transacting activity of the month.
	Note please consider [ Add Customer, Supplier Add ] as transacting activity.
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

/*version-01*/
do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_month asc) seq 
	from 
		(select left(tagada_date::text, 7) txn_month
		from 
			(select generate_series(0, current_date-'2022-08-01'::date, 1)+'2022-08-01'::date tagada_date
			) tbl1 
		group by 1
		) tbl1; 

	loop 
		delete from data_vajapora.txn_mau_stats
		where to_char(first_txn_date, 'YYYY-MM')=(select txn_month from data_vajapora.help_c where seq=var_seq);
		
		insert into data_vajapora.txn_mau_stats
		select 
			mobile_no, 
			min(created_timestamp) first_txn_datetime, 
			min(created_datetime) first_txn_date 
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=(select txn_month from data_vajapora.help_c where seq=var_seq)
		group by 1; 

		commit; 
		raise notice 'Data generated for: %', (select txn_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_mau_stats; 

-- to pivot
select 
	to_char(first_txn_date, 'YYYY-MM') mtu_month, 
	date_part('day', first_txn_date) first_txn_day_month, 
	count(1) txn_mau
from data_vajapora.txn_mau_stats
where first_txn_date<current_date
group by 1, 2 
order by 1, 2; 

-- cumulative
with 
	temp_table as
	(select 
		date_part('day', first_txn_date) first_txn_day_month, 
		count(1) txn_mau
	from data_vajapora.txn_mau_stats
	where to_char(first_txn_date, 'YYYY-MM')='2022-09'
	group by 1
	) 
	
select 
	tbl1.first_txn_day_month, 
	tbl1.txn_mau, 
	sum(tbl2.txn_mau) txn_mau_cumulative, 
	sum(tbl2.txn_mau)/(select sum(txn_mau) from temp_table) txn_mau_cumulative_pct
from 
	temp_table tbl1 
	inner join 
	temp_table tbl2 on(tbl2.first_txn_day_month<=tbl1.first_txn_day_month) 
group by 1, 2 
order by 1; 

/*version-02*/
do $$ 

declare 
	var_seq int:=1; 
begin 
	raise notice 'New OP goes below:'; 
	
	-- months for statistics
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select *, row_number() over(order by txn_month asc) seq 
	from 
		(select left(txn_date::text, 7) txn_month
		from 
			(select generate_series(0, current_date-'2022-08-01'::date, 1)+'2022-08-01'::date txn_date
			) tbl1 
		group by 1
		) tbl1; 

	loop 
		delete from data_vajapora.txn_mau_stats_2
		where yr_month=(select txn_month from data_vajapora.help_c where seq=var_seq);
		delete from data_vajapora.txn_mau_stats_3
		where yr_month=(select txn_month from data_vajapora.help_c where seq=var_seq);
		
		-- first txn
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as 
		select mobile_no, min(auto_id) auto_id 
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=(select txn_month from data_vajapora.help_c where seq=var_seq)
		group by 1; 
		
		-- first txn type
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as 
		select 
			mobile_no, 
			auto_id, 
			case 
				when txn_type like 'Add %' then 'add_cust_supp'
				when txn_type like 'CREDIT_%' then 'credit_txn'
				else 'cash_txn'
			end first_txn_type
		from 
			data_vajapora.help_a tbl1 
			inner join 
			(select auto_id, txn_type 
			from tallykhata.tallykhata_fact_info_final 
			) tbl2 using(auto_id); 
		
		-- first txn distribution
		insert into data_vajapora.txn_mau_stats_2
		select 
			(select txn_month from data_vajapora.help_c where seq=var_seq) yr_month, 
			first_txn_type, 
			count(mobile_no) mtus
		from data_vajapora.help_b 
		group by 1, 2; 
	
		-- all txn
		drop table if exists data_vajapora.help_d; 
		create table data_vajapora.help_d as 
		select mobile_no, txn_type 
		from tallykhata.tallykhata_fact_info_final 
		where to_char(created_datetime, 'YYYY-MM')=(select txn_month from data_vajapora.help_c where seq=var_seq); 
		
		-- all txn merchants
		insert into data_vajapora.txn_mau_stats_3
		select 
			(select txn_month from data_vajapora.help_c where seq=var_seq) yr_month,
			count(distinct mobile_no) mtu,
			count(distinct case when txn_type like 'Add %' then mobile_no else null end) mtu_cust_supp_add,
			count(distinct case when txn_type like 'CREDIT_%' then mobile_no else null end) mtu_credit, 
			count(distinct case when txn_type not like 'CREDIT_%' and txn_type not like 'Add %' then mobile_no else null end) mtu_cash
		from data_vajapora.help_d;

		commit; 
		raise notice 'Data generated for: %', (select txn_month from data_vajapora.help_c where seq=var_seq); 
		var_seq:=var_seq+1; 
		if var_seq=(select max(seq) from data_vajapora.help_c)+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.txn_mau_stats_2;

select * 
from data_vajapora.txn_mau_stats_3;

/*further understanding*/

-- added cust/supp but did no txn
select count(distinct mobile_no) added_but_notxn 
from 
	(select mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where 
		entry_type=2
		and to_char(created_datetime, 'YYYY-MM')='2022-09'
	) tbl1 
	
	left join 
	
	(select mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where 
		entry_type=1
		and to_char(created_datetime, 'YYYY-MM')='2022-09'
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

-- did cash txn but no credit txn
select count(distinct mobile_no) added_but_notxn 
from 
	(select mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type in
			('MALIK_DILO',
			'CASH_PURCHASE',
			'CASH_SALE',
			'CASH_ADJUSTMENT',
			'DIGITAL_CASH_SALE',
			'EXPENSE',
			'MALIK_NILO')
		and to_char(created_datetime, 'YYYY-MM')='2022-09'
	) tbl1 
	
	left join 
	
	(select mobile_no 
	from tallykhata.tallykhata_fact_info_final 
	where 
		txn_type like 'CREDIT%'
		and to_char(created_datetime, 'YYYY-MM')='2022-09'
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

/*struggle to find first entry*/

-- journal
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, min(created_timestamp) first_txn_time
from tallykhata.tallykhata_fact_info_final 
where 
	-- to_char(created_datetime, 'YYYY-MM')='2022-08'
	created_datetime=current_date-7
	and entry_type=1
group by 1; 

-- account
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile_no, min(created_timestamp) first_txn_time
from tallykhata.tallykhata_fact_info_final 
where 
	-- to_char(created_datetime, 'YYYY-MM')='2022-08'
	created_datetime=current_date-7
	and entry_type=2
group by 1; 

-- combined
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select * from data_vajapora.help_a tbl1 
union all 
select * from data_vajapora.help_b tbl2; 

-- find multiple entries (?)
select mobile_no, first_txn_time, count(*) txns
from 
	(select mobile_no, min(first_txn_time) first_txn_time 
	from data_vajapora.help_c 
	group by 1
	) tbl1 
	
	inner join 
	
	data_vajapora.help_c tbl2 using(mobile_no, first_txn_time) 
group by 1, 2 
order by 3 desc;
