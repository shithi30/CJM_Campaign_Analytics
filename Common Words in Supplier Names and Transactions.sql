/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/11O-xV7m_MdXifXwAwX_uQz3DtGJkFu7fA7TMLzSSmXo/edit?usp=sharing
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 

What kind of data are available TK users' supplier name or transaction detail? 
What are the common strings or phrases used by MMs?

Sir, we have analyzed data of supplier-names and MMs' transaction details with the said suppliers. 
- Popular terms used in transactions: জমা, cash, নগদ, টাকা, bKash, বাবদ, bank etc.
- Popular terms found in supplier-names: ভাই, mama, kaka, দোকান, ltd, বাজার, store etc.

Pls publish top 1000 list. 
Are these terms in suppliers names or full supplier names? We expect to see some more company or product names such as Coke, Pepsi, Pran, Wheel, Maggie, Chips, Biscuit etc. 

Sir, these are derived from all the info we have about any supplier-name. 
We have now mined supplier-names of last 3 months' grocery MMs and found: 
- companies: প্রান, স্টার, ফ্রেশ, বসুন্ধরা, আকিজ, অলিম্পিক, ইস্পাহানি, ইউনিলিভার, danish, এসিআই, স্কয়ার, তিব্বত, গ্রামীণ, ডানো, rupchanda etc.
- FMCG: বিস্কুট, রুটি, সিগারেট, আইসক্রিম, চিপস, কয়েল, চানাচুর, পাউডার, চাল, চা etc. 
*/



/* descriptions */

-- last 7 days' descriptions with suppliers
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, account_id, description, to_tsvector(description) description_vector
	from public.journal 
	where 
		description is not null
		and description!=''
		and date(create_date)>=current_date-7
	) tbl1 
	
	inner join 
	
	(select id account_id, mobile_no
	from public.account
	where type=3
	) tbl2 using(mobile_no, account_id); 

/*
-- view data
select *
from data_vajapora.help_a
order by random()
limit 10000; 

select count(*) 
from  data_vajapora.help_a; 
*/

-- view statistics
select row_number() over(order by nentry desc, ndoc desc) serial, word, ndoc found_in_descriptions, nentry times_found
from ts_stat('SELECT description_vector FROM data_vajapora.help_a')  
where 
	word !~ '^[0-9\.+-]+$'
	and word !~ '^[০-৯\.+-]+$'
	and length(word)!=1
order by nentry desc, ndoc desc
limit 100;  



/* names */

-- last 180 days' suppliers' names
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select concat(name, ' ', description) description, to_tsvector(concat(name, ' ', description)) description_vector
from public.account
where 
	type=3
	and concat(name, ' ', description) is not null
	and concat(name, ' ', description)!=''
	and date(create_date)>=current_date-180;  

/*-- view data
select *
from data_vajapora.help_b
order by random()
limit 10000; 

select count(*) 
from  data_vajapora.help_b; 
*/

-- view statistics
select row_number() over(order by nentry desc, ndoc desc) serial, word, ndoc found_in_descriptions, nentry times_found
from ts_stat('SELECT description_vector FROM data_vajapora.help_b')  
where 
	word !~ '^[0-9\.+-]+$'
	and word !~ '^[০-৯\.+-]+$'
	and length(word)!=1
order by nentry desc, ndoc desc
limit 100;  



/* names: grocery of last 180 days */ 

drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select *
from 
	(select distinct mobile_no, contact
	from tallykhata.tallykhata_fact_info_final 
	where 
		created_datetime>=current_date-180
		and txn_type in('CREDIT_PURCHASE_RETURN', 'CASH_PURCHASE', 'CREDIT_PURCHASE')
	) tbl1 
	
	inner join 
	
	(select mobile_no, contact, concat(name, ' ', description) description, to_tsvector(concat(name, ' ', description)) description_vector
	from public.account
	where 
		type=3
		and concat(name, ' ', description) is not null
		and concat(name, ' ', description)!=''
	) tbl2 using(mobile_no, contact)
	
	inner join 
	
	(select mobile mobile_no 
	from tallykhata.tallykhata_user_personal_info 
	where bi_business_type='Grocery Business'
	) tbl3 using(mobile_no); 


/*-- view data
select *
from data_vajapora.help_c
order by random()
limit 10000; 

select count(*) 
from  data_vajapora.help_c; 
*/

-- view statistics
select row_number() over(order by nentry desc, ndoc desc) serial, word, ndoc found_in_descriptions, nentry times_found
from ts_stat('SELECT description_vector FROM data_vajapora.help_c')  
where 
	word !~ '^[0-9\.+-]+$'
	and word !~ '^[০-৯\.+-]+$'
	and length(word)!=1
order by nentry desc, ndoc desc
limit 1000;  

সমিতি
-- প্রান
বেকারী
বিস্কুট
রুটি
-- স্টার
-- ফ্রেশ
-- বসুন্ধরা
-- মোল্লা
সিগারেট
আইসক্রিম
-- আকিজ
-- অলিম্পিক
-- ইস্পাহানি
চিপস
কয়েল
-- ইউনিলিভার
-- danish
-- এসিআই
চানাচুর
পাউডার
-- রয়েল
-- স্কয়ার
-- তিব্বত
-- গ্রামীণ
-- ডানো
-- rupchanda
