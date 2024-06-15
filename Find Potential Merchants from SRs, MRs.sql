/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): to find potential users, to find SRs/MRs
*/

-- Part-01: grocery suppliers' descriptive names (suppliers who have transacted in the last 180 days) 
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
	
	(select mobile_no, contact, concat(name, ' ', description) description
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

-- Part-02: look up popular supplier-name patterns from: https://docs.google.com/spreadsheets/d/11O-xV7m_MdXifXwAwX_uQz3DtGJkFu7fA7TMLzSSmXo/edit#gid=0

-- Part-03: find popular shop-name patterns for grocery 
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile mobile_no, concat(shop_name) description, to_tsvector(concat(shop_name)) description_vector
from tallykhata.tallykhata_user_personal_info 
where 
	bi_business_type='Grocery Business'
	and shop_name is not null
	and shop_name!=''; 
	
select row_number() over(order by nentry desc, ndoc desc) serial, word, ndoc found_in_descriptions, nentry times_found
from ts_stat('SELECT description_vector FROM data_vajapora.help_b')  
where 
	word !~ '^[0-9\.+-]+$'
	and word !~ '^[০-৯\.+-]+$'
	and length(word)!=1
order by nentry desc, ndoc desc
limit 100;  

-- results: potential TK merchants
select 
	supplier_contact, 
	supplier_name_details, 
	customer_contact, 
	customer_name_details
from 
	(-- suppliers from popular companies 
	select contact supplier_contact, description supplier_name_details
	from data_vajapora.help_c -- from Part-01
	where 
		-- from Part-02
		1!=1
		or description ilike '%কোম্পানি%'
		or description ilike '%কম্পানি%'
		or description ilike '%ব্যাংক%'
		or description ilike '%লিমিটেড%'
		or description ilike '%গ্রুপ%'
		or description ilike '%প্রাণ%'
		or description ilike '%স্টার%'
		or description ilike '%ফ্রেশ%'
		or description ilike '%বসুন্ধরা%'
		or description ilike '%bashundhara%'
		or description ilike '%আকিজ%'
		or description ilike '%অলিম্পিক%'
		or description ilike '%ইস্পাহানি%'
		or description ilike '%মিল্ক%'
		or description ilike '%cocola%'
		or description ilike '%কোকোলা%'
		or description ilike '%ইউনিলিভার%'
		or description ilike '%danish%'
		or description ilike '%ব্র্যাক%'
		or description ilike '%এসিআই%'
		or description ilike '%নুডুলস%'
		or description ilike '%তিব্বত%'
		or description ilike '%গ্রামীণ%'
		or description ilike '%ডানো%'
		or description ilike '%polar%'
		or description ilike '%পোলার%'
		or description ilike '%rupchada%'
		or description ilike '%rupchanda%'
	) tbl1 
	
	inner join 
	
	(-- suppliers with TK
	select mobile_number supplier_contact
	from public.register_usermobile
	) tbl2 using(supplier_contact)

	inner join 

	(-- those suppliers' customers, who are businesses
	select mobile_no supplier_contact, contact customer_contact, concat(name, ' ', description) customer_name_details
	from public.account
	where 
		type=2 
		and
		(-- from Part-03
		name ilike '%store%'
		or name ilike '%স্টোর%'
		or name ilike '%ষ্টোর%'
		or name ilike '%stor%'
		or name ilike '%shop%'
		or name ilike '%জেনারেল%'
		or name ilike '%মেসার্স%'
		or name ilike '%ভ্যারাইটিজ%'
		or name ilike '%ভ্যারাইটি%'
		or name ilike '%ভেরাইটিজ%'
		or name ilike '%general%'
		or name ilike '%ডিপার্টমেন্টাল%'
		or name ilike '%স্টর%'
		or name ilike '%বাজার%'
		or name ilike '%stur%'
		or name ilike '%ইস্টর%'
		or name ilike '%onlin%'
		or name ilike '%ইস্টোর%'
		or name ilike '%ভ্যারাইটিস%'
		or name ilike '%istor%'
		or name ilike '%workshop%'
		or name ilike '%ইষ্টোর%'
		or name ilike '%stori%'
		or name ilike '%department%'
		or name ilike '%স্টোর%'
		or name ilike '%মেসাস%'
		or name ilike '%estor%'
		or name ilike '%genarel%')
	) tbl3 using(supplier_contact)

	left join 
	
	(-- customers not with TK
	select mobile_number customer_contact
	from public.register_usermobile
	) tbl4 using(customer_contact) 
where tbl4.customer_contact is null; 

/*
-- sanity check
select *
from public.register_usermobile
where mobile_number in 
	('01725700796',
	'01632692051',
	'01935818782',
	'01714588769',
	'01714686380',
	'01790037727');

# Logic: 
- prothome dekhi popular company gulir supplier kara ase
- erpor dekhi eder moddhe TK te kara ase 
- erpor dekhi eder customer kara, ei customer der bhitor business kara 
- erpor ensure kori ei customer guli TK te nai 
- eder list share kore
*/

-- Version-02: for all BI-types 

drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select contact, name
from public.account
where 
	type in(2, 3)
	and date(create_date)>=current_date-365-365; 

-- results: potential TK merchants
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from
	data_vajapora.help_d tbl1

	left join 
	
	(select mobile_number contact
	from public.register_usermobile
	) tbl4 using(contact)
where 
	tbl4.contact is null
	and 
		(name ilike '%store%'
		or name ilike '%স্টোর%'
		or name ilike '%এন্টারপ্রাইজ%'
		or name ilike '%ষ্টোর%'
		or name ilike '%telecom%'
		or name ilike '%enterpris%'
		or name ilike '%টেলিকম%'
		or name ilike '%মেসার্স%'
		or name ilike '%stor%'
		or name ilike '%দোকান%'
		or name ilike '%ট্রেডার্স%'
		or name ilike '%pharmaci%'
		or name ilike '%shop%'
		or name ilike '%fashion%'
		or name ilike '%বাজার%'
		or name ilike '%সেন্টার%'
		or name ilike '%হাউজ%'
		or name ilike '%ফ্যাশন%'
		or name ilike '%হল%'
		or name ilike '%এন্টার%'
		or name ilike '%মেডিকেল%'
		or name ilike '%telicom%'
		or name ilike '%ফার্মেসী%'
		or name ilike '%ইলেকট্রিক%'
		or name ilike '%ব্যবসা%'
		or name ilike '%জেনারেল%'
		or name ilike '%কম্পিউটার%'
		or name ilike '%ভ্যারাইটিজ%'
		or name ilike '%ফার্ম%'
		or name ilike '%সমিতি%'
		or name ilike '%ঘর%'
		or name ilike '%ইলেকট্রনিক্স%'
		or name ilike '%pharma%'
		or name ilike '%মেসাস%'
		or name ilike '%মুদি%'
		or name ilike '%হোটেল%'
		or name ilike '%ইলেকট্রনিক%'
		or name ilike '%agro%'
		or name ilike '%খামার%'
		or name ilike '%ফার্মেসি%'
		or name ilike '%dokan%'
		or name ilike '%ভান্ডার%'
		or name ilike '%বিতান%'
		or name ilike '%center%'
		or name ilike '%মেডিসিন%'
		or name ilike '%ফার্নিচার%'
		or name ilike '%farm%'
		or name ilike '%ট্রেডাস%'
		or name ilike '%ডিজিটাল%'
		or name ilike '%টেইলার্স%'
		or name ilike '%গার্মেন্টস%'
		or name ilike '%অটো%'
		or name ilike '%লিমিটেড%'
		or name ilike '%এগ্রো%'
		or name ilike '%পোল্ট্রি%'
		or name ilike '%servic%'
		or name ilike '%টেডাস%'
		or name ilike '%point%'
		or name ilike '%garment%'
		or name ilike '%ভেরাইটিজ%'
		or name ilike '%compani%'
		or name ilike '%corpor%'
		or name ilike '%digit%'
		or name ilike '%কসমেটিকস%'
		or name ilike '%pharmaceut%'
		or name ilike '%studio%'
		or name ilike '%স্টুডিও%'
		or name ilike '%মিল%'
		or name ilike '%ইলেকট্রনিকস%'
		or name ilike '%ব্রাদার্স%'
		or name ilike '%ভ্যারাইটি%'
		or name ilike '%শপ%'
		or name ilike '%cosmet%'
		or name ilike '%সুজ%'
		or name ilike '%কর্নার%'
		or name ilike '%মুদির%'
		or name ilike '%সু%'
		or name ilike '%পরিবহন%'
		or name ilike '%কসমেটিক%'
		or name ilike '%মের্সাস%'
		or name ilike '%hotel%'
		or name ilike '%কর্ণার%'
		or name ilike '%সমবায়%'
		or name ilike '%auto%'
		or name ilike '%লাইব্রেরী%'
		or name ilike '%টেইলাস%'
		or name ilike '%পান%'
		or name ilike '%কসমেটিক্স%'
		or name ilike '%হোমিও%'
		or name ilike '%market%'
		or name ilike '%tredar%'
		or name ilike '%মটরস%'
		or name ilike '%এনটার%'
		or name ilike '%zone%'
		or name ilike '%hardwar%'
		or name ilike '%কৃষি%'
		or name ilike '%ট্রেডিং%'
		or name ilike '%পয়েন্ট%'
		or name ilike '%drug%'
		or name ilike '%কালেকশন%'
		or name ilike '%fish%'
		or name ilike '%ডিপার্টমেন্টাল%'
		or name ilike '%স্টার%'
		or name ilike '%সার্ভিসিং%'
		or name ilike '%industri%'
		or name ilike '%ফানিচার%'
		or name ilike '%কফি%'
		or name ilike '%জুয়েলার্স%'
		or name ilike '%মাছের%'
		or name ilike '%কার%'
		or name ilike '%নেটওয়ার্ক%'
		or name ilike '%অনলাইন%'
		or name ilike '%বিজনেস%'
		or name ilike '%varieti%'
		or name ilike '%ভ্যারাইটিস%'
		or name ilike '%ব্যাবসা%'
		or name ilike '%স্বপ্ন%'
		or name ilike '%cloth%'
		or name ilike '%technolog%'
		or name ilike '%car%'
		or name ilike '%কনফেকশনারি%'
		or name ilike '%সন্স%'
		or name ilike '%হাট%'
		or name ilike '%ইস্টর%'
		or name ilike '%stur%'
		or name ilike '%enterpr%'
		or name ilike '%mill%'
		or name ilike '%দুকান%'
		or name ilike '%bitan%'
		or name ilike '%fabric%'
		or name ilike '%ফামেসী%'
		or name ilike '%হার্ডওয়্যার%'
		or name ilike '%গাড়ি%'
		or name ilike '%dukan%'
		or name ilike '%ডেইরি%'
		or name ilike '%বেকারি%'
		or name ilike '%telikom%'
		or name ilike '%enterpric%'
		or name ilike '%dairi%'
		or name ilike '%tailor%'
		or name ilike '%মার্কেট%'
		or name ilike '%কোম্পানি%'
		or name ilike '%ইঞ্জিনিয়ারিং%'
		or name ilike '%কনফেকশনারী%'
		or name ilike '%বস্ত্রালয়%'
		or name ilike '%express%'
		or name ilike '%ইস্টোর%'
		or name ilike '%media%'
		or name ilike '%টেডার্স%'
		or name ilike '%সেলুন%'
		or name ilike '%মেটাল%'
		or name ilike '%দৈনিক%'
		or name ilike '%রাইস%'
		or name ilike '%power%'
		or name ilike '%রেস্টুরেন্ট%'
		or name ilike '%গ্লাস%'
		or name ilike '%steel%'
		or name ilike '%madic%'
		or name ilike '%ইন্টারন্যাশনাল%'
		or name ilike '%net%'
		or name ilike '%privat%'
		or name ilike '%বস্ত্র%'
		or name ilike '%মাছ%'
		or name ilike '%mudi%'
		or name ilike '%মসজিদ%'
		or name ilike '%tech%'
		or name ilike '%ফার্মা%'
		or name ilike '%রেন্ট%'
		or name ilike '%ghor%'
		or name ilike '%ব্যাংক%'
		or name ilike '%লাইব্রেরি%'
		or name ilike '%ষ্টোর%'
		or name ilike '%হার্ডওয়ার%'
		or name ilike '%ফেব্রিক্স%'
		or name ilike '%design%'
		or name ilike '%প্রাইভেট%'
		or name ilike '%workshop%'
		or name ilike '%agenc%'
		or name ilike '%librari%'
		or name ilike '%ইষ্টোর%'
		or name ilike '%বেকারী%'
		or name ilike '%গ্যালারী%'
		or name ilike '%সেনেটারী%'
		or name ilike '%স্টল%'
		or name ilike '%plastic%'
		or name ilike '%ট্রেড%'
		or name ilike '%ষ্টর%'
		or name ilike '%farmaci%'
		or name ilike '%গিফট%'
		or name ilike '%এজেন্সি%'
		or name ilike '%ষ্টেশনারী%'
		or name ilike '%department%'
		or name ilike '%accessori%'
		or name ilike '%cafe%'
		or name ilike '%ইষ্টর%'
		or name ilike '%ট্রের্ডাস%'
		or name ilike '%export%'
		or name ilike '%টাইলস%'
		or name ilike '%transport%'
		or name ilike '%ওয়াকসপ%'
		or name ilike '%textil%'
		or name ilike '%pharmeci%'
		or name ilike '%ইন্জিনিয়ারিং%'
		or name ilike '%মুদিখানা%'
		or name ilike '%বেডিং%'
		or name ilike '%telekom%'
		or name ilike '%stationari%'
		or name ilike '%dental%'
		or name ilike '%sanitari%'
		or name ilike '%stall%'
		or name ilike '%elect%'
		or name ilike '%metal%'
		or name ilike '%ইন্টারপ্রাইজ%'
		or name ilike '%মার্ট%'
		or name ilike '%হার্ডওয়ার%'
		or name ilike '%স্টোর%'
		or name ilike '%ইঞ্জিনিয়ারিং%'
		or name ilike '%ডিপার্টমেন্ট%'
		or name ilike '%সবজি%'
		or name ilike '%tex%'
		or name ilike '%প্রতিষ্ঠান%'
		or name ilike '%শপিং%'
		or name ilike '%ইলেক্ট্রনিক্স%'
		or name ilike '%ডেকোরেটর%'
		or name ilike '%গাড়ির%'
		or name ilike '%internet%'
		or name ilike '%ক্রোকারিজ%'
		or name ilike '%firm%'
		or name ilike '%কর্পোরেশন%'
		or name ilike '%মৎস%'
		or name ilike '%এনটারপ্রাইজ%'
		or name ilike '%মিডিয়া%'
		or name ilike '%ফ্যাশান%'
		or name ilike '%poltri%'
		or name ilike '%glass%'
		or name ilike '%ওয়ার্কসপ%'
		or name ilike '%বাড়ির%'
		or name ilike '%কনস্ট্রাকশন%'
		or name ilike '%ইলেক্ট্রিক%'
		or name ilike '%খানা%'
		or name ilike '%বস্ত্রালয়%'
		or name ilike '%জুয়েলার্স%'
		or name ilike '%ডেইরী%'
		or name ilike '%ওয়াকসপ%'
		or name ilike '%পল্লী%'
		or name ilike '%সমবায়%'
		or name ilike '%tradar%'
		or name ilike '%রিচার্জ%'
		or name ilike '%বোরকা%'
		or name ilike '%gift%'
		or name ilike '%ডিস%'
		or name ilike '%গ্যাস%'
		or name ilike '%coffe%'
		or name ilike '%মেডিক্যাল%'
		or name ilike '%পোলট্রি%'
		or name ilike '%কেবল%'
		or name ilike '%ডেন্টাল%'
		or name ilike '%varaiti%'
		or name ilike '%মডেল%'
		or name ilike '%স্টুর%'
		or name ilike '%মিষ্টান্ন%'
		or name ilike '%লেডিস%'
		or name ilike '%ইন্টার%'
		or name ilike '%কুলিং%'
		or name ilike '%treda%'
		or name ilike '%brick%'
		or name ilike '%কোং%'
		or name ilike '%প্লাস্টিক%'
		or name ilike '%ইসটোর%'
		or name ilike '%সাউন্ড%'
		or name ilike '%হাডওয়ার%'
		or name ilike '%সংগঠন%'
		or name ilike '%সংস্থা%'
		or name ilike '%ক্লাব%'
		or name ilike '%টেড্রাস%'
		or name ilike '%সংঘ%'
		or name ilike '%somiti%'
		or name ilike '%pvt%'
		or name ilike '%art%'
		or name ilike '%club%'
		or name ilike '%academi%'
		or name ilike '%গ্যালারি%'
		or name ilike '%সঞ্চয়%'
		or name ilike '%bajar%'
		or name ilike '%ফ্রেন্ডস%'
		or name ilike '%walton%'
		or name ilike '%জোন%'
		or name ilike '%ইলেক্ট্রনিক%'
		or name ilike '%বেবসা%'
		or name ilike '%ষ্টুডিও%'
		or name ilike '%ট্রান্সপোর্ট%'
		or name ilike '%vision%'
		or name ilike '%চাষ%'
		or name ilike '%টেক্সটাইল%'
		or name ilike '%confectioneri%'
		or name ilike '%communic%'
		or name ilike '%ডিজাইন%'
		or name ilike '%entar%'
		or name ilike '%্যবসা%'
		or name ilike '%বেপারী%'
		or name ilike '%পার্টস%'
		or name ilike '%নেটওয়ার্ক%'
		or name ilike '%fashon%'
		or name ilike '%trust%'
		or name ilike '%জুয়েলাস%'
		or name ilike '%পোল্টি%'
		or name ilike '%স্টেশনারি%'
		or name ilike '%মেশিনারীজ%'
		or name ilike '%ইন্টারনেট%'
		or name ilike '%project%'
		or name ilike '%ভাড়া%'
		or name ilike '%door%'
		or name ilike '%টিম্বার%'
		or name ilike '%genarel%'
		or name ilike '%ষ্টীল%'
		or name ilike '%নেট%'
		or name ilike '%মাট%'
		or name ilike '%মটর%'
		or name ilike '%বস্রালয়%'
		or name ilike '%মিষ্টি%'
		or name ilike '%স্টিল%'
		or name ilike '%ইলেকটিক%'
		or name ilike '%builder%'
		or name ilike '%রং%'
		or name ilike '%farmesi%'
		or name ilike '%veraiti%'
		or name ilike '%ডিম%'
		or name ilike '%টোর%'
		or name ilike '%কণার%'
		or name ilike '%ইসটর%'
		or name ilike '%এস্টোর%'
		or name ilike '%ব্যাগ%'
		or name ilike '%cng%'
		or name ilike '%light%'
		or name ilike '%ফটোকপি%'
		or name ilike '%variti%'
		or name ilike '%এসটোর%'
		or name ilike '%astor%'
		or name ilike '%হেয়ার%'
		or name ilike '%বুক%'
		or name ilike '%ডোর%'
		or name ilike '%surgic%'
		or name ilike '%কাটিং%'
		or name ilike '%bag%'
		or name ilike '%বস্র%'
		or name ilike '%কারখানা%'
		or name ilike '%কুমার%'
		or name ilike '%ফার্ণিচার%'
		or name ilike '%টিভি%'
		or name ilike '%ইস্টুর%'
		or name ilike '%মেলা%'
		or name ilike '%মটরস্%'
		or name ilike '%garden%'
		or name ilike '%পেপার%'
		or name ilike '%senter%'
		or name ilike '%্যাশন%'
		or name ilike '%factori%'
		or name ilike '%gold%'
		or name ilike '%কনার%'
		or name ilike '%পাওয়ার%'
		or name ilike '%ভিলা%'
		or name ilike '%press%'
		or name ilike '%ষ্ঠোর%'
		or name ilike '%ওয়ার্কশপ%'
		or name ilike '%cosmat%'
		or name ilike '%madicin%'
		or name ilike '%কেন্দ্র%'
		or name ilike '%বিউটি%'
		or name ilike '%পয়েন্ট%'
		or name ilike '%paint%'
		or name ilike '%kitchen%'
		or name ilike '%শিল্পালয়%'
		or name ilike '%গাড়ি%'
		or name ilike '%গামেন্টস%'
		or name ilike '%বস্তালয়%'
		or name ilike '%bekari%'
		or name ilike '%মাল্টিমিডিয়া%'
		or name ilike '%স্যানেটারী%'
		or name ilike '%homoeo%'
		or name ilike '%দোকানদার%'
		or name ilike '%গ্যারেজ%'
		or name ilike '%পপুলার%'
		or name ilike '%কবুতর%'
		or name ilike '%sani%'
		or name ilike '%paper%'
		or name ilike '%মোদির%'
		or name ilike '%সেনেটারি%'
		or name ilike '%gari%'
		or name ilike '%ফারনিচার%'
		or name ilike '%consult%'
		or name ilike '%প্রেস%'
		or name ilike '%বই%'
		or name ilike '%হাসপাতাল%'
		or name ilike '%golden%'
		or name ilike '%ভাণ্ডার%'
		or name ilike '%ইন্জিনিয়ারিং%'
		or name ilike '%ক্ষুদ্র%'
		or name ilike '%বাগান%'
		or name ilike '%depart%'
		or name ilike '%মেশিনারিজ%'
		or name ilike '%apparel%'
		or name ilike '%gas%'); 
