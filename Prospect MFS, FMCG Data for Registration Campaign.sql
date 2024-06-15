/*
- Viz: 
- Data: 
- Table: data_vajapora.prospect_mfs_fmcg 
- File: prospect_MFS_FMCG.csv
- Email thread: Data Requirement: MFS & FMCG
- Notes (if any): 
*/

create table data_vajapora.prospect_mfs_fmcg as
select *
from 
	(select distinct mobile mobile_no, industry
	from tallykhata.final_tk_prospect_db_list
	where industry in('MFS','FMCG')
	) tbl1
	
	left join 
	
	(select distinct mobile mobile_no
	from public.registered_users
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null;

-- final data shared
select *
from data_vajapora.prospect_mfs_fmcg; 
