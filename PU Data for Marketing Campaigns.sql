/*
- Viz: 
- Data: 
- Function: 
- Table: data_vajapora.pu_june
- File: 
- Path: 
- Document/Presentation: 
- Email thread: Data Requirement: June PU
- Notes (if any): 
*/

-- 187,401 users
create table data_vajapora.pu_june as
select distinct mobile_no
from tallykhata.tk_power_users_10 
where report_date>='2021-06-01' and report_date<='2021-06-30';