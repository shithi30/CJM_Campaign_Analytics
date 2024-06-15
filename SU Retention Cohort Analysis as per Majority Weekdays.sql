/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1263656316
- Data: 
	- https://docs.google.com/spreadsheets/d/1jcFLdV3N__t8kFGVWc4-hyVQDMLZ35wK07rZlDLlw5Q/edit#gid=521865406
	- https://docs.google.com/spreadsheets/d/1jcFLdV3N__t8kFGVWc4-hyVQDMLZ35wK07rZlDLlw5Q/edit#gid=1689697442
	- https://docs.google.com/spreadsheets/d/1jcFLdV3N__t8kFGVWc4-hyVQDMLZ35wK07rZlDLlw5Q/edit#gid=1892977648
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

drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, to_char(report_date, 'YYYY-WW') spu_year_wk, count(*) spu_days
from tallykhata.tk_spu_aspu_data 
where pu_type in('SPU')
group by 1, 2
having count(*)>3; 

with 
	temp_table as
	(select 
		min_spu_year_wk, 
		spu_year_wk, 
		count(mobile_no) sus, 
		row_number() over(partition by min_spu_year_wk order by spu_year_wk)-1 seq
	from 
		(select mobile_no, min(spu_year_wk) min_spu_year_wk 
		from data_vajapora.help_a 
		group by 1
		) tbl1 
		
		inner join 
		
		data_vajapora.help_a tbl2 using(mobile_no)
	group by 1, 2 
	having min_spu_year_wk>='2022-01'
	order by 1, 2
	) 
	
select min_spu_year_wk, spu_year_wk, seq, sus, sus_init, sus*1.00/sus_init sus_init_pct
from 
	temp_table tbl1 
	
	inner join 
	
	(select min_spu_year_wk, sus sus_init 
	from temp_table 
	where seq=0
	) tbl2 using(min_spu_year_wk); 
