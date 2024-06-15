CREATE OR REPLACE PROCEDURE data_vajapora.prc_mail_calling()
 LANGUAGE plpgsql
AS $procedure$
/****************************************************************************************
 Parameters:   

 ----------------------------------------------------------------------------------------
 
 Author Name    = 
 Supervised By  = 
 Creation Date  = 
 Description    = 
 Source Tables  = 
 Target Tables  =
                    
 
 Date           Name                Description
 ----------------------------------------------------------------------------------------


 ****************************************************************************************/
declare
    v_error_msg     text;
    v_job_category  varchar(255); 
    v_job_name      varchar(255);
    ------Log-------------
    v_sql           text :='';
    v_block         varchar(255);
    v_proc_name     varchar(255);      
    v_load_date     date;
    v_run_date      date;
    v_sql_err       text :='';
    v_excute_ind    boolean := true;      ---TODO

    ----------------------
    v_from_date     date;
    v_to_date       date; 
    ----------------------
    v_date          date;

       
        
begin
  
    v_from_date := current_date-1;
    v_to_date   := current_date;



    ------Log-------------
    v_proc_name := 'prc_mail_calling';  
    v_load_date := v_from_date;
    v_run_date  := v_from_date+1;
    ----------------------
    v_job_category  := 'DWH JOBS'; 
    v_job_name      := 'data_vajapora.'||v_proc_name||'()';  



-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

   

    --------------------------------------------------------------------------------
    -------------------------function-01()-----------------------------
    --------------------------------------------------------------------------------
    v_block:='calling data_vajapora.help_a_generation()';
    v_sql := 'select * from data_vajapora.help_a_generation();    
			  analyze data_vajapora.help_a;              
            ';

    if v_excute_ind=true then
        execute v_sql;
    end if;


    commit;        


  


    --------------------------------------------------------------------------------
    ----------------------function-2()------------------------------
    --------------------------------------------------------------------------------
    v_block:='calling data_vajapora.help_b_generation()';
    v_sql := 'select * from data_vajapora.help_b_generation();   
			  analyze data_vajapora.help_b;               
            ';

    if v_excute_ind=true then
        execute v_sql;
    end if;


    commit;        


    --------------------------------------------------------------------------------
    ----------------------function-3()----------------------------------
    --------------------------------------------------------------------------------
    v_block:='calling data_vajapora.metrics_generation()';
    v_sql := 'select * from data_vajapora.metrics_generation();                  
            ';

    if v_excute_ind=true then
        execute v_sql;
    end if;


    commit;        




-----------------------------------------------------------------------------------------------------------
------------------------------Log--------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

raise notice '[DWH JOB] Procedure Completed Successfully';
 
end $procedure$
;

call data_vajapora.prc_mail_calling();

-- see which queries are running
select t.datname, t.pid, t.usename, t.backend_start, t.state_change, t.state, t.query , application_name
FROM pg_stat_activity t
WHERE  state='active'
order by backend_start desc 
;



