CREATE OR REPLACE FUNCTION default.f_partition_traffic(p_table text, p_partition_key text, p_start_date timestamp, p_end_date timestamp, p_pxf_table text, p_user_id text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE 
    v_ext_table text;          
    v_temp_table text;         
    v_sql text;                 
    v_pxf text;                 
    v_result text;                     
    v_dist_key text;               
    v_params text;                  
    v_where text;               
    v_load_interval interval;      
    v_start_date text;          
    v_end_date text;           
    v_table_oid int;                
    v_cnt int8;                    
BEGIN
    
    v_ext_table = p_table||'_ext';  
    v_temp_table = p_table||'_tmp';     

    
    SELECT c.oid
    INTO v_table_oid
    FROM pg_class AS c 
    INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE n.nspname||'.'||c.relname = p_table
    LIMIT 1;

    
    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key = 'DISTRIBUTED RANDOMLY';  
    ELSE
        v_dist_key = pg_get_table_distributedby(v_table_oid);  
    END IF;

   
    SELECT coalesce('with (' || array_to_string(reloptions, ', ') || ')', '')
    INTO v_params
    FROM pg_class 
    WHERE oid = p_table::regclass;

    
    v_load_interval = '1 month'::INTERVAL;

    
    v_pxf = 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='
                    ||p_user_id||'&PASS='||p_pass;
    RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;  

   
    v_sql = 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table||'; 
	    CREATE EXTERNAL TABLE '||v_ext_table||'(plant bpchar(4), "date" bpchar(10), "time" bpchar(6), frame_id bpchar(10), quantity int4)
            LOCATION ('''||v_pxf||''') ON ALL
            FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
            ENCODING ''UTF8''';
    RAISE NOTICE 'EXTERNAL TABLE: %', v_sql;  
    EXECUTE v_sql; 

	WHILE p_start_date < p_end_date LOOP

	    v_start_date := DATE_TRUNC('month', p_start_date);
	    v_end_date := DATE_TRUNC('month', p_start_date) + v_load_interval;
	
		v_where = 'to_date('||p_partition_key||', ''DD.MM.YYYY'')'||' >= '''||v_start_date||'''::date AND '||'to_date('||p_partition_key||', ''DD.MM.YYYY'')'||' < '''||v_end_date||'''::date';
	
	    v_sql = 'DROP TABLE IF EXISTS '||v_temp_table||';
	                CREATE TABLE '||v_temp_table||' (LIKE '||p_table||') ' ||v_params||' '||v_dist_key||';';    
	    RAISE NOTICE 'TEMP TABLE: %', v_sql; 
	    EXECUTE v_sql;  
	
	    v_sql = 'INSERT INTO '|| v_temp_table ||' SELECT plant, TO_DATE("date",''DD-MM-YYYY'') as date, "time", frame_id, quantity FROM '||v_ext_table||' WHERE '||v_where;
	    RAISE NOTICE 'INSERT from EXTERNAL: %', v_sql;  
	    EXECUTE v_sql;  
	    GET DIAGNOSTICS v_cnt = ROW_COUNT;  
	    RAISE NOTICE 'INSERTED ROWS: %', v_cnt;  
	
	    EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_temp_table;
	
	    v_sql = 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||p_start_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION;';        
	    RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql; 
	    EXECUTE v_sql; 

		p_start_date := p_start_date + v_load_interval;

	END LOOP;

    EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;
    RETURN v_result; 
END;
$$
EXECUTE ON ANY;




CREATE OR REPLACE FUNCTION default.f_load_full(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$

DECLARE 

 v_ext_table_name text;
 v_sql text;
 v_gpfdist text;
 v_result int;

BEGIN
	
	v_ext_table_name = p_table||'_ext';

	EXECUTE 'TRUNCATE TABLE '||p_table;

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;

	v_gpfdist = 'GPFDIST://172.16.128.214:8080/'||p_file_name||'.csv';
	
	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||'(LIKE '||p_table||')
			 LOCATION ('''||v_gpfdist||'''
			 ) ON ALL 
			 FORMAT ''CSV'' ( HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
			 ENCODING ''UTF8''
			 SEGMENT REJECT LIMIT 10 rows';

	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;

	EXECUTE v_sql;

	EXECUTE 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;

	EXECUTE 'SELECT COUNT(1) FROM '||p_table INTO v_result;

	RETURN v_result; 

END;

$$
EXECUTE ON ANY;
