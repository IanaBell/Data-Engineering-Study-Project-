create or replace function std7_111.f_load_write_log(p_log_type text, p_log_message text, p_location text)
	returns void
	language plpgsql
	volatile
as $$

declare

	v_log_type text;
	v_log_message text;
	v_sql text;
	v_location text;
	v_res text;
	
begin
	
	v_log_type = upper(p_log_type);
	v_location = lower(p_location);
	if v_log_type not in ('ERROR', 'INFO') then
	 raise exception 'Illegal log type! Use one of: ERROR, INFO';
	end if;

raise notice '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

v_log_message := replace(p_log_message, '''', '''''');

v_sql = 'INSERT INTO std7_111.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
			VALUES ( ' || nextval('std7_111.log_id_seq')|| ' ,
				   ''' || v_log_type || ''',
					 ' || coalesce('''' || v_log_message || '''', '''empty''')|| ',
					 ' || coalesce('''' || v_location || '''', 'null')|| ',
					 ' || CASE when v_log_type = 'ERROR' then true else false end || ',
						current_timestamp, current_user);';
					
raise notice 'INESRT SQL IS: %', v_sql;
v_res := dblink('adb_server', v_sql);
end;

$$
execute on any;

create or replace function std7_111.f_load_full(p_table text, p_file_name text)
	returns int4
	language plpgsql
	volatile
as $$

declare

 v_ext_table_name text;
 v_sql text;
 v_gpfdist text;
 v_result int;
 
begin
	
	v_ext_table_name = p_table||'_ext';

	execute 'truncate table '||p_table;

	execute 'drop external table if exists '||v_ext_table_name;

	v_gpfdist = 'gpfdist://172.16.128.154:8080/'||p_file_name||'.csv';

	v_sql = 'create external table '||v_ext_table_name||'(like '||p_table||')
			 location ('''||v_gpfdist||''' 
             ) on all 
			 format ''csv'' ( header delimiter '';'' null '''' escape ''"'' quote ''"'')
			 encoding ''utf8''';

	raise notice 'external table is: %', v_sql;

	execute v_sql;

	execute 'insert into '||p_table||' select * from '||v_ext_table_name;

	execute 'select count(1) from '||p_table into v_result;

	return v_result;
end;

$$
execute on any;


$$
EXECUTE ON ANY;
