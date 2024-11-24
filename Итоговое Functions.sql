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

create or replace function std7_111.f_load_simple_partition(p_table text, p_partition_key text, 
															p_start_date timestamp, p_end_date timestamp, 
															p_pxf_table text, p_user_id text, p_pass text)
	returns int4
	language plpgsql
	volatile
as $$
	

declare
	v_ext_table text;
	v_temp_table text;
	v_sql text;
	v_pxf text;
	v_result int;
	v_dist_key text;
	v_params text;
	v_where text;
	p_partition_key_data_type text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
	v_table_oid int4;
	v_cnt int8;
begin
	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';

	select c.oid ---достаём ключ распределения таблицы
	into v_table_oid ---записываем айди таблицы в переменную
	from pg_class as c 
	inner join pg_namespace as n
	on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table
	limit 1;

	if v_table_oid = 0 or v_table_oid is null then ---с помощью системной функции получим ключ распределения
		v_dist_key = 'distributed randomly';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;

	select coalesce('with (' || array_to_string(reloptions, ', ') || ')', '') ---записываем строку с параметрами целевой таблицы
	from pg_class
	into v_params
	where oid = p_table::regclass;

	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date );

	v_where = p_partition_key ||' >= '''||v_start_date||'''::date and '||p_partition_key||' < '''||v_end_date||'''::date';

	v_pxf := 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user_id||'&PASS='||p_pass;
	raise notice 'pxf connection string: %', v_pxf;
	
	execute 'drop external table if exists ' ||v_ext_table;
	v_sql := 'create external table '||v_ext_table||'(like '||p_table||')
			location ( '''||v_pxf||''') on all
			format ''custom'' (formatter = ''pxfwritable_import'')
			encoding ''utf8''';
	raise notice 'external table is: %', v_sql;
	execute v_sql;
	
	v_sql := 'drop table if exists '|| v_temp_table ||';
			 create table '|| v_temp_table ||' (like '||p_table||') ' ||v_params||' '||v_dist_key||';';
	raise notice 'temp table is: %', v_sql;
	execute v_sql;
		
	
	v_sql := 'insert into '|| v_temp_table ||' select * from '||v_ext_table||' where '||v_where;
	raise notice 'insert into temp table: %', v_sql;
	execute v_sql;	

	get diagnostics v_cnt = row_count;
	raise notice 'inserted rows: %', v_cnt;

	v_sql := 'delete from '||p_table||' where '||v_where;
	raise notice 'delete from main table: %', v_sql;
	execute v_sql;
	
	v_sql := 'insert into '||p_table||' select * from '||v_temp_table;
	raise notice 'insert into main table: %', v_sql;
	execute v_sql;

	execute 'select count(1) from '||p_table||' where '||v_where into v_result;

	return v_result;

end;
 
$$
execute on any;

create or replace function std7_111.f_load_traffic_partition(p_table text, p_partition_key text, 
															p_start_date timestamp, p_end_date timestamp, 
															p_pxf_table text, p_user_id text, p_pass text)
	returns int4
	language plpgsql
	volatile
as $$
	

declare
	v_ext_table text;
	v_temp_table text;
	v_sql text;
	v_pxf text;
	v_result int;
	v_dist_key text;
	v_params text;
	v_where text;
	p_partition_key_data_type text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
	v_table_oid int4;
	v_cnt int8;
begin
	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';

	select c.oid
	into v_table_oid
	from pg_class as c 
	inner join pg_namespace as n
	on c.relnamespace = n.oid
	where n.nspname||'.'||c.relname = p_table
	limit 1;

	if v_table_oid = 0 or v_table_oid is null then 
		v_dist_key = 'distributed randomly';
	else
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	end if;

	select coalesce('with (' || array_to_string(reloptions, ', ') || ')', '')
	from pg_class
	into v_params
	where oid = p_table::regclass;

	v_load_interval = '1 month'::interval;
	v_start_date := date_trunc('month', p_start_date);
	v_end_date := date_trunc('month', p_end_date );

	v_where = 'to_date('||p_partition_key||', ''DD.MM.YYYY'') >= '''||v_start_date||'''::date AND to_date('||p_partition_key||', ''DD.MM.YYYY'') < '''||v_end_date||'''::date';

	v_pxf := 'pxf://'||p_pxf_table||'?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='||p_user_id||'&PASS='||p_pass;
	raise notice 'pxf connection string: %', v_pxf;
	
	execute 'drop external table if exists ' ||v_ext_table;
	v_sql := 'create external table '||v_ext_table||'(like '||p_table||')
			location ( '''||v_pxf||''') on all
			format ''custom'' (formatter = ''pxfwritable_import'')
			encoding ''utf8''';
	raise notice 'external table is: %', v_sql;
	execute v_sql;
	
	v_sql := 'drop table if exists '|| v_temp_table ||';
			 create table '|| v_temp_table ||' (like '||p_table||') ' ||v_params||' '||v_dist_key||';';
	raise notice 'temp table is: %', v_sql;
	execute v_sql;
		
	
	v_sql := 'insert into '|| v_temp_table ||' select * from '||v_ext_table||' where '||v_where;
	raise notice 'insert into temp table: %', v_sql;
	execute v_sql;	

	get diagnostics v_cnt = row_count;
	raise notice 'inserted rows: %', v_cnt;

	v_sql := 'delete from '||p_table||' where '||v_where;
	raise notice 'delete from main table: %', v_sql;
	execute v_sql;
	
	v_sql := 'insert into '||p_table||' select * from '||v_temp_table;
	raise notice 'insert into main table: %', v_sql;
	execute v_sql;

	execute 'select count(1) from '||p_table||' where '||v_where into v_result;

	return v_result;

end;
 
$$
execute on any;			

CREATE OR REPLACE FUNCTION std7_111.f_load_mart_final(p_start_date varchar, p_end_date varchar)
RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$


DECLARE 
	v_return int;
	v_sql text;
	v_start_date date;
	v_end_date date;
BEGIN 

	PERFORM std7_111.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'Start f_load_mart_final',
									  p_location := 'Sales final mart calculation');

	v_start_date = date_trunc('day',to_date(p_start_date, 'YYYYMMDD'));
	v_end_date = date_trunc('day',to_date(p_end_date, 'YYYYMMDD'));
	
	EXECUTE 'DROP TABLE IF EXISTS std7_111.mart_final';

	v_sql := 'CREATE TABLE std7_111.mart_final (
		plant varchar(4),
		plant_txt text,
		turnover numeric(17, 2),
		coupon_discount numeric(17, 2),
		turnover_after_discount numeric(17, 2),
		sold_materials int4, 
		bills_num int4,
		traffic int4,
		promo_materials int4,
		promo_materials_percent numeric(17, 2),
		materials_avg numeric(17, 2),
		plant_cr numeric(17, 2),
		bill_avg numeric(17, 2),
		revenue_per_client_avg numeric(17, 2)
		) 
		WITH (
			appendonly=true,
			orientation=column,
			compresstype=zstd,
			compresslevel=1
		)
		DISTRIBUTED RANDOMLY';
	EXECUTE v_sql;
	
	EXECUTE 'INSERT INTO std7_111.mart_final WITH bills_df AS (
					SELECT  bh.plant AS plant, 
							SUM(bi.rpa_sat) AS turnover,
							SUM(bi.qty) AS sold_materials,
							COUNT(DISTINCT bi.billnum) AS bills_num
					FROM std7_111.bills_item bi
					JOIN std7_111.bills_head bh
					ON bi.billnum = bh.billnum
					WHERE bi.calday BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''
					GROUP BY 1				
					),
				 traffic_df AS (
					SELECT  plant,
							SUM(quantity) AS traffic
					FROM std7_111.traffic
					GROUP BY 1
					),
				 coupons_staging AS (
					SELECT  c.plant AS plant,
							c.material AS material,
							CASE 
								WHEN p.promo_type = ''001'' THEN p.promo_size
								WHEN p.promo_type = ''002'' THEN (p.promo_size::numeric * bi.rpa_sat) / (bi.qty * 100)
							END AS discount,
							ROW_NUMBER() OVER (PARTITION BY c.coupon_num) AS rank
					FROM std7_111.coupons c
					JOIN std7_111.promos p ON c.coupon_promo = p.promo_id
					JOIN std7_111.bills_item bi ON c.billnum = bi.billnum AND c.material = bi.material
					WHERE c.calday BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''
					),
				 coupons_df AS (
					SELECT  plant,
							COUNT(material) AS promo_materials,
							SUM(discount) AS coupon_discount
					FROM coupons_staging
					WHERE rank = 1
					GROUP BY 1
					)

			SELECT  s.plant,
					s.txt AS plant_text,
					bd.turnover, 
					cd.coupon_discount,
					bd.turnover - cd.coupon_discount AS turnover_after_discount, 
					bd.sold_materials,
					bd.bills_num,
					td.traffic,
					cd.promo_materials,
					ROUND((cd.promo_materials::numeric / bd.sold_materials) * 100, 1) AS promo_materials_percent, 
					ROUND(bd.sold_materials::numeric / bd.bills_num, 2) AS materials_avg, 
					ROUND((bd.bills_num::numeric / td.traffic) * 100, 2) AS plant_cr, 
					ROUND(bd.turnover::numeric / bd.bills_num, 1) AS bill_avg, 
					CASE 
						WHEN td.traffic = 0 THEN 0
						ELSE ROUND(bd.turnover::numeric / td.traffic, 1)
					END AS revenue_per_client_avg 
			FROM  std7_111.stores s
			LEFT JOIN bills_df bd ON s.plant = bd.plant
			LEFT JOIN traffic_df td ON s.plant = td.plant
			LEFT JOIN coupons_df cd ON s.plant = cd.plant
			ORDER BY 1';

	 EXECUTE 'SELECT COUNT(*) FROM std7_111.mart_final' INTO v_return;

     RETURN v_return;
END;

$$
EXECUTE ON ANY;
