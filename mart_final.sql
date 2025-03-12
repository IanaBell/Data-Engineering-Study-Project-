CREATE OR REPLACE FUNCTION default.f_load_mart_final(p_start_date varchar, p_end_date varchar)
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

	PERFORM default.f_load_write_log(p_log_type := 'INFO',
					 p_log_message := 'Start f_load_mart_final',
					 p_location := 'Sales final mart calculation');

	v_start_date = to_date(p_start_date, 'YYYYMMDD');
	v_end_date = to_date(p_end_date, 'YYYYMMDD');
	
	EXECUTE 'DROP TABLE IF EXISTS default.mart_final';

	v_sql := 'CREATE TABLE default.mart_final (
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
	
	EXECUTE 'INSERT INTO default.mart_final WITH bills_df AS (
					SELECT  bh.plant AS plant, 
							SUM(bi.rpa_sat) AS turnover,
							SUM(bi.qty) AS sold_materials,
							COUNT(DISTINCT bi.billnum) AS bills_num
					FROM default.bills_item bi
					JOIN default.bills_head bh
					ON bi.billnum = bh.billnum
					WHERE bi.calday BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''
					GROUP BY 1				
					),
				 traffic_df AS (
					SELECT  plant,
							SUM(quantity) AS traffic
					FROM default.traffic tr
					WHERE tr.date BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''
					GROUP BY 1
					),
				 coupons_stg AS (
					SELECT  c.plant AS plant,
							c.material AS material,
							CASE 
								WHEN p.promo_type = ''001'' THEN p.promo_size
								WHEN p.promo_type = ''002'' THEN (p.promo_size::numeric * bi.rpa_sat) / (bi.qty * 100)
							END AS discount,
							ROW_NUMBER() OVER (PARTITION BY c.coupon_num) AS rank
					FROM default.coupons c
					JOIN default.promos p ON c.coupon_promo = p.promo_id
					JOIN default.bills_item bi ON c.billnum = bi.billnum AND c.material = bi.material
					WHERE c.calday BETWEEN '''||v_start_date||''' AND '''||v_end_date||'''
					),
				 coupons_df AS (
					SELECT  plant,
							COUNT(material) AS promo_materials,
							SUM(discount) AS coupon_discount
					FROM coupons_stg
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
			FROM  default.stores s
			LEFT JOIN bills_df bd ON s.plant = bd.plant
			LEFT JOIN traffic_df td ON s.plant = td.plant
			LEFT JOIN coupons_df cd ON s.plant = cd.plant
			ORDER BY 1';

	 EXECUTE 'SELECT COUNT(*) FROM default.mart_final' INTO v_return;

     RETURN v_return;
END;
$$
EXECUTE ON ANY;
