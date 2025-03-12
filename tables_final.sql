CREATE TABLE default.traffic (
	plant varchar(4) NULL,
	"date" date NULL,
	"time" varchar(6) NULL,
	frame_id varchar(10) NULL,
	quantity int4 NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (frame_id)
PARTITION BY RANGE (date)
(
	START ('2021-01-01'::DATE) INCLUSIVE
	END ('2021-03-01'::DATE) EXCLUSIVE
	EVERY (INTERVAL '1 month'),
	DEFAULT PARTITION def
);

CREATE TABLE default.bills_head (
	billnum int8 NULL,
	plant varchar(4) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE (calday)
(
	START ('2021-01-01'::DATE) INCLUSIVE
	END ('2021-03-01'::DATE) EXCLUSIVE
	EVERY (INTERVAL '1 month'),
	DEFAULT PARTITION def
);

CREATE TABLE default.bills_item (
	billnum int8 NULL,
	billitem int8 NULL,
	material int8 NULL,
	qty int8 NULL,
	netval numeric(17, 2) NULL,
	tax numeric(17, 2) NULL,
	rpa_sat numeric(17, 2) NULL,
	calday date NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE (calday)
(
	START ('2021-01-01'::DATE) INCLUSIVE
	END ('2021-03-01'::DATE) EXCLUSIVE
	EVERY (INTERVAL '1 month'),
	DEFAULT PARTITION def
);

CREATE TABLE default.stores (
	plant varchar(4) NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

CREATE TABLE default.coupons (
	plant varchar(4) NULL,
	calday date NULL,
	coupon_num varchar(20) NULL,
	coupon_promo varchar NULL,
	material int8 NULL,
	billnum int8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE (calday)
(
	START ('2021-01-01'::DATE) INCLUSIVE
	END ('2021-03-01'::DATE) EXCLUSIVE
	EVERY (INTERVAL '1 month')
);

CREATE TABLE default.promos (
	promo_id varchar NULL,
	promo_name varchar(20) NULL,
	promo_type varchar(4) NULL,
	material int8 NULL,
	promo_size int8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;

CREATE TABLE default.promo_types (
	promo_type varchar(4) NULL,
	"text" text NULL
) 
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;
