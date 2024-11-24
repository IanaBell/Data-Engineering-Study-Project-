CREATE TABLE std7_111.traffic (
	plant varchar(4) NULL,
	"date" varchar(10) NULL,
	"time" varchar(6) NULL,
	frame_id varchar(10) NULL,
	quantity int4 NULL
) 
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY;

CREATE TABLE std7_111.bills_head (
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
DISTRIBUTED BY (billnum);

CREATE TABLE std7_111.bills_item (
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
DISTRIBUTED BY (billnum);

CREATE TABLE std7_111.stores (
	plant varchar(4) NULL,
	txt text NULL
)
DISTRIBUTED REPLICATED;

CREATE TABLE std7_111.coupons (
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
DISTRIBUTED RANDOMLY;

CREATE TABLE std7_111.promos (
	promo_id varchar NULL,
	promo_name varchar(20) NULL,
	promo_type varchar(4) NULL,
	material int8 NULL,
	promo_size int8 NULL
)
DISTRIBUTED REPLICATED;

CREATE TABLE std7_111.promo_types (
	promo_type varchar(4) NULL,
	"text" text NULL
) 
DISTRIBUTED REPLICATED;

