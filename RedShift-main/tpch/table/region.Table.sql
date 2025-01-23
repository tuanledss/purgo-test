
CREATE TABLE tpch.region (
	r_regionkey INTEGER NOT NULL, 
	r_name CHAR(25) NOT NULL, 
	r_comment VARCHAR(152) NOT NULL, 
	CONSTRAINT region_pkey PRIMARY KEY (r_regionkey)
) DISTSTYLE KEY DISTKEY (r_regionkey) SORTKEY (r_regionkey)

