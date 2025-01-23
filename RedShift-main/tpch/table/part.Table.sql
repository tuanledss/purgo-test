
CREATE TABLE tpch.part (
	p_partkey BIGINT NOT NULL, 
	p_name VARCHAR(55) NOT NULL, 
	p_mfgr CHAR(25) NOT NULL, 
	p_brand CHAR(10) NOT NULL, 
	p_type VARCHAR(25) NOT NULL, 
	p_size INTEGER NOT NULL, 
	p_container CHAR(10) NOT NULL, 
	p_retailprice NUMERIC(12, 2) NOT NULL, 
	p_comment VARCHAR(23) NOT NULL, 
	CONSTRAINT part_pkey PRIMARY KEY (p_partkey)
) DISTSTYLE KEY DISTKEY (p_partkey) SORTKEY (p_partkey)

