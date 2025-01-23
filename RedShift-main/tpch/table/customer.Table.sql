
CREATE TABLE tpch.customer (
	c_custkey BIGINT NOT NULL, 
	c_name VARCHAR(25) NOT NULL, 
	c_address VARCHAR(40) NOT NULL, 
	c_nationkey INTEGER NOT NULL, 
	c_phone CHAR(15) NOT NULL, 
	c_acctbal NUMERIC(12, 2) NOT NULL, 
	c_mktsegment CHAR(10) NOT NULL, 
	c_comment VARCHAR(117) NOT NULL, 
	CONSTRAINT customer_pkey PRIMARY KEY (c_custkey)
) DISTSTYLE KEY DISTKEY (c_custkey) SORTKEY (c_custkey)

