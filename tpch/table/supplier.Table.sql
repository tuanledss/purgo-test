
CREATE TABLE tpch.supplier (
	s_suppkey INTEGER NOT NULL, 
	s_name CHAR(25) NOT NULL, 
	s_address VARCHAR(40) NOT NULL, 
	s_nationkey INTEGER NOT NULL, 
	s_phone CHAR(15) NOT NULL, 
	s_acctbal NUMERIC(12, 2) NOT NULL, 
	s_comment VARCHAR(101) NOT NULL, 
	CONSTRAINT supplier_pkey PRIMARY KEY (s_suppkey)
) DISTSTYLE KEY DISTKEY (s_suppkey) SORTKEY (s_suppkey)

