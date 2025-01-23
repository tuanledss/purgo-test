
CREATE TABLE tpch.lineitem (
	l_orderkey BIGINT NOT NULL, 
	l_partkey BIGINT NOT NULL, 
	l_suppkey INTEGER NOT NULL, 
	l_linenumber INTEGER NOT NULL, 
	l_quantity NUMERIC(12, 2) NOT NULL, 
	l_extendedprice NUMERIC(12, 2) NOT NULL, 
	l_discount NUMERIC(12, 2) NOT NULL, 
	l_tax NUMERIC(12, 2) NOT NULL, 
	l_returnflag CHAR(1) NOT NULL, 
	l_linestatus CHAR(1) NOT NULL, 
	l_shipdate DATE NOT NULL, 
	l_commitdate DATE NOT NULL, 
	l_receiptdate DATE NOT NULL, 
	l_shipinstruct CHAR(25) NOT NULL, 
	l_shipmode CHAR(10) NOT NULL, 
	l_comment VARCHAR(44) NOT NULL, 
	CONSTRAINT lineitem_pkey PRIMARY KEY (l_orderkey, l_linenumber)
) DISTSTYLE KEY DISTKEY (l_orderkey) SORTKEY (l_shipdate, l_orderkey)

