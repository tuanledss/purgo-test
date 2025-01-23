
CREATE TABLE tpch.orders (
	o_orderkey BIGINT NOT NULL, 
	o_custkey BIGINT NOT NULL, 
	o_orderstatus CHAR(1) NOT NULL, 
	o_totalprice NUMERIC(12, 2) NOT NULL, 
	o_orderdate DATE NOT NULL, 
	o_orderpriority CHAR(15) NOT NULL, 
	o_clerk CHAR(15) NOT NULL, 
	o_shippriority INTEGER NOT NULL, 
	o_comment VARCHAR(79) NOT NULL, 
	CONSTRAINT orders_pkey PRIMARY KEY (o_orderkey)
) DISTSTYLE KEY DISTKEY (o_orderkey) SORTKEY (o_orderdate, o_orderkey)

