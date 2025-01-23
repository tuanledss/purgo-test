
CREATE TABLE tpch.partsupp (
	ps_partkey BIGINT NOT NULL, 
	ps_suppkey INTEGER NOT NULL, 
	ps_availqty INTEGER NOT NULL, 
	ps_supplycost NUMERIC(12, 2) NOT NULL, 
	ps_comment VARCHAR(199) NOT NULL, 
	CONSTRAINT partsupp_pkey PRIMARY KEY (ps_partkey, ps_suppkey)
) DISTSTYLE KEY DISTKEY (ps_partkey) SORTKEY (ps_partkey)

