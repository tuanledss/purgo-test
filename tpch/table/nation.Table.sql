
CREATE TABLE tpch.nation (
	n_nationkey INTEGER NOT NULL, 
	n_name CHAR(25) NOT NULL, 
	n_regionkey INTEGER NOT NULL, 
	n_comment VARCHAR(152) NOT NULL, 
	CONSTRAINT nation_pkey PRIMARY KEY (n_nationkey)
) DISTSTYLE KEY DISTKEY (n_nationkey) SORTKEY (n_nationkey)

