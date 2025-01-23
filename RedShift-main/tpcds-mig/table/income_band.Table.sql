
CREATE TABLE tpcds.income_band (
    ib_income_band_sk INT NOT NULL, 
    ib_lower_bound INT, 
    ib_upper_bound INT
) 
USING DELTA;

ALTER TABLE tpcds.income_band
ADD CONSTRAINT income_band_pkey PRIMARY KEY (ib_income_band_sk);
