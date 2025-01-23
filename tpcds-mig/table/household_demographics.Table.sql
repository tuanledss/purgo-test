
CREATE TABLE tpcds.household_demographics (
	hd_demo_sk INT NOT NULL, 
	hd_income_band_sk INT, 
	hd_buy_potential STRING, 
	hd_dep_count INT, 
	hd_vehicle_count INT
) USING DELTA;

ALTER TABLE tpcds.household_demographics ADD CONSTRAINT household_demographics_pkey PRIMARY KEY (hd_demo_sk);

