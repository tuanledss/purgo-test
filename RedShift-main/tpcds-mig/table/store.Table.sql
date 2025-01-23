
CREATE TABLE tpcds.store (
  s_store_sk INT NOT NULL,
  s_store_id STRING NOT NULL,
  s_rec_start_date DATE,
  s_rec_end_date DATE,
  s_closed_date_sk INT,
  s_store_name STRING,
  s_number_employees INT,
  s_floor_space INT,
  s_hours STRING,
  s_manager STRING,
  s_market_id INT,
  s_geography_class STRING,
  s_market_desc STRING,
  s_market_manager STRING,
  s_division_id INT,
  s_division_name STRING,
  s_company_id INT,
  s_company_name STRING,
  s_street_number STRING,
  s_street_name STRING,
  s_street_type STRING,
  s_suite_number STRING,
  s_city STRING,
  s_county STRING,
  s_state STRING,
  s_zip STRING,
  s_country STRING,
  s_gmt_offset DECIMAL(5, 2),
  s_tax_precentage DECIMAL(5, 2)
)
USING DELTA;

ALTER TABLE tpcds.store ADD CONSTRAINT store_pkey PRIMARY KEY (s_store_sk);
