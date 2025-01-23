
CREATE TABLE tpcds.customer_address (
    ca_address_sk INT NOT NULL, 
    ca_address_id STRING NOT NULL, 
    ca_street_number STRING, 
    ca_street_name STRING, 
    ca_street_type STRING, 
    ca_suite_number STRING, 
    ca_city STRING, 
    ca_county STRING, 
    ca_state STRING, 
    ca_zip STRING, 
    ca_country STRING, 
    ca_gmt_offset DECIMAL(5, 2), 
    ca_location_type STRING,
    PRIMARY KEY (ca_address_sk)
) USING DELTA

