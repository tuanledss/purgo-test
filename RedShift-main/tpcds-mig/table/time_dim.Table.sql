
CREATE TABLE tpcds.time_dim (
    t_time_sk INT NOT NULL, 
    t_time_id STRING NOT NULL, 
    t_time INT, 
    t_hour INT, 
    t_minute INT, 
    t_second INT, 
    t_am_pm STRING, 
    t_shift STRING, 
    t_sub_shift STRING, 
    t_meal_time STRING
);

ALTER TABLE tpcds.time_dim ADD CONSTRAINT time_dim_pkey PRIMARY KEY (t_time_sk);
