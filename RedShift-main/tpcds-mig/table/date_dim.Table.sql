
CREATE TABLE tpcds.date_dim (
  d_date_sk INT NOT NULL, 
  d_date_id STRING NOT NULL, 
  d_date DATE, 
  d_month_seq INT, 
  d_week_seq INT, 
  d_quarter_seq INT, 
  d_year INT, 
  d_dow INT, 
  d_moy INT, 
  d_dom INT, 
  d_qoy INT, 
  d_fy_year INT, 
  d_fy_quarter_seq INT, 
  d_fy_week_seq INT, 
  d_day_name STRING, 
  d_quarter_name STRING, 
  d_holiday STRING, 
  d_weekend STRING, 
  d_following_holiday STRING, 
  d_first_dom INT, 
  d_last_dom INT, 
  d_same_day_ly INT, 
  d_same_day_lq INT, 
  d_current_day STRING, 
  d_current_week STRING, 
  d_current_month STRING, 
  d_current_quarter STRING, 
  d_current_year STRING
)
USING DELTA
TBLPROPERTIES ('primaryKey' = 'd_date_sk')
