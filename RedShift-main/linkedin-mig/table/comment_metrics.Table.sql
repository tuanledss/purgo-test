
CREATE TABLE linkedin.comment_metrics (
	comment STRING, 
	likes INT, 
	post_url STRING, 
	comment_date DATE
) USING DELTA
