
CREATE TABLE twitter.tweet_metrics (
	created_at TIMESTAMPTZ, 
	edit_history_tweet_ids VARCHAR(2000), 
	followers BIGINT, 
	following BIGINT, 
	impression_count BIGINT, 
	insert_dttm TIMESTAMPTZ, 
	like_count BIGINT, 
	quote_count BIGINT, 
	reply_count BIGINT, 
	retweet_count BIGINT, 
	text VARCHAR(2000), 
	tweet_id VARCHAR(256), 
	hashttags VARCHAR(2000)
)

