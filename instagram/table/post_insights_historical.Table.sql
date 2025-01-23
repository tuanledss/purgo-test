
CREATE TABLE instagram.post_insights_historical (
	caption VARCHAR(1000), 
	comments_count INTEGER, 
	like_count INTEGER, 
	media_product_type VARCHAR(256), 
	media_type VARCHAR(256), 
	media_url VARCHAR(1000), 
	is_comment_enabled VARCHAR(1000), 
	permalink VARCHAR(256), 
	post_timestamp TIMESTAMP WITHOUT TIME ZONE, 
	id BIGINT, 
	plays VARCHAR(256), 
	total_interactions VARCHAR(256), 
	is_shared_to_feed BOOLEAN
)

