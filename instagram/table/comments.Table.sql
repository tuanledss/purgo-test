
CREATE TABLE instagram.comments (
	post_id BIGINT, 
	post_timestamp TIMESTAMPTZ, 
	text VARCHAR(256), 
	comments_id BIGINT
)

