
CREATE TABLE twitter.followers (
	created_date TIMESTAMPTZ, 
	description VARCHAR(1000), 
	display_name VARCHAR(256), 
	followers_count INTEGER, 
	following_count INTEGER, 
	insert_dttm TIMESTAMPTZ, 
	listed_count INTEGER, 
	profile_image_url VARCHAR(256), 
	tweet_count INTEGER, 
	user_id VARCHAR(256), 
	user_location VARCHAR(256), 
	user_name VARCHAR(256), 
	user_verified BOOLEAN
)

