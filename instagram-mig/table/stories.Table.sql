
CREATE TABLE instagram.stories (
  id BIGINT,
  taps_forward INT,
  taps_back INT,
  replies INT,
  reach INT,
  exits INT,
  follows INT,
  `total interactions` INT,
  shares INT
) USING DELTA
