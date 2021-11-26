CREATE TABLE IF NOT EXISTS tweet (
    tweet_id INT PRIMARY KEY,
    tweet VARCHAR(500) NOT NULL,
    label VARCHAR(3) NULL
);

CREATE TABLE IF NOT EXISTS tweet_stats (
    tweet_id INT,
    collected_at TIMESTAMP,
    num_likes INT,
    PRIMARY KEY (tweet_id, collected_at)
);
