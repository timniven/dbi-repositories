CREATE TABLE IF NOT EXISTS tweet (
    tweet_id INT PRIMARY KEY,
    tweet VARCHAR(500) NOT NULL,
    label VARCHAR(3) NULL
);
