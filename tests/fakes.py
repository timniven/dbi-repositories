import os
from typing import Optional, List

from dbi_repositories.postgres import PostgresRepository


class TweetRepository(PostgresRepository):

    def __init__(self):
        super().__init__(
            host=os.environ['PGSQL_HOST'],
            port=int(os.environ['PGSQL_PORT']),
            user=os.environ['PGSQL_USERNAME'],
            password=os.environ['PGSQL_PASSWORD'],
            db_name=os.environ['PGSQL_DB_NAME'],
            table='tweets')

    def add(self, tweet_id: int, tweet: str):
        super().add(tweet_id=tweet_id, tweet=tweet)

    def delete(self, tweet_id: int):
        super().delete(tweet_id=tweet_id)

    def exists(self, tweet_id: int, tweet: str):
        super().exists(tweet_id=tweet_id, tweet=tweet)

    def get(self, tweet_id: int, projection: Optional[List[str]] = None):
        pass

    def search(self, tweet_id: int, tweet: str):
        pass
