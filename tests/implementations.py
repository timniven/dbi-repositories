import os
from typing import Optional, List

from dbi_repositories.postgres import PostgresRepository


class TweetRepository(PostgresRepository):

    def __init__(self, table: str = 'tweet'):
        super().__init__(
            host=os.environ['PGSQL_HOST'],
            port=int(os.environ['PGSQL_PORT']),
            user=os.environ['PGSQL_USERNAME'],
            password=os.environ['PGSQL_PASSWORD'],
            db_name=os.environ['PGSQL_DB_NAME'],
            table=table)
        self._create_table()

    def _create_table(self):
        sql = f'CREATE TABLE IF NOT EXISTS {self.table} ( ' \
              f'tweet_id INT PRIMARY KEY, ' \
              f'tweet VARCHAR(500) NOT NULL );'
        self._execute(sql)

    def add(self, tweet_id: int, tweet: str) -> None:
        super().add(tweet_id=tweet_id, tweet=tweet)

    def all(self, projection: Optional[List[str]] = None):
        return super().all(projection=projection)

    def delete(self, tweet_id: int) -> None:
        super().delete(tweet_id=tweet_id)

    def exists(self, tweet_id: int):
        return super().exists(tweet_id=tweet_id)

    def get(self, tweet_id: int, projection: Optional[List[str]] = None):
        return super().get(tweet_id=tweet_id, projection=projection)

    def search(self,
               tweet_id: Optional[int] = None,
               tweet: Optional[str] = None,
               projection: Optional[List[str]] = None):
        return super().search(
            tweet_id=tweet_id,
            tweet=tweet,
            projection=projection)
