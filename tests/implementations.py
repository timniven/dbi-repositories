import os
from typing import Dict, Optional, List, Union

from dbi_repositories.mongo import get_client, MongoRepository
from dbi_repositories.postgres import PostgresRepository


class TweetPgsqlRepository(PostgresRepository):

    def __init__(self, table: str = 'tweet'):
        super().__init__(
            host=os.environ['PGSQL_HOST'],
            port=int(os.environ['PGSQL_PORT']),
            user=os.environ['PGSQL_USERNAME'],
            password=os.environ['PGSQL_PASSWORD'],
            db_name=os.environ['PGSQL_DB_NAME'],
            table=table,
            create_table_if_not_exists=True)

    def _table_definition(self) -> str:
        return """CREATE TABLE IF NOT EXISTS TABLE_NAME (
            tweet_id INT PRIMARY KEY,
            tweet VARCHAR(500) NOT NULL
        );"""

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


class TweetMongoRepository(MongoRepository):

    def __init__(self, collection_name: str):
        client = get_client(
            host=os.environ['MONGO_HOST'],
            port=int(os.environ['MONGO_PORT']),
            username=os.environ['MONGO_USERNAME'],
            password=os.environ['MONGO_PASSWORD'])
        super().__init__(
            client=client,
            db_name='test_mongo_twitter',
            collection_name=collection_name,
            _id_attr='id')
