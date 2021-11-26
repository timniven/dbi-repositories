from datetime import datetime
import os
from typing import Dict, Optional, List

from dbi_repositories.mongo import get_client, MongoRepository
from dbi_repositories.postgres import ConnectionFactory, create_db, \
    PostgresRepository


with open('docker/provisions/postgres/startup/test_schema.sql') as f:
    test_schema = f.read().strip()


def get_test_connection_factory(db_name: str = os.environ['PGSQL_DB_NAME']) \
        -> ConnectionFactory:
    return ConnectionFactory(
        host=os.environ['PGSQL_HOST'],
        port=int(os.environ['PGSQL_PORT']),
        user=os.environ['PGSQL_USERNAME'],
        password=os.environ['PGSQL_PASSWORD'],
        db_name=db_name)


def create_test_database(db_name: str, schema: str = test_schema):
    connection_factory = get_test_connection_factory()
    create_db(connection_factory, db_name, schema)


class TweetPgsqlRepository(PostgresRepository):

    def __init__(self, db_name: str):
        super().__init__(
            connection_factory=get_test_connection_factory(db_name=db_name),
            table_name='tweet',
            primary_keys=['tweet_id'])

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


class TweetStatsRepository(PostgresRepository):

    def __init__(self, db_name: str):
        super().__init__(
            connection_factory=get_test_connection_factory(db_name=db_name),
            table_name='tweet_stats',
            primary_keys=['tweet_id', 'collected_at'])

    def get(self, tweet_id: int, collected_at: datetime):
        return super().get(tweet_id=tweet_id, collected_at=collected_at)


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
