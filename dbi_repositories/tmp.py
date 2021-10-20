from datetime import date, datetime
import os
from typing import Dict, List, NoReturn, Optional, Union

import pymongo
from pymongo.errors import DuplicateKeyError


class TwitterMongoOp:

    def __init__(self, client):
        self.client = client
        self.db = self.client.get_database(os.environ['TWITTER_MONGO_DB_NAME'])
        self.collection = self.db.get_collection(
            os.environ['TWITTER_MONGO_COLLECTION_NAME'])
        self.page_size = os.environ['PAGE_SIZE']


class AddTweet(TwitterMongoOp):

    def __call__(self,
                 tweet: Dict,
                 error_if_duplicate: bool = False) -> NoReturn:
        try:
            self.collection.insert_one(tweet)
        except DuplicateKeyError as e:
            if error_if_duplicate:
                raise e
            else:
                pass


class GetTweet(TwitterMongoOp):

    def __call__(self, tweet_id: int) -> Union[Dict, None]:
        cursor = self.collection.find({'_id': tweet_id})
        return next(cursor)


class GetTweetsByKwargs(TwitterMongoOp):

    def __call__(self, **kwargs) -> List[Dict]:
        cursor = self.collection.find(kwargs)
        return [x for x in cursor]


class GetTweetsByScreenName(TwitterMongoOp):

    def __call__(self,
                 screen_name: str,
                 start: Optional[date] = None,
                 end: Optional[date] = None,
                 page: int = 1) -> List[Dict]:
        params = {'screen_name': screen_name}
        if start or end:
            params['timestamp'] = {}
        if start:
            start = datetime.combine(start, datetime.min.time()).timestamp()
            params['timestamp']['$gte'] = start
        if end:
            end = datetime.combine(end, datetime.min.time()).timestamp()
            params['timestamp']['$lte'] = end
        cursor = self.collection \
            .find(params) \
            .skip(int(self.page_size) * (page - 1)) \
            .limit(int(self.page_size))
        tweets = [x for x in cursor]
        return tweets


class GetAccountFirstDataDateTime(TwitterMongoOp):

    def __call__(self, screen_name: str) -> Union[datetime, None]:
        cursor = self.collection \
            .find({'screen_name': screen_name}) \
            .sort([('timestamp', pymongo.ASCENDING)]) \
            .limit(1)
        record = next(cursor)
        if not record:
            return None
        date_time = datetime.fromtimestamp(record['timestamp'])
        return date_time


class GetAccountLastDataDateTime(TwitterMongoOp):

    def __call__(self, screen_name: str) -> Union[datetime, None]:
        cursor = self.collection \
            .find({'screen_name': screen_name}) \
            .sort([('timestamp', pymongo.DESCENDING)]) \
            .limit(1)
        record = next(cursor)
        if not record:
            return None
        date_time = datetime.fromtimestamp(record['timestamp'])
        return date_time


class TwitterMongo:
    """Facade class wrapping the ops."""

    def __init__(self):
        # Create this client once for each process, and reuse it for all
        # operations. It is a common mistake to create a new client for each
        # request, which is very inefficient.
        # https://pymongo.readthedocs.io/en/stable/faq.html#how-does-connection-pooling-work-in-pymongo
        self.client = pymongo.MongoClient(
            host=os.environ['TWITTER_MONGO_HOST'],
            port=int(os.environ['TWITTER_MONGO_PORT']),
            username=os.environ['TWITTER_MONGO_USERNAME'],
            password=os.environ['TWITTER_MONGO_PASSWORD'],
            authSource='admin',
            authMechanism='SCRAM-SHA-256')

        # functions
        self.add_tweet = AddTweet(self.client)
        self.get_tweet = GetTweet(self.client)
        self.get_tweets_by_kwargs = GetTweetsByKwargs(self.client)
        self.get_tweets_by_screen_name = GetTweetsByScreenName(self.client)
        self.get_account_first_data_datetime = \
            GetAccountFirstDataDateTime(self.client)
        self.get_account_last_data_datetime = \
            GetAccountLastDataDateTime(self.client)
