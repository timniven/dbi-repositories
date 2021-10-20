import logging
from typing import Any, Dict, Generator, Optional

import pymongo

from dbi_repositories.base import Repository


class MongoRepository(Repository):
    # re connection handling: https://pymongo.readthedocs.io/en/stable/faq.html#how-does-connection-pooling-work-in-pymongo
    # make one pymongo.MongoClient / process, let it handle multiple connections
    # don't make a new client per request (very inefficient)

    def __init__(self,
                 host: str,
                 port: int,
                 username: str,
                 password: str,
                 db_name: str,
                 collection_name: str,
                 _id_attr: Optional[str] = None):
        super().__init__()

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.collection_name = collection_name
        self._id_attr = _id_attr

        self.client = pymongo.MongoClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password)

        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def add(self, item: Dict, **kwargs) -> None:
        if self._id_attr:
            item['_id'] = item[self._id_attr]
        self.collection.insert_one(item)

    def all(self, **kwargs) -> Generator:
        cursor = self.collection.find({})
        for x in cursor:
            yield x

    def commit(self):
        # not relevant
        logging.warning('commit() called on MongoRepository, '
                        'but it is not defined for MongoDb. '
                        'Check the behavior is as you expect.')

    def connect(self):
        # done in the constructor, and no need to use __enter__
        pass

    def count(self) -> int:
        return self.collection.estimated_document_count()

    def delete(self, key: Any, **kwargs):
        self.collection.delete_one({'_id': key})

    def dispose(self):
        # no need to dispose here
        pass

    def exists(self, *args, **kwargs) -> bool:
        generator = self.search(kwargs)
        exists = True
        try:
            _ = next(generator)
        except StopIteration:
            exists = False
        return exists

    def get(self, key: Any, **kwargs):
        cursor = self.search({'_id': key})
        try:
            item = next(cursor)
        except StopIteration:
            item = None
        return item

    def search(self, *args, **kwargs):
        cursor = self.collection.find(kwargs)
        for x in cursor:
            yield x
