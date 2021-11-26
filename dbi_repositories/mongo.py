from collections.abc import MutableMapping
import logging
from typing import Any, Dict, Generator, List, Optional

import pymongo
from pymongo.errors import BulkWriteError, DuplicateKeyError

from dbi_repositories.base import Repository


def get_client(host: str, port: int, username: str, password: str) \
        -> pymongo.MongoClient:
    return pymongo.MongoClient(
        host=host,
        port=port,
        username=username,
        password=password)


class MongoRepository(Repository):
    # NOTE: construct here has a client, because in the usual case where you
    # can have more than one collection/repo, you should still use the same
    # single connection per thread, so share them around...

    def __init__(self,
                 client: pymongo.MongoClient,
                 db_name: str,
                 collection_name: str,
                 _id_attr: Optional[str] = None):
        super().__init__()

        self.db_name = db_name
        self.collection_name = collection_name
        self._id_attr = _id_attr

        self.client = client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def add(self,
            item: MutableMapping,
            error_duplicates: bool = False,
            **kwargs) -> None:
        if self._id_attr:
            item['_id'] = item[self._id_attr]
        try:
            self.collection.insert_one(item)
        except DuplicateKeyError as e:
            if error_duplicates:
                raise e

    def add_many(self,
                 items: List[MutableMapping],
                 error_duplicates: bool = False,
                 **kwargs) -> None:
        if self._id_attr:
            for item in items:
                item['_id'] = item[self._id_attr]
        try:
            # NOTE: ordered=False reason: if ordered, the documents will be
            # inserted in the order supplied, if False, they will all be tried
            # in parallel, so the only ones that fail are the ones that are
            # supposed to, so the way this function works is to insert all
            # legitimate items.
            self.collection.insert_many(items, ordered=False)
        except BulkWriteError as e:
            duplicate_error = 'duplicate key error' in e.__dict__['_message']
            if duplicate_error and error_duplicates:
                raise e

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
        item = self.collection.find_one({'_id': key})
        return item

    def update(self, item: MutableMapping, **kwargs):
        self.collection.replace_one(
            filter={'_id': item['_id']},
            replacement=item,
            upsert=False)

    def update_attributes(self, key: Any, **kwargs):
        self.collection.update_one(
            filter={'_id': key},
            update={'$set': kwargs})

    def search(self, *args, **kwargs):
        cursor = self.collection.find(kwargs)
        for x in cursor:
            yield x

    def upsert(self, item: MutableMapping, **kwargs):
        self.collection.replace_one(
            filter={'_id': item['_id']},
            replacement=item,
            upsert=False)

    def update_many(self, items: List[MutableMapping], **kwargs):
        raise NotImplementedError('Have not found a good way yet.')
