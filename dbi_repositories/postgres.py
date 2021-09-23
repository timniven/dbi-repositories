from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import psycopg2
from psycopg2.extras import RealDictCursor

from dbi_repositories.base import Repository


class PostgresRepository(Repository):

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 db_name: str,
                 table: str):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.table = table
        self.conn = None
        self.cursor = None

    def _execute(self, sql: str, values: Optional[List[Any]] = None):
        if self.cursor:
            self.cursor.execute(sql, values)
            return self.cursor
        else:
            with self._get_connection() as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(sql, values)
                return cursor

    @staticmethod
    def _get_conditions_and_values(**kwargs) \
            -> Tuple[str, List[Any]]:
        conditions = []
        values = []
        for attr, value in kwargs.items():
            if value:  # sometimes values can be None - don't take those
                conditions.append(f'{attr} = %s')
                values.append(value)
        conditions = ' AND '.join(conditions)
        return conditions, values

    def _get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.db_name)

    @staticmethod
    def _get_selector(**kwargs) -> str:
        selector = '*'
        if 'projection' in kwargs and kwargs['projection']:
            selector = ','.join(kwargs['projection'])
        return selector

    def _iterate(self, cursor) -> Generator:
        for item in cursor:
            yield item

    def add(self, *args, **kwargs):
        attrs = []
        values = []
        for attr, value in kwargs.items():
            attrs.append(attr)
            values.append(value)
        attrs = ','.join(attrs)
        value_placeholders = ['%s'] * len(values)
        value_placeholders = ','.join(value_placeholders)
        sql = f'INSERT INTO {self.table} ' \
              f'({attrs}) ' \
              f'VALUES ({value_placeholders});'
        _ = self._execute(sql, values)

    def all(self, **kwargs) -> Generator:
        selector = self._get_selector(**kwargs)
        sql = f'SELECT {selector} FROM {self.table};'
        cursor = self._execute(sql)
        for item in cursor:
            yield item

    def commit(self):
        if self.conn:
            self.conn.commit()

    def connect(self):
        self.conn = self._get_connection()
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def dispose(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None

    def count(self) -> int:
        sql = f'SELECT COUNT(*) FROM {self.table};'
        cursor = self._execute(sql)
        result = cursor.fetchone()
        return result['count']

    def delete(self, *args, **kwargs):
        conditions, values = self._get_conditions_and_values(**kwargs)
        sql = f'DELETE FROM {self.table} WHERE {conditions};'
        _ = self._execute(sql, values)

    def exists(self, *args, **kwargs) -> bool:
        # NOTE: only handles `=` conditions
        conditions, values = self._get_conditions_and_values(**kwargs)
        sql = f'SELECT COUNT(*) FROM {self.table} ' \
              f'WHERE {conditions};'
        cursor = self._execute(sql, values)
        result = cursor.fetchone()
        return result['count'] > 0

    def get(self, *args, **kwargs) \
            -> Union[Dict, None]:
        items = self.search(**kwargs)
        items = list(items)
        if len(items) == 0:
            # TODO: anything to do around checking and erroring here?
            return None
        if len(items) > 1:
            raise ValueError('No unique record, could be a deeper problem.')
        return items[0]

    def search(self, *args, **kwargs) \
            -> Generator:
        # NOTE: only handles `=` conditions
        conditions, values = self._get_conditions_and_values(**kwargs)
        selector = self._get_selector(**kwargs)
        sql = f'SELECT {selector} FROM {self.table} WHERE {conditions};'
        cursor = self._execute(sql, values)
        return self._iterate(cursor)
