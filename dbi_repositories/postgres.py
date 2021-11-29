import logging
from typing import Any, Dict, Generator, List, MutableMapping, Optional, \
    Tuple, Union

import psycopg2
from psycopg2 import extensions
from psycopg2.errors import UniqueViolation
from psycopg2.extras import RealDictCursor

from dbi_repositories.base import Repository


"""
Ref:
https://www.psycopg.org/docs/usage.html#transactions-control
"""


class ConnectionFactory:

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 db_name: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name

    def __call__(self, db_name: Optional[str] = None):
        if not db_name:
            db_name = self.db_name
        return get_connection(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db_name=db_name)


def get_connection(host: str,
                   port: int,
                   user: str,
                   password: str,
                   db_name: str):
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db_name,
        sslmode='require')


def create_db(connection_factory: ConnectionFactory,
              db_name: str,
              sql_schema: str):
    connection = connection_factory()
    connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    with connection.cursor() as cursor:
        sql = f'CREATE DATABASE {db_name};'
        cursor.execute(sql)
    connection.set_isolation_level(extensions.ISOLATION_LEVEL_DEFAULT)
    connection.commit()
    connection.close()
    with connection_factory(db_name) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_schema)


class PostgresRepository(Repository):
    """Base Postgres repository.

    This is meant to be opinionated, and the opinion is that all transactions
    are atomic (single transaction). Hence `add_many` and `delete_many`.
    """

    def __init__(self,
                 connection_factory: ConnectionFactory,
                 table_name: str,
                 primary_keys: List[str]):
        super().__init__()
        self.connection_factory = connection_factory
        self.table_name = table_name
        self.primary_keys = primary_keys

    def _execute_generator_return(self,
                                  sql: str,
                                  values: Optional[List[Any]] = None) \
            -> Generator:
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, values)
                for item in cursor:
                    yield self._map_item_out(dict(item))

    def _execute_no_return(self,
                           sql: str,
                           values: Optional[List[Any]] = None) \
            -> None:
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, values)

    def _execute_single_return(self,
                               sql: str,
                               values: Optional[List[Any]] = None) -> Any:
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, values)
                item = cursor.fetchone()
                return self._map_item_out(dict(item))

    @staticmethod
    def _get_conditions_and_values(
            alias: Optional[str] = None,
            join_char: str = ', ',
            **kwargs) \
            -> Tuple[str, List[Any]]:
        conditions = []
        values = []
        for attr, value in kwargs.items():
            if alias:
                attr = f'{alias}.{attr}'
            if value:  # sometimes values can be None - don't take those
                conditions.append(f'{attr} = %s')
                values.append(value)
        conditions = join_char.join(conditions)
        return conditions, values

    @staticmethod
    def _get_selector(**kwargs) -> str:
        selector = '*'
        if 'projection' in kwargs and kwargs['projection']:
            selector = ','.join(kwargs['projection'])
        return selector

    def _get_update_sql_and_values(self,
                                   item: MutableMapping,
                                   condition_keys: List[str],
                                   update_keys: List[str],
                                   include_table_name: bool = True,
                                   alias: Optional[str] = None) \
            -> Tuple[str, List[Any]]:
        update = {k: item[k] for k in update_keys}
        where = {k: item[k] for k in condition_keys}
        update_conditions, update_values = \
            self._get_conditions_and_values(**update)
        where_conditions, where_values = \
            self._get_conditions_and_values(
                alias=alias,
                join_char=' AND ',
                **where)
        values = update_values + where_values
        sql = 'UPDATE '
        if include_table_name:
            sql += f'{self.table_name} '
        sql += f'SET {update_conditions} ' \
               f'WHERE {where_conditions};'
        return sql, values

    def _item_to_insert_statement(self,
                                  item: MutableMapping,
                                  upsert: bool = False,
                                  ignore_duplicates: bool = False) \
            -> Tuple[str, List[Any]]:
        if upsert and ignore_duplicates:
            raise ValueError('Pick one of `upsert` and `ignore_duplicates`.')
        attrs = []
        values = []
        for attr, value in item.items():
            attrs.append(attr)
            values.append(value)
        attrs = ','.join(attrs)
        value_placeholders = ['%s'] * len(values)
        value_placeholders = ','.join(value_placeholders)
        sql = f'INSERT INTO {self.table_name} AS tn ' \
              f'({attrs}) ' \
              f'VALUES ({value_placeholders})'
        if ignore_duplicates:
            primary_keys = ','.join(self.primary_keys)
            sql += f' ON CONFLICT ({primary_keys}) DO NOTHING;'
        elif upsert:
            primary_keys = ','.join(self.primary_keys)
            update_statement, update_values = self._get_update_sql_and_values(
                item=item,
                condition_keys=self.primary_keys,
                update_keys=[k for k in item.keys()
                             if k not in self.primary_keys],
                include_table_name=False,
                alias='tn')
            sql += f' ON CONFLICT ({primary_keys}) DO {update_statement}'
            values += update_values
        else:
            sql += ';'
        return sql, values

    def _map_item_in(self, item: MutableMapping) -> Dict:
        return {k: v for k, v in item.items()}

    def _map_item_out(self, item: Dict) -> MutableMapping:
        return item

    def add(self,
            item: MutableMapping,
            ignore_duplicates: bool = False,
            **kwargs) -> None:
        item = self._map_item_in(item)
        sql, values = self._item_to_insert_statement(
            item=item,
            ignore_duplicates=ignore_duplicates,
            upsert=False)
        self._execute_no_return(sql, values)

    def add_many(self,
                 items: List[MutableMapping],
                 ignore_duplicates: bool = False,
                 **kwargs):
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for item in items:
                    item = self._map_item_in(item)
                    sql, values = self._item_to_insert_statement(
                        item=item,
                        ignore_duplicates=ignore_duplicates,
                        upsert=False)
                    _ = cursor.execute(sql, values)

    def all(self, **kwargs) -> Generator:
        selector = self._get_selector(**kwargs)
        sql = f'SELECT {selector} FROM {self.table_name};'
        return self._execute_generator_return(sql)

    def commit(self) -> None:
        logging.warning(
            'commit() is not implemented for '
            'dbi_repositories.postgres.PostgresRepository. '
            'A call to this function is doing nothing and can be removed. '
            'Each base function is atomic and commits automatically.')

    def count(self) -> int:
        sql = f'SELECT COUNT(*) FROM {self.table_name};'
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                return result['count']

    def delete(self, conditions: Dict, **kwargs) -> None:
        conditions, values = self._get_conditions_and_values(**conditions)
        sql = f'DELETE FROM {self.table_name} WHERE {conditions};'
        self._execute_no_return(sql, values)

    def delete_many(self, conditions: List[Dict], **kwargs) -> None:
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for cond in conditions:
                    cond, values = self._get_conditions_and_values(**cond)
                    sql = f'DELETE FROM {self.table_name} WHERE {cond};'
                    cursor.execute(sql, values)

    def exists(self, *args, **kwargs) -> bool:
        # NOTE: only handles `=` conditions
        conditions, values = self._get_conditions_and_values(**kwargs)
        sql = f'SELECT COUNT(*) FROM {self.table_name} ' \
              f'WHERE {conditions};'
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, values)
                result = cursor.fetchone()
                return result['count'] > 0

    def get(self, *args, **kwargs) \
            -> Union[Dict, None]:
        items = self.search(**kwargs)
        try:
            item = next(items)
            return item
        except StopIteration:
            return None

    def search(self, *args, **kwargs) -> Generator:
        # NOTE: only handles `=` conditions
        conditions, values = self._get_conditions_and_values(
            join_char=' AND ',
            **kwargs)
        selector = self._get_selector(**kwargs)
        sql = f'SELECT {selector} FROM {self.table_name} WHERE {conditions};'
        return self._execute_generator_return(sql, values)

    def update(self,
               item: MutableMapping,
               condition_keys: List[str],
               update_keys: List[str]) -> None:
        sql, values = self._get_update_sql_and_values(
            item, condition_keys, update_keys)
        self._execute_no_return(sql, values)

    def update_many(self,
                    items: List[MutableMapping],
                    condition_keys: List[str],
                    update_keys: List[str]) -> None:
        with self.connection_factory() as conn:
            with conn.cursor() as cursor:
                for item in items:
                    sql, values = self._get_update_sql_and_values(
                        item, condition_keys, update_keys)
                    cursor.execute(sql, values)

    def upsert(self, item: MutableMapping, **kwargs) -> None:
        item = self._map_item_in(item)
        sql, values = self._item_to_insert_statement(item, upsert=True)
        self._execute_no_return(sql, values)

    def upsert_many(self, items: List[MutableMapping], **kwargs) -> None:
        with self.connection_factory() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                for item in items:
                    item = self._map_item_in(item)
                    sql, values = self._item_to_insert_statement(
                        item, upsert=True)
                    _ = cursor.execute(sql, values)
