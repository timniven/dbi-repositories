import os

import psycopg2

from dbi_repositories import util


def connection_factory():
    conn = psycopg2.connect(
        host=os.environ['PGSQL_HOST'],
        port=int(os.environ['PGSQL_PORT']),
        user=os.environ['PGSQL_USERNAME'],
        password=os.environ['PGSQL_PASSWORD'],
        dbname=os.environ['PGSQL_DB_NAME'])
    return conn


util.wait_for_pgsql(connection_factory)
