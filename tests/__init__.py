import os
import time

import psycopg2


def wait_for_pgsql():
    ready = False
    while not ready:
        try:
            conn = psycopg2.connect(
                host=os.environ['PGSQL_HOST'],
                port=int(os.environ['PGSQL_PORT']),
                user=os.environ['PGSQL_USERNAME'],
                password=os.environ['PGSQL_PASSWORD'],
                dbname=os.environ['PGSQL_DB_NAME'])
            conn.close()
            ready = True
        except psycopg2.OperationalError:
            print('Waiting for PGSQL...')
            time.sleep(0.1)


wait_for_pgsql()
