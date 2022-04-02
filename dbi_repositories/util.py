import time

import psycopg2


def wait_for_pgsql(connection_factory, sleep_for: float = 0.1):
    ready = False
    while not ready:
        try:
            conn = connection_factory()
            conn.close()
            ready = True
        except psycopg2.OperationalError:
            print('Waiting for PGSQL...')
            time.sleep(sleep_for)
