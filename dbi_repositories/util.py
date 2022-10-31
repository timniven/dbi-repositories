import time
from typing import List

import psycopg2


def get_chunks(items: List, n: int):
    """Yield successive n-sized chunks from lst.

    https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    """
    for i in range(0, len(items), n):
        yield items[i:i + n]


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
