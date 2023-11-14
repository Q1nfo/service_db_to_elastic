import logging
from functools import wraps

import backoff
import psycopg2
from psycopg2.extras import DictCursor
from pydantic_settings import BaseSettings

from state import JsonFileStorage, State


class Settings(BaseSettings):
    INTERVAL_QUERY: int = 30
    LIMIT: int = 100
    FILE_STATE: str = 'data_storage'
    FILE_LOG: str = 'etl.log'
    URL_ES: str = 'http://localhost:9200/_bulk'
    ES_INDEX_NAME: str = 'movies'

    DB_NAME: str = 'movies_yandex'
    USER: str = 'postgres'
    PASSWORD: str = '12345'
    HOST: str = '127.0.0.1'
    PORT: int = 5432

    logging.basicConfig(
        filename=FILE_LOG,
        format='%(asctime)s %(clientip)-15s %(user)-8s %(message)s',
        filemode='w',
        level=logging.DEBUG
    )

    state: State = State(JsonFileStorage(FILE_STATE))
    logger: logging.getLoggerClass() = logging.getLogger()


settings = Settings()


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


@backoff.on_exception(backoff.expo,
                      Exception,
                      jitter=None,
                      logger=settings.logger,
                      max_tries=10)
def connection_postgres(settings):
    """
    Подключение к POSTGRES.
    В случае неудачи произойдет повторное подключение в соответсвии с модулем backoff
    :param settings:
    :return:
    """
    dsl = {'dbname': 'movies_yandex', 'user': 'postgres', 'password': 12345, 'host': '127.0.0.1', 'port': 5432}

    return psycopg2.connect(**dsl, cursor_factory=DictCursor)
