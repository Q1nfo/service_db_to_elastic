import json
from datetime import datetime
from pprint import pprint
from typing import Tuple, List, Coroutine, Any

import backoff
import requests
from psycopg2.extras import RealDictCursor

from service import settings, Settings, connection_postgres, coroutine


class ESLoader:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = self.settings.logger

    @backoff.on_exception(backoff.expo,
                          Exception,
                          jitter=None,
                          logger=settings.logger,
                          max_tries=10)
    def request_post(self, query: str) -> str:
        """
        Отправка запроса в ES и возврат ответа
        """
        print('START REQUEST')

        return requests.post(
            self.settings.URL_ES,
            data=query,
            headers={
                'Content-type': 'application/x-ndjson'
            }
        ).content.decode()

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        """
        подготовка bulk запроса в Elasticsearch
        """
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps({'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str) -> None:
        """
        Отправка запроса и разбор его ошибок
        """
        print('start WORK')
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        content = self.request_post(str_query)
        json_response = json.loads(content)

        print(json_response)

        # for item in json_response['items']:
        #     error_message = item['index'].get('error')
        #     if error_message:
        #         self.logger.error(error_message)


class ETL:
    def __init__(self, es_loader: ESLoader, pg_conn, db_table: str):
        self.es_loader = es_loader
        self.settings = es_loader.settings
        self.logger = self.settings.logger
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor(cursor_factory=RealDictCursor)
        self.db_table = db_table

    @backoff.on_exception(backoff.expo,
                          Exception,
                          jitter=None,
                          logger=settings.logger,
                          max_tries=10)
    def execute_query(self, query: str, params: Tuple[str]) -> List[dict]:
        """
        Выполнение запроса к POSTGRES.
        В случае ошибки, проверка соединения и повторное попытка в соответсвии с модулем backoff
        :param query:
        :param params:
        :return:
        """
        if not self.pg_conn or self.pg_conn.closed:
            self.pg_conn = connection_postgres(self.settings)
            self.cursor = self.pg_conn.cursor(cursor_factory=RealDictCursor)

        self.cursor.execute(query, params)
        print('SOME Execute')
        return self.cursor.fetchall()

    def postgres_producer(self, target: Coroutine) -> None:
        """
        Получает обновленные данные пачками из переданной таблицы
        при старте получает из постоянного хранилища updated_at
        сохраняет updated_at в постоянное хранилище перед завершение работы
        :param target:
        :return:
        """
        updated_at = self.settings.state.get_state(f'{self.db_table}_updated_at')
        if updated_at:
            updated_at = datetime.fromisoformat(updated_at)
        else:
            updated_at = datetime(1, 1, 1, 0, 0, 0, )

        while True:
            query = f"""
                SELECT
                    tbl.id,
                    tbl.updated_at
                FROM content.{self.db_table} as tbl
                WHERE tbl.updated_at > %s
                ORDER BY tbl.updated_at
                LIMIT {self.settings.LIMIT};
            """

            data = []
            result = self.execute_query(query, (updated_at,))
            for row in result:
                data.append(
                    row['id']
                )
            if data:
                updated_at = result[-1]['updated_at']
                target.send(tuple(data))
                self.settings.state.set_state(f'{self.db_table}_updated_at', updated_at.isoformat())
            else:
                break

    # ДОБАВЛЯЕТ ВСЕ ПОЛЯ
    # def postgres_producer(self, target: Coroutine) -> None:
    #     """
    #     Получает обновленные данные пачками из переданной таблицы
    #     при старте получает из постоянного хранилища updated_at
    #     сохраняет updated_at в постоянное хранилище перед завершение работы
    #     :param target:
    #     :return:
    #     """
    #     updated_at = self.settings.state.get_state(f'{self.db_table}_updated_at')
    #     if updated_at:
    #         updated_at = datetime.fromisoformat(updated_at)
    #     else:
    #         updated_at = datetime(1, 1, 1, 0, 0, 0, )
    #
    #     while True:
    #         query = f"""
    #             SELECT
    #                 tbl.id,
    #                 tbl.updated_at
    #             FROM content.{self.db_table} as tbl
    #         """
    #
    #         data = []
    #         result = self.execute_query(query, (updated_at,))
    #         for row in result:
    #             data.append(
    #                 row['id']
    #             )
    #         if data:
    #             updated_at = result[-1]['updated_at']
    #             target.send(tuple(data))
    #             self.settings.state.set_state(f'{self.db_table}_updated_at', updated_at.isoformat())
    #         else:
    #             break

    @coroutine
    def postgres_enricher(self, target: Coroutine) -> Coroutine:
        """
        принимает id person или id genre , передает id film_work
        """

        if self.db_table == 'genre' or self.db_table == 'person':
            table_name = f'{self.db_table}_film_work'
            id_field = f'{self.db_table}_id'
        else:
            return

        while param_query := (yield):
            updated_at = datetime(1, 1, 1, 0, 0, 0)
            placeholder = ', '.join(['%s'] * len(param_query))
            while True:
                query = f"""
                        SELECT
                            fw.id,
                            fw.updated_at
                        FROM content.film_work fw
                        JOIN content.{table_name} tfw ON tfw.film_work_id = fw.id
                        WHERE fw.updated_at > %s AND tfw.{id_field} in ({placeholder})
                        ORDER BY fw.updated_at, fw.id
                        LIMIT {self.settings.LIMIT}
                    """

                data = []
                result = self.execute_query(query, (updated_at,) + param_query)
                for row in result:
                    data.append(row['id'])
                if data:
                    updated_at = result[-1]['updated_at']
                    target.send(tuple(data))
                else:
                    break

    @coroutine
    def postgres_merger(self, target: Coroutine) -> Coroutine:
        """
        принимает id фильмов
        отдает данные по фильмам
        """

        while param_query := (yield):
            placeholder = ', '.join(['%s'] * len(param_query))
            query = f"""
                    SELECT
                        fw.id as fw_id,
                        fw.title,
                        fw.description,
                        fw.rating,
                        fw.type,
                        fw.created_at,
                        fw.updated_at,
                        pfw.role,
                        p.id,
                        p.full_name,
                        g.name as genre
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON pfw.person_id = p.id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON gfw.genre_id = g.id
                    WHERE fw.id in ({placeholder});
                """

            result = self.execute_query(query, param_query)
            target.send(result)

    #ДОБАВЛЯЕТ ВСЕ ПОЛЯ
    # @coroutine
    # def postgres_merger(self, target: Coroutine) -> Coroutine:
    #     """
    #     принимает id фильмов
    #     отдает данные по фильмам
    #     """
    #
    #     while param_query := (yield):
    #         placeholder = ', '.join(['%s'] * len(param_query))
    #         query = f"""
    #                 SELECT
    #                     fw.id as fw_id,
    #                     fw.title,
    #                     fw.description,
    #                     fw.rating,
    #                     fw.type,
    #                     fw.created_at,
    #                     fw.updated_at,
    #                     pfw.role,
    #                     p.id,
    #                     p.full_name,
    #                     g.name as genre
    #                 FROM content.film_work fw
    #                 LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    #                 LEFT JOIN content.person p ON pfw.person_id = p.id
    #                 LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    #                 LEFT JOIN content.genre g ON gfw.genre_id = g.id
    #             """
    #
    #         result = self.execute_query(query, param_query)
    #         target.send(result)

    @coroutine
    def transform(self, target: Coroutine) -> Coroutine:
        """
        принимает данные по филльма, трансформирует их для формата elasticsearch
        """

        def add_value(value: Any, list_values: List) -> None:
            if value not in list_values:
                list_values.append(value)

        while data := (yield):
            film_data = {}
            for row in data:
                if row['fw_id'] in film_data:
                    data_movie = film_data[row['fw_id']]
                else:
                    data_movie = {
                        'id': row['fw_id'],
                        'title': row['title'],
                        'description': row['description'],
                        'imdb_rating': row['rating'],
                        'genre': [],
                        'writers': [],
                        'actors': [],
                        'actors_names': [],
                        'writers_names': [],
                        'director': [],
                    }
                    film_data[row['fw_id']] = data_movie

                if row['genre']:
                    add_value(row['genre'], data_movie['genre'])

                if row['role'] == 'director':
                    add_value(row['full_name'], data_movie['director'])
                elif row['role'] == 'actor':
                    actor = {'id': row['id'], 'name': row['full_name']}
                    add_value(actor, data_movie['actors'])
                    add_value(row['full_name'], data_movie['actors_names'])
                elif row['role'] == 'writer':
                    writer = {'id': row['id'], 'name': row['full_name']}
                    add_value(writer, data_movie['writers'])
                    add_value(row['full_name'], data_movie['writers_names'])

            target.send(film_data.values())

    @coroutine
    def loader(self) -> Coroutine:
        """
        Отправка запрса в ES и разбор ошибок сохранненых данных
        """

        while records := (yield):
            self.es_loader.load_to_es(records, self.settings.ES_INDEX_NAME)
