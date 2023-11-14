import time

from etl import ESLoader, ETL
from service import settings, connection_postgres

if __name__ == '__main__':
    es_loader = ESLoader(settings)
    pg_conn = connection_postgres(settings)

    etl_film_work = ETL(es_loader, pg_conn, 'film_work')
    etl_person = ETL(es_loader, pg_conn, 'person')
    etl_genre = ETL(es_loader, pg_conn, 'genre')

    print('CLASS WAS CREATED')

    etl_film_work_postgres_merger = etl_film_work.postgres_merger(
        etl_film_work.transform(
            etl_film_work.loader()
        )
    )

    etl_person_postgres_enricher = etl_person.postgres_enricher(
        etl_person.postgres_merger(
            etl_person.transform(
                etl_person.loader()
            )
        )
    )

    etl_genre_postgres_enricher = etl_genre.postgres_enricher(
        etl_genre.postgres_merger(
            etl_genre.transform(
                etl_genre.loader()
            )
        )
    )

    while True:
        etl_film_work.postgres_producer(etl_film_work_postgres_merger)
        etl_person.postgres_producer(etl_person_postgres_enricher)
        etl_genre.postgres_producer(etl_genre_postgres_enricher)
        time.sleep(settings.INTERVAL_QUERY)
