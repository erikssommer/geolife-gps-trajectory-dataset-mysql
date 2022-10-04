FROM mysql:8.0.30

COPY .env .env

COPY docker/db_init.sh /docker-entrypoint-initdb.d/db_init.sh

EXPOSE 3306