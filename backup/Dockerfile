# Postgres with pgBackRest (https://pgbackrest.org). One image for both jobs: the database,
# which archives its WAL as it writes it, and the backup scheduler that runs next to it.
# Keep the tag in step with the postgres image in docker-compose.yml: same major version, since
# they share the data directory.
FROM postgres:18.6-alpine

RUN apk add --no-cache pgbackrest jq \
    && install -d -o postgres -g postgres -m 750 /var/lib/pgbackrest /var/log/pgbackrest /tmp/pgbackrest

COPY pgbackrest.conf /etc/pgbackrest/pgbackrest.conf
COPY scheduler.sh /usr/local/bin/backup-scheduler
