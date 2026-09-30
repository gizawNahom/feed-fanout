#!/bin/sh
# The backup scheduler. Postgres archives WAL continuously by itself (every change, within
# archive_timeout); this takes the periodic full backups a restore starts from.
#
# Every hour: if the newest full backup is older than BACKUP_INTERVAL_HOURS, take one.
# The container is healthy once a first backup exists.
set -eu

interval=$(( ${BACKUP_INTERVAL_HOURS:-24} * 3600 ))
ready=/tmp/pgbackrest/ready

trap 'exit 0' TERM INT

pgbackrest --stanza=main stanza-create   # sets up the repository; does nothing if it exists
pgbackrest --stanza=main check           # WAL archiving works, end to end

while true; do
    newest=$(pgbackrest --stanza=main --output=json info \
        | jq '[.[0].backup[] | select(.type == "full") | .timestamp.stop] | max // 0')
    if [ $(( $(date +%s) - newest )) -ge "$interval" ]; then
        pgbackrest --stanza=main --type=full backup
    fi
    touch "$ready"
    sleep 3600 &
    wait $!
done
