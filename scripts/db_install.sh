#!/usr/bin/env bash

TIMESCALEDB_HOST=localhost
TIMESCALEDB_PORT=30432
TIMESCALEDB_PASSWORD=postgres

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -h|--db-host)
    TIMESCALEDB_HOST="$2"
    shift
    shift
    ;;
    -p|--db-port)
    TIMESCALEDB_PORT="$2"
    shift
    shift
    ;;
    -P|db-password)
    TIMESCALEDB_PASSWORD="$2"
    shift
    shift
    ;;
    *)
    POSITIONAL+=("$1") # save it in an array for later
    shift
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

pg_install_sql(){
  db=$1
  sql_script=$2
  psql -h "$TIMESCALEDB_HOST" -p "$TIMESCALEDB_PORT" -U postgres -P "$TIMESCALEDB_PASSWORD" -d "$db" -f "$sql_script"
}

pg_install_sql postgres ../sql/serenitydb_install.sql
pg_install_sql serenity ../sql/serenitydb_schema.sql
pg_install_sql serenity ../sql/serenitydb_grants.sql
pg_install_sql postgres ../sql/sharadar_install.sql
pg_install_sql sharadar  ../sql/sharadar_schema.sql