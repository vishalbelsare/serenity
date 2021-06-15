#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
SQL_DIR="${SCRIPT_DIR}/../sql"

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
  PGPASSWORD=$TIMESCALEDB_PASSWORD psql -h "$TIMESCALEDB_HOST" -p "$TIMESCALEDB_PORT" -U postgres -d "$db" -f "$sql_script"
}

pg_install_sql postgres $SQL_DIR/serenitydb_install.sql
pg_install_sql serenity $SQL_DIR/serenitydb_schema.sql
pg_install_sql serenity $SQL_DIR/serenitydb_grants.sql
pg_install_sql postgres $SQL_DIR/sharadar_install.sql
pg_install_sql sharadar $SQL_DIR/sharadar_schema.sql
