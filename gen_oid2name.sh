#!/bin/sh

PSQL_OPTS='-p 5432'

psql -A -t -F' ' $PSQL_OPTS postgres -c 'SELECT oid,spcname FROM pg_tablespace ORDER BY oid'
psql -A -t -F' ' $PSQL_OPTS postgres -c 'SELECT oid,datname FROM pg_database ORDER BY oid'
psql -A -t -F' ' $PSQL_OPTS postgres -c 'SELECT oid,relname FROM pg_class ORDER BY oid'

