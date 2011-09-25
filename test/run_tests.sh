#!/bin/sh

SOURCE_ROOT="/tmp/pgsql/source"
VERSIONS="9.1.0 9.0.4 8.4.8 8.3.15 8.2.21"

for v in $VERSIONS
  do 
     PGHOME=/tmp/pgsql/${v}
     PGDATA=${PGHOME}/data

     ./xlogdump-${v} -S test/xlog/${v}/* > test/results/xlogdump_S.${v}
     ./xlogdump-${v} test/xlog/${v}/* > test/results/xlogdump.${v}
done;
