#!/bin/sh

SOURCE_ROOT="/tmp/pgsql/source"
VERSIONS="9.2beta2 9.1.4 9.0.8 8.4.12 8.3.19"

for v in $VERSIONS
  do pushd ${SOURCE_ROOT}/postgresql-${v};
     if [ ! -f ${SOURCE_ROOT}/postgresql-${v}/src/backend/postgres ]; then
       ./configure --prefix=/tmp/pgsql/${v};
       make clean;
       make;
     fi;

     if [ ! -d /tmp/pgsql/${v} ]; then
       make install
     fi;

     popd;

     env USE_PGXS=1 PATH=/tmp/pgsql/${v}/bin:$PATH make clean;
     env USE_PGXS=1 PATH=/tmp/pgsql/${v}/bin:$PATH make;

     if [ -f xlogdump ]; then
	 success="$success $v"
     else
	 failed="$failed $v"
     fi;

     rm -rf xlogdump-${v};
     mv xlogdump xlogdump-${v};
done;

echo "-------------------------------------------"
echo "Success:$success";
echo "Failed:$failed";
