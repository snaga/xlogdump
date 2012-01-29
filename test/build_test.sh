#!/bin/sh

SOURCE_ROOT="/tmp/pgsql/source"
VERSIONS="9.1.0 9.0.4 8.4.8 8.3.15"

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

     make top_builddir=${SOURCE_ROOT}/postgresql-${v} clean;
     make top_builddir=${SOURCE_ROOT}/postgresql-${v};

     rm -rf xlogdump-${v};
     cp xlogdump xlogdump-${v};
done;
