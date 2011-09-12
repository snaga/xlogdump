#!/bin/sh

SOURCE_ROOT="/tmp/pgsql/source"
VERSIONS="9.1rc1 9.0.1 8.4.5 8.3.15 8.2.21"

for v in $VERSIONS
  do pushd ${SOURCE_ROOT}/postgresql-${v};
     if [ ! -f ${SOURCE_ROOT}/postgresql-${v}/src/backend/postgres ]; then
       ./configure --prefix=/tmp/pgsql/${v};
       make clean;
       make;
     fi;
     popd;

     make top_builddir=${SOURCE_ROOT}/postgresql-${v} clean;
     make top_builddir=${SOURCE_ROOT}/postgresql-${v};

     rm -rf xlogdump-${v};
     cp xlogdump xlogdump-${v};
done;
