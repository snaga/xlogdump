# $PostgreSQL: pgsql/contrib/pgbench/Makefile,v 1.14 2005/09/27 17:13:08 tgl Exp $

PROGRAM = xlogdump
OBJS    = xlogdump.o ../../src/port/sprompt.o ../../src/backend/utils/hash/pg_crc.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

DOCS = README.xlogdump

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/xlogdump
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
