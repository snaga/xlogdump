PROGRAM = xlogdump
OBJS    = strlcpy.o xlogdump.o xlogdump_rmgr.o xlogdump_statement.o xlogdump_oid2name.o $(top_builddir)/src/backend/utils/hash/pg_crc.o

PG_CPPFLAGS = -I$(libpq_srcdir) -DDATADIR=\"$(datadir)\"
PG_LIBS = $(libpq_pgport)

DATA = oid2name.txt
EXTRA_CLEAN = oid2name.txt

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

majorversion=`echo $(VERSION) | sed -e 's/\.[^\.]*$$//g'`

xlogdump_oid2name.o: oid2name.txt

oid2name.txt:
	cp oid2name-$(majorversion).*.txt oid2name.txt
