VERSION_STR="0.6devel"

OS := $(shell uname -s)

PROGRAM = xlogdump
OBJS    = xlogdump.o xlogdump_rmgr.o xlogdump_statement.o xlogdump_oid2name.o

# MacOS provides strlcpy(), no need for a private version.  At some point this
# should be replaced with a more general test for the routine
ifneq (Darwin,$(OS))
OBJS += strlcpy.o
endif

PG_CPPFLAGS = -DVERSION_STR=\"$(VERSION_STR)\" -I. -I$(libpq_srcdir) -DDATADIR=\"$(datadir)\"
PG_LIBS = $(libpq_pgport) libxlogparse.a

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

majorversion=`echo $(VERSION) | sed -e 's/^\([0-9]*\)\.\([0-9]*\).*/\1\2/g'`

xlogdump_oid2name.o: oid2name.txt

oid2name.txt:
	cp oid2name-$(majorversion).txt oid2name.txt

xlogdump: all-lib

clean: clean-lib

all-lib clean-lib:
	$(MAKE) -f Makefile.libxlogparse $@
