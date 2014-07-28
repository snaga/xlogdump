VERSION_STR="0.1"

NAME = xlogtranslate
OBJS = strlcpy.o xlogtranslate.o
SO_MAJOR_VERSION = 0
SO_MINOR_VERSION = 1

PG_CPPFLAGS = -DVERSION_STR=\"$(VERSION_STR)\" -I. -I$(libpq_srcdir) -DDATADIR=\"$(datadir)\"
PG_LIBS = $(libpq_pgport)

subdir = contrib/xlogdump
top_builddir = postgres
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
include $(top_builddir)/src/Makefile.shlib

majorversion=`echo $(VERSION) | sed -e 's/^\([0-9]*\)\.\([0-9]*\).*/\1\2/g'`

test-xlogtranslate: $(shlib)
	gcc -o test-xlogtranslate test-xlogtranslate.c libxlogtranslate.a
