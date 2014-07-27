VERSION_STR="0.1a"

# PROGRAM = xlogtranslate
# MODULE_big = libxlogtranslate
OBJS       = strlcpy.o xlogtranslate.o

PG_CPPFLAGS = -DVERSION_STR=\"$(VERSION_STR)\" -I. -I$(libpq_srcdir) -DDATADIR=\"$(datadir)\"
PG_LIBS = $(libpq_pgport)

subdir = contrib/xlogdump
top_builddir = postgres
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

majorversion=`echo $(VERSION) | sed -e 's/^\([0-9]*\)\.\([0-9]*\).*/\1\2/g'`


thelib: $(OBJS)
	gcc -dynamiclib -undefined suppress -flat_namespace $(OBJS) -o libxlogtranslate.so

