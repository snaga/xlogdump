#ifndef __XLOGDUMP_OID2NAME__
#define __XLOGDUMP_OID2NAME__

#include "c.h"
#include "libpq-fe.h"

bool DBConnect(const char *, const char *, char *, const char *);

void getSpaceName(uint32, char *, size_t);
void getDbName(uint32, char *, size_t);
void getRelName(uint32, char *, size_t);

int relid2attr_begin();
int relid2attr_fetch(int, char *, Oid *);
void relid2attr_end(void);

bool do_oid2name(void);

void DBDisconnect(void);

#endif /* __XLOGDUMP_OID2NAME__ */
