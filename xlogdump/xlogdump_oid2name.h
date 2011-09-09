/*
 * xlogdump_oid2name.h
 *
 * a collection of functions to get database object names from the oids
 * by looking up the system catalog.
 */
#ifndef __XLOGDUMP_OID2NAME_H__
#define __XLOGDUMP_OID2NAME_H__

#include "c.h"
#include "libpq-fe.h"

bool DBConnect(const char *, const char *, char *, const char *);

char *getSpaceName(uint32, char *, size_t);
char *getDbName(uint32, char *, size_t);
char *getRelName(uint32, char *, size_t);

int relname2attr_begin(const char *);
int relname2attr_fetch(int, char *, Oid *);
void relname2attr_end(void);

bool oid2name_enabled(void);

void DBDisconnect(void);

#endif /* __XLOGDUMP_OID2NAME_H__ */
