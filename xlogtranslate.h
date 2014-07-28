/*
 * xlogdump.h
 *
 * Common header file for the xlogdump utility.
 */
#ifndef __XLOGDUMP_H__
#define __XLOGDUMP_H__

#include <stdint.h>

typedef struct result {
	char		entryType;
	uint32_t	xlogid;
	uint32_t	xrecoff;
	uint32_t	xid;
	int			space;
	int			db;
	int			relation;
	uint32_t	fromBlk;
	uint32_t	fromOff;
	uint32_t	toBlk;
	uint32_t	toOff;
	void		*next;
} Result;

typedef struct results {
	int			count;
	Result		*first;
	Result		*last;
} Results;

Result* parseWalFile(char*, uint32_t);
void freeWalResult(Result*);

#endif /* __XLOGDUMP_H__ */