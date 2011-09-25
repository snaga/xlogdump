/*
 * xlogdump.h
 *
 * Common header file for the xlogdump utility.
 */
#ifndef __XLOGDUMP_H__
#define __XLOGDUMP_H__

#define VERSION_STR "0.5.0"

struct transInfo
{
	TransactionId		xid;
	uint32			tot_len;
	int			status;
	struct transInfo	*next;
};

typedef struct transInfo transInfo;
typedef struct transInfo *transInfoPtr;

/* Transactions status used only with -t option */
static const char * const status_names[3] = {
	"NOT COMMITED",					/* 0 */
	"COMMITED    ",					/* 1 */
	"ABORTED     "
};

#endif /* __XLOGDUMP_H__ */
