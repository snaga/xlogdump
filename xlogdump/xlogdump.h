/*-------------------------------------------------------------------------
 *
 * xlogdump.h
 *		Common header file for the xlogdump utility.
 *
 *
 *
 * Portions Copyright (c) 1996-2004, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

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

