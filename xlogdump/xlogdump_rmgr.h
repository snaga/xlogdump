#ifndef __XLOGDUMP_RMGR__
#define __XLOGDUMP_RMGR__

#include "postgres.h"
#include "access/gist_private.h"
#include "access/xlog.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

/* XXX these ought to be in smgr.h, but are not */
#define XLOG_SMGR_CREATE	0x10
#define XLOG_SMGR_TRUNCATE	0x20

typedef struct xl_smgr_create
{
	RelFileNode rnode;
} xl_smgr_create;

typedef struct xl_smgr_truncate
{
	BlockNumber blkno;
	RelFileNode rnode;
} xl_smgr_truncate;

typedef struct
{
    gistxlogPageUpdate *data;
    int         len;
    IndexTuple *itup;
    OffsetNumber *todelete;
} PageUpdateRecord;

/* copied from backend/access/gist/gistxlog.c */
typedef struct
{
    gistxlogPage *header;
    IndexTuple *itup;
} NewPage;

/* copied from backend/access/gist/gistxlog.c */
typedef struct
{
    gistxlogPageSplit *data;
    NewPage    *page;
} PageSplitRecord;

void print_rmgr_xlog(XLogRecord *, uint8, bool);
void print_rmgr_xact(XLogRecord *, uint8, bool);
void print_rmgr_smgr(XLogRecord *, uint8);
void print_rmgr_clog(XLogRecord *, uint8);
void print_rmgr_multixact(XLogRecord *, uint8);
void print_rmgr_heap2(XLogRecord *, uint8);
void print_rmgr_heap(XLogRecord *, uint8, bool);
void print_rmgr_btree(XLogRecord *, uint8);
void print_rmgr_gist(XLogRecord *, uint8);

#endif /* __XLOGDUMP_RMGR__ */
