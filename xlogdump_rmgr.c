/*
 * xlogdump_rmgr.c
 *
 * a collection of functions which print xlog records generated
 * by each resource manager.
 */
#include "xlogdump_rmgr.h"
#include "xlogdump.h"

#include <time.h>

#include "postgres.h"
#include "access/clog.h"
#include "access/gist_private.h"
#include "access/htup.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/pg_control.h"
#include "commands/dbcommands.h"

#include "utils/relmapper.h"

#include "xlogdump_statement.h"

/*
 * See access/tramsam/rmgr.c for more details.
 */
const char * const RM_names[RM_MAX_ID+1] = {
	"XLOG",						/* 0 */
	"Transaction",					/* 1 */
	"Storage",					/* 2 */
	"CLOG",						/* 3 */
	"Database",					/* 4 */
	"Tablespace",					/* 5 */
	"MultiXact",					/* 6 */
	"RelMap",					/* 7 */
	"Standby",					/* 8 */
	"Heap2",					/* 9 */
	"Heap",						/* 10 */
	"Btree",					/* 11 */
	"Hash",						/* 12 */
	"Gin",						/* 13 */
	"Gist",						/* 14 */
	"Sequence",					/* 15 */
};

/* copy from utils/timestamp.h */
#define SECS_PER_DAY	86400
#ifdef HAVE_INT64_TIMESTAMP
#define USECS_PER_DAY	INT64CONST(86400000000)
#endif

/* copy from utils/datetime.h */
#define UNIX_EPOCH_JDATE		2440588 /* == date2j(1970, 1, 1) */
#define POSTGRES_EPOCH_JDATE	2451545 /* == date2j(2000, 1, 1) */

static bool dump_enabled = true;

struct xlogdump_rmgr_stats_t {
	int xlog_checkpoint;
	int xlog_switch;
	int xlog_backup_end;
	int xact_commit;
	int xact_abort;
	int heap_insert;
	int heap_delete;
	int heap_update;
	int heap_hot_update;
	int heap_move;
	int heap_newpage;
	int heap_lock;
	int heap_inplace;
	int heap_init_page;
};

static struct xlogdump_rmgr_stats_t rmgr_stats;

void
print_xlog_rmgr_stats(int rmid)
{
	switch (rmid)
	{
	case RM_XLOG_ID:
		printf("                 checkpoint: %d, switch: %d, backup end: %d\n",
		       rmgr_stats.xlog_checkpoint,
		       rmgr_stats.xlog_switch,
		       rmgr_stats.xlog_backup_end);
		break;

	case RM_XACT_ID:
		printf("                 commit: %d, abort: %d\n",
		       rmgr_stats.xact_commit,
		       rmgr_stats.xact_abort);
		break;

	case RM_HEAP_ID:
		printf("                 ins: %d, upd/hot_upd: %d/%d, del: %d\n",
		       rmgr_stats.heap_insert,
		       rmgr_stats.heap_update,
		       rmgr_stats.heap_hot_update,
		       rmgr_stats.heap_delete);
		break;
	}
}

void
enable_rmgr_dump(bool flag)
{
	dump_enabled = flag;
}

/*
 * a common part called by each `print_rmgr_*()' to print a xlog record header
 * with the detail.
 */
static void
print_rmgr_record(XLogRecPtr cur, XLogRecord *rec, const char *detail)
{
	if (!dump_enabled)
		return;

	PRINT_XLOGRECORD_HEADER(cur, rec);
	printf("%s\n", detail);
}

void
print_rmgr_heap2(XLogRecPtr cur, XLogRecord *record, uint8 info)
{
	char buf[1024];

	switch (info)
	{
		case XLOG_HEAP2_FREEZE:
		{
			xl_heap_freeze xlrec;
			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			snprintf(buf, sizeof(buf), "freeze: ts %d db %d rel %d block %d cutoff_xid %d",
				xlrec.node.spcNode,
				xlrec.node.dbNode,
				xlrec.node.relNode,
				xlrec.block, xlrec.cutoff_xid
			);
		}
		break;

		case XLOG_HEAP2_CLEAN:
		{
			xl_heap_clean xlrec;
			int total_off;
			int nunused = 0;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			total_off = (record->xl_len - SizeOfHeapClean) / sizeof(OffsetNumber);

			if (total_off > xlrec.nredirected + xlrec.ndead)
				nunused = total_off - (xlrec.nredirected + xlrec.ndead);

			snprintf(buf, sizeof(buf), "clean%s: s/d/r:%d/%d/%d block:%u redirected/dead/unused:%d/%d/%d removed xid:%d",
			       "",
			       xlrec.node.spcNode, xlrec.node.dbNode, xlrec.node.relNode,
			       xlrec.block,
			       xlrec.nredirected, xlrec.ndead, nunused
			       , xlrec.latestRemovedXid
			       );
			break;
		}
		break;

		case XLOG_HEAP2_CLEANUP_INFO:
		{
			xl_heap_cleanup_info xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			snprintf(buf, sizeof(buf), "cleanup_info: s/d/r:%d/%d/%d removed xid:%d",
				 xlrec.node.spcNode, xlrec.node.dbNode, xlrec.node.relNode,
				 xlrec.latestRemovedXid);
		}
		break;

		default:
			snprintf(buf, sizeof(buf), "unknown HEAP2 operation - %d.", info);
			break;
	}

	print_rmgr_record(cur, record, buf);
}

void
print_rmgr_heap(XLogRecPtr cur, XLogRecord *record, uint8 info)
{
	char buf[1024];

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
		{
			xl_heap_insert xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			snprintf(buf, sizeof(buf), "insert%s: s/d/r:%d/%d/%d blk/off:%u/%u",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));
			/* If backup block doesn't exist, dump rmgr data. */
			if (!(record->xl_info & XLR_BKP_BLOCK_MASK))
			{
				char buf2[1024];

				xl_heap_header *header = (xl_heap_header *)
					(XLogRecGetData(record) + SizeOfHeapInsert);
				snprintf(buf2, sizeof(buf2), " header: t_infomask2 %d t_infomask %d t_hoff %d",
					header->t_infomask2,
					header->t_infomask,
					header->t_hoff);

				strlcat(buf, buf2, sizeof(buf));
			}
			else
				strlcat(buf, " header: none", sizeof(buf));

			rmgr_stats.heap_insert++;
			break;
		}
		case XLOG_HEAP_DELETE:
		{
			xl_heap_delete xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
					
			snprintf(buf, sizeof(buf), "delete%s: s/d/r:%d/%d/%d block %u off %u",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));

			rmgr_stats.heap_delete++;
			break;
		}
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_HOT_UPDATE:
		{
			xl_heap_update xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			snprintf(buf, sizeof(buf), "%supdate%s: s/d/r:%d/%d/%d block %u off %u to block %u off %u",
				   (info & XLOG_HEAP_HOT_UPDATE) ? "hot_" : "",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid),
				   ItemPointerGetBlockNumber(&xlrec.newtid),
				   ItemPointerGetOffsetNumber(&xlrec.newtid));

			if ((info & XLOG_HEAP_OPMASK) == XLOG_HEAP_UPDATE)
				rmgr_stats.heap_update++;
			else
				rmgr_stats.heap_hot_update++;

			break;
		}
#if PG_VERSION_NUM < 90000
		case XLOG_HEAP_MOVE:
		{
			xl_heap_update xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			snprintf(buf, sizeof(buf), "move%s: s/d/r:%d/%d/%d block %u off %u to block %u off %u",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid),
				   ItemPointerGetBlockNumber(&xlrec.newtid),
				   ItemPointerGetOffsetNumber(&xlrec.newtid));

			rmgr_stats.heap_move++;
			break;
		}
#endif
		case XLOG_HEAP_NEWPAGE:
		{
			xl_heap_newpage xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			snprintf(buf, sizeof(buf), "newpage: s/d/r:%d/%d/%d block %u", 
					xlrec.node.spcNode, xlrec.node.dbNode, xlrec.node.relNode,
				   xlrec.blkno);

			rmgr_stats.heap_newpage++;
			break;
		}
		case XLOG_HEAP_LOCK:
		{
			xl_heap_lock xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			snprintf(buf, sizeof(buf), "lock %s: s/d/r:%d/%d/%d block %u off %u",
				   xlrec.shared_lock ? "shared" : "exclusive",
				   xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));

			rmgr_stats.heap_lock++;
			break;
		}

		case XLOG_HEAP_INPLACE:
		{
			xl_heap_inplace xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			snprintf(buf, sizeof(buf), "inplace: s/d/r:%d/%d/%d block %u off %u", 
					xlrec.target.node.spcNode, xlrec.target.node.dbNode, xlrec.target.node.relNode,
				   	ItemPointerGetBlockNumber(&xlrec.target.tid),
				   	ItemPointerGetOffsetNumber(&xlrec.target.tid));

			rmgr_stats.heap_inplace++;
			break;
		}

		case XLOG_HEAP_INIT_PAGE:
		{
			snprintf(buf, sizeof(buf), "init page");
			rmgr_stats.heap_init_page++;
			break;
		}

		default:
			snprintf(buf, sizeof(buf), "unknown HEAP operation - %d.", (info & XLOG_HEAP_OPMASK));
			break;
	}

	print_rmgr_record(cur, record, buf);
}

void
print_rmgr_seq(XLogRecPtr cur, XLogRecord *record, uint8 info)
{
	/* FIXME: need to be implemented. */
	print_rmgr_record(cur, record, "seq");
}

