#include "xlogdump_rmgr.h"

#include <time.h>

#include "postgres.h"
#include "access/clog.h"
#include "access/gist_private.h"
#include "access/htup.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/pg_control.h"

#include "xlogdump_oid2name.h"
#include "xlogdump_statement.h"

static char *str_time(time_t);
static bool dump_xlog_btree_insert_meta(XLogRecord *);
/* GIST stuffs */
static void decodePageUpdateRecord(PageUpdateRecord *, XLogRecord *);
static void decodePageSplitRecord(PageSplitRecord *, XLogRecord *);

static char *
str_time(time_t tnow)
{
	static char buf[32];

	strftime(buf, sizeof(buf),
			 "%Y-%m-%d %H:%M:%S %Z",
			 localtime(&tnow));

	return buf;
}

void
print_rmgr_xlog(XLogRecord *record, uint8 info, bool hideTimestamps)
{
	if (info == XLOG_CHECKPOINT_SHUTDOWN ||
	    info == XLOG_CHECKPOINT_ONLINE)
	{
		CheckPoint	*checkpoint = (CheckPoint*) XLogRecGetData(record);
		if(!hideTimestamps)
			printf("checkpoint: redo %u/%08X; tli %u; nextxid %u;\n"
			       "  nextoid %u; nextmulti %u; nextoffset %u; %s at %s\n",
			       checkpoint->redo.xlogid, checkpoint->redo.xrecoff,
			       checkpoint->ThisTimeLineID, checkpoint->nextXid, 
			       checkpoint->nextOid,
			       checkpoint->nextMulti,
			       checkpoint->nextMultiOffset,
			       (info == XLOG_CHECKPOINT_SHUTDOWN) ?
			       "shutdown" : "online",
			       str_time(checkpoint->time));
		else
			printf("checkpoint: redo %u/%08X; tli %u; nextxid %u;\n"
			       "  nextoid %u; nextmulti %u; nextoffset %u; %s\n",
			       checkpoint->redo.xlogid, checkpoint->redo.xrecoff,
			       checkpoint->ThisTimeLineID, checkpoint->nextXid, 
			       checkpoint->nextOid,
			       checkpoint->nextMulti,
			       checkpoint->nextMultiOffset,
			       (info == XLOG_CHECKPOINT_SHUTDOWN) ?
			       "shutdown" : "online");
		
	}
	else if (info == XLOG_NEXTOID)
	{
		Oid		nextOid;
		
		memcpy(&nextOid, XLogRecGetData(record), sizeof(Oid));
		printf("nextOid: %u\n", nextOid);
	}
	else if (info == XLOG_SWITCH)
	{
		printf("switch:\n");
	}
	else if (info == XLOG_NOOP)
	{
		printf("noop:\n");
	}
}

void
print_rmgr_xact(XLogRecord *record, uint8 info, bool hideTimestamps)
{
	if (info == XLOG_XACT_COMMIT)
	{
		xl_xact_commit	xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
		if(!hideTimestamps)
			printf("commit: %u at %s\n", record->xl_xid,
				   str_time(xlrec.xact_time));
		else
			printf("commit: %u\n", record->xl_xid);
	}
	else if (info == XLOG_XACT_ABORT)
	{
		xl_xact_abort	xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
		if(!hideTimestamps)
			printf("abort: %u at %s\n", record->xl_xid,
				   str_time(xlrec.xact_time));
		else
			printf("abort: %u\n", record->xl_xid);
	}
}

void
print_rmgr_smgr(XLogRecord *record, uint8 info)
{
	char spaceName[NAMEDATALEN];
	char dbName[NAMEDATALEN];
	char relName[NAMEDATALEN];

	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create	xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
		getSpaceName(xlrec.rnode.spcNode, spaceName, sizeof(spaceName));
		getDbName(xlrec.rnode.dbNode, dbName, sizeof(dbName));
		getRelName(xlrec.rnode.relNode, relName, sizeof(relName));
		printf("create rel: %s/%s/%s\n", 
			spaceName, dbName, relName);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate	xlrec;

		memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
		getSpaceName(xlrec.rnode.spcNode, spaceName, sizeof(spaceName));
		getDbName(xlrec.rnode.dbNode, dbName, sizeof(dbName));
		getRelName(xlrec.rnode.relNode, relName, sizeof(relName));
		printf("truncate rel: %s/%s/%s at block %u\n",
			 spaceName, dbName, relName, xlrec.blkno);
	}
}

void
print_rmgr_clog(XLogRecord *record, uint8 info)
{
	if (info == CLOG_ZEROPAGE)
	{
		int		pageno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));
		printf("zero clog page 0x%04x\n", pageno);
	}
}

void
print_rmgr_multixact(XLogRecord *record, uint8 info)
{
	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_MULTIXACT_ZERO_OFF_PAGE:
		{
			int		pageno;

			memcpy(&pageno, XLogRecGetData(record), sizeof(int));
			printf("zero offset page 0x%04x\n", pageno);
			break;
		}
		case XLOG_MULTIXACT_ZERO_MEM_PAGE:
		{
			int		pageno;

			memcpy(&pageno, XLogRecGetData(record), sizeof(int));
			printf("zero members page 0x%04x\n", pageno);
			break;
		}
		case XLOG_MULTIXACT_CREATE_ID:
		{
			xl_multixact_create xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			printf("multixact create: %u off %u nxids %u\n",
				   xlrec.mid,
				   xlrec.moff,
				   xlrec.nxids);
			break;
		}
	}
}

void
print_rmgr_heap2(XLogRecord *record, uint8 info)
{
	char spaceName[NAMEDATALEN];
	char dbName[NAMEDATALEN];
	char relName[NAMEDATALEN];

	switch (info)
	{
		case XLOG_HEAP2_FREEZE:
		{
			xl_heap_freeze xlrec;
			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			printf("freeze: ts %d db %d rel %d block %d cutoff_xid %d\n",
				xlrec.node.spcNode,
				xlrec.node.dbNode,
				xlrec.node.relNode,
				xlrec.block, xlrec.cutoff_xid
			);
		}
		break;

		case XLOG_HEAP2_CLEAN:
#if PG_VERSION_NUM < 90000
		case XLOG_HEAP2_CLEAN_MOVE:
#endif
		{
			xl_heap_clean xlrec;
			int total_off;
			int nunused = 0;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));

			total_off = (record->xl_len - SizeOfHeapClean) / sizeof(OffsetNumber);

			if (total_off > xlrec.nredirected + xlrec.ndead)
				nunused = total_off - (xlrec.nredirected + xlrec.ndead);

			printf("clean%s: ts %s db %s rel %s block %u redirected %d dead %d unused %d\n",
#if PG_VERSION_NUM < 90000
			       info == XLOG_HEAP2_CLEAN_MOVE ? "_move" : "",
#else
			       "",
#endif
			       spaceName, dbName, relName,
			       xlrec.block,
			       xlrec.nredirected, xlrec.ndead, nunused);
			break;
		}
		break;
	}
}

void
print_rmgr_heap(XLogRecord *record, uint8 info, bool statements)
{
	char spaceName[NAMEDATALEN];
	char dbName[NAMEDATALEN];
	char relName[NAMEDATALEN];

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
		{
			xl_heap_insert xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			if(statements)
				printInsert((xl_heap_insert *) XLogRecGetData(record), record->xl_len - SizeOfHeapInsert - SizeOfHeapHeader, relName);

			printf("insert%s: ts %s db %s rel %s block %u off %u\n",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   spaceName, dbName, relName,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));
			/* If backup block doesn't exist, dump rmgr data. */
			if (!(record->xl_info & XLR_BKP_BLOCK_MASK))
			{
				xl_heap_header *header = (xl_heap_header *)
					(XLogRecGetData(record) + SizeOfHeapInsert);
				printf("header: t_infomask2 %d t_infomask %d t_hoff %d\n",
					header->t_infomask2,
					header->t_infomask,
					header->t_hoff);
			}
			else
				printf("header: none\n");

			break;
		}
		case XLOG_HEAP_DELETE:
		{
			xl_heap_delete xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));
					
			if(statements)
				printf("DELETE FROM %s WHERE ...", relName);
					
			printf("delete%s: ts %s db %s rel %s block %u off %u\n",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   spaceName, dbName, relName,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));
			break;
		}
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_HOT_UPDATE:
		{
			xl_heap_update xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			if(statements)
				printUpdate((xl_heap_update *) XLogRecGetData(record), record->xl_len - SizeOfHeapUpdate - SizeOfHeapHeader, relName);

			printf("%supdate%s: ts %s db %s rel %s block %u off %u to block %u off %u\n",
				   (info & XLOG_HEAP_HOT_UPDATE) ? "hot_" : "",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   spaceName, dbName, relName,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid),
				   ItemPointerGetBlockNumber(&xlrec.newtid),
				   ItemPointerGetOffsetNumber(&xlrec.newtid));
			break;
		}
#if PG_VERSION_NUM < 90000
		case XLOG_HEAP_MOVE:
		{
			xl_heap_update xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));
			printf("move%s: ts %s db %s rel %s block %u off %u to block %u off %u\n",
				   (info & XLOG_HEAP_INIT_PAGE) ? "(init)" : "",
				   spaceName, dbName, relName,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid),
				   ItemPointerGetBlockNumber(&xlrec.newtid),
				   ItemPointerGetOffsetNumber(&xlrec.newtid));
			break;
		}
#endif
		case XLOG_HEAP_NEWPAGE:
		{
			xl_heap_newpage xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));
			printf("newpage: ts %s db %s rel %s block %u\n", 
					spaceName, dbName, relName,
				   xlrec.blkno);
			break;
		}
		case XLOG_HEAP_LOCK:
		{
			xl_heap_lock xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));
			printf("lock %s: ts %s db %s rel %s block %u off %u\n",
				   xlrec.shared_lock ? "shared" : "exclusive",
				   spaceName, dbName, relName,
				   ItemPointerGetBlockNumber(&xlrec.target.tid),
				   ItemPointerGetOffsetNumber(&xlrec.target.tid));
			break;
		}
#ifdef XLOG_HEAP_INPLACE
		case XLOG_HEAP_INPLACE:
		{
			xl_heap_inplace xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));
			printf("inplace: ts %s db %s rel %s block %u off %u\n", 
					spaceName, dbName, relName,
				   	ItemPointerGetBlockNumber(&xlrec.target.tid),
				   	ItemPointerGetOffsetNumber(&xlrec.target.tid));
			break;
		}
#endif
	}
}

static bool
dump_xlog_btree_insert_meta(XLogRecord *record)
{
	char spaceName[NAMEDATALEN];
	char dbName[NAMEDATALEN];
	char relName[NAMEDATALEN];

	xl_btree_insert *xlrec = (xl_btree_insert *) XLogRecGetData(record);
	char *datapos;
	int datalen;
	xl_btree_metadata md;
	BlockNumber downlink;

	/* copied from btree_xlog_insert(nbtxlog.c:191) */
	datapos = (char *) xlrec + SizeOfBtreeInsert;
	datalen = record->xl_len - SizeOfBtreeInsert;

	if ( getSpaceName(xlrec->target.node.spcNode, spaceName, sizeof(spaceName))==NULL ||
	     getDbName(xlrec->target.node.dbNode, dbName, sizeof(dbName))==NULL ||
	     getRelName(xlrec->target.node.relNode, relName, sizeof(relName))==NULL )
		return false;

	/* downlink */
	memcpy(&downlink, datapos, sizeof(BlockNumber));
	datapos += sizeof(BlockNumber);
	datalen -= sizeof(BlockNumber);

	/* xl_insert_meta */
	memcpy(&md, datapos, sizeof(xl_btree_metadata));
	datapos += sizeof(xl_btree_metadata);
	datalen -= sizeof(xl_btree_metadata);

	printf("insert_meta: index %s/%s/%s tid %u/%u downlink %u froot %u/%u\n", 
		spaceName, dbName, relName,
		BlockIdGetBlockNumber(&xlrec->target.tid.ip_blkid),
		xlrec->target.tid.ip_posid,
		downlink,
		md.fastroot, md.fastlevel
	);

	return true;
}

void
print_rmgr_btree(XLogRecord *record, uint8 info)
{
	char spaceName[NAMEDATALEN];
	char dbName[NAMEDATALEN];
	char relName[NAMEDATALEN];

	switch (info)
	{
		case XLOG_BTREE_INSERT_LEAF:
		{
			xl_btree_insert xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			printf("insert_leaf: index %s/%s/%s tid %u/%u\n",
					spaceName, dbName, relName,
				   	BlockIdGetBlockNumber(&xlrec.target.tid.ip_blkid),
				   	xlrec.target.tid.ip_posid);
			break;
		}
		case XLOG_BTREE_INSERT_UPPER:
		{
			xl_btree_insert xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			printf("insert_upper: index %s/%s/%s tid %u/%u\n",
					spaceName, dbName, relName,
				   	BlockIdGetBlockNumber(&xlrec.target.tid.ip_blkid),
				   	xlrec.target.tid.ip_posid);
			break;
		}
		case XLOG_BTREE_INSERT_META:
			dump_xlog_btree_insert_meta(record);
			/* FIXME: need to check the result code. */
			break;
		case XLOG_BTREE_SPLIT_L:
		case XLOG_BTREE_SPLIT_L_ROOT:
		{
			char *datapos = XLogRecGetData(record);
			xl_btree_split xlrec;
			OffsetNumber newitemoff;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			datapos += SizeOfBtreeSplit;
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));

			printf("split_l%s: index %s/%s/%s rightsib %u\n",
				info == XLOG_BTREE_SPLIT_L_ROOT ? "_root" : "",
				spaceName, dbName, relName, xlrec.rightsib);
			printf(" lsib %u rsib %u rnext %u level %u firstright %u\n",
				xlrec.leftsib, xlrec.rightsib,
				xlrec.rnext, xlrec.level, xlrec.firstright
			);
			/* downlinks */
			if (xlrec.level > 0)
			{
				BlockIdData downlink;
				memcpy(&downlink, XLogRecGetData(record) + SizeOfBtreeSplit, sizeof(downlink));
				datapos += sizeof(downlink);
				printf("downlink: %u\n",
					BlockIdGetBlockNumber(&downlink));
			}
			/* newitemoff */
			memcpy(&newitemoff, datapos, sizeof(OffsetNumber));
			datapos += sizeof(OffsetNumber);
			printf("newitemoff: %u\n", newitemoff);
			/* newitem (only when bkpblock1 is not recorded) */
			if (!(record->xl_info & XLR_BKP_BLOCK_1))
			{
				IndexTuple newitem = (IndexTuple)datapos;
				printf("newitem: { block %u pos 0x%x }\n",
					BlockIdGetBlockNumber(&newitem->t_tid.ip_blkid),
					newitem->t_tid.ip_posid);
			}
			/* items in right page should be here */
			break;
		}
		case XLOG_BTREE_SPLIT_R:
		case XLOG_BTREE_SPLIT_R_ROOT:
		{
			xl_btree_split xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));

			printf("split_r%s: index %s/%s/%s leftsib %u\n", 
					info == XLOG_BTREE_SPLIT_R_ROOT ? "_root" : "",
					spaceName, dbName, relName, xlrec.leftsib);
			break;
		}
		case XLOG_BTREE_DELETE:
		{
			xl_btree_delete xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));
			printf("delete: index %s/%s/%s block %u\n", 
					spaceName, dbName,	relName,
				   	xlrec.block);
			break;
		}
		case XLOG_BTREE_DELETE_PAGE:
		{
			xl_btree_delete_page xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			printf("delete_page: index %s/%s/%s tid %u/%u deadblk %u\n",
					spaceName, dbName, relName,
				   	BlockIdGetBlockNumber(&xlrec.target.tid.ip_blkid),
				   	xlrec.target.tid.ip_posid,
				   	xlrec.deadblk);
			break;
		}
		case XLOG_BTREE_DELETE_PAGE_META:
		{
			xl_btree_delete_page xlrec;
			xl_btree_metadata md;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			memcpy(&md, XLogRecGetData(record) + sizeof(xlrec),
				sizeof(xl_btree_metadata));

			printf("delete_page_meta: index %s/%s/%s tid %u/%u deadblk %u root %u/%u froot %u/%u\n", 
					spaceName, dbName, relName,
				   	BlockIdGetBlockNumber(&xlrec.target.tid.ip_blkid),
				   	xlrec.target.tid.ip_posid,
				   	xlrec.deadblk,
					md.root, md.level, md.fastroot, md.fastlevel);
			break;
		}
		case XLOG_BTREE_NEWROOT:
		{
			xl_btree_newroot xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.node.relNode, relName, sizeof(relName));

			printf("newroot: index %s/%s/%s rootblk %u level %u\n", 
					spaceName, dbName, relName,
				   	xlrec.rootblk, xlrec.level);
			break;
		}
		case XLOG_BTREE_DELETE_PAGE_HALF:
		{
			xl_btree_delete_page xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
			getSpaceName(xlrec.target.node.spcNode, spaceName, sizeof(spaceName));
			getDbName(xlrec.target.node.dbNode, dbName, sizeof(dbName));
			getRelName(xlrec.target.node.relNode, relName, sizeof(relName));

			printf("delete_page_half: index %s/%s/%s tid %u/%u deadblk %u\n",
					spaceName, dbName, relName,
				   	BlockIdGetBlockNumber(&xlrec.target.tid.ip_blkid),
				   	xlrec.target.tid.ip_posid,
				   	xlrec.deadblk);
			break;
		}
	}
}

/* copied from backend/access/gist/gistxlog.c */
static void
decodePageUpdateRecord(PageUpdateRecord *decoded, XLogRecord *record)
{
	char	   *begin = XLogRecGetData(record),
			   *ptr;
	int			i = 0,
				addpath = 0;

	decoded->data = (gistxlogPageUpdate *) begin;

	if (decoded->data->ntodelete)
	{
		decoded->todelete = (OffsetNumber *) (begin + sizeof(gistxlogPageUpdate) + addpath);
		addpath = MAXALIGN(sizeof(OffsetNumber) * decoded->data->ntodelete);
	}
	else
		decoded->todelete = NULL;

	decoded->len = 0;
	ptr = begin + sizeof(gistxlogPageUpdate) + addpath;
	while (ptr - begin < record->xl_len)
	{
		decoded->len++;
		ptr += IndexTupleSize((IndexTuple) ptr);
	}

	decoded->itup = (IndexTuple *) malloc(sizeof(IndexTuple) * decoded->len);

	ptr = begin + sizeof(gistxlogPageUpdate) + addpath;
	while (ptr - begin < record->xl_len)
	{
		decoded->itup[i] = (IndexTuple) ptr;
		ptr += IndexTupleSize(decoded->itup[i]);
		i++;
	}
}

/* copied from backend/access/gist/gistxlog.c */
static void
decodePageSplitRecord(PageSplitRecord *decoded, XLogRecord *record)
{
	char	   *begin = XLogRecGetData(record),
			   *ptr;
	int			j,
				i = 0;

	decoded->data = (gistxlogPageSplit *) begin;
	decoded->page = (NewPage *) malloc(sizeof(NewPage) * decoded->data->npage);

	ptr = begin + sizeof(gistxlogPageSplit);
	for (i = 0; i < decoded->data->npage; i++)
	{
		Assert(ptr - begin < record->xl_len);
		decoded->page[i].header = (gistxlogPage *) ptr;
		ptr += sizeof(gistxlogPage);

		decoded->page[i].itup = (IndexTuple *)
			malloc(sizeof(IndexTuple) * decoded->page[i].header->num);
		j = 0;
		while (j < decoded->page[i].header->num)
		{
			Assert(ptr - begin < record->xl_len);
			decoded->page[i].itup[j] = (IndexTuple) ptr;
			ptr += IndexTupleSize((IndexTuple) ptr);
			j++;
		}
	}
}

void
print_rmgr_gist(XLogRecord *record, uint8 info)
{
	switch (info)
	{
		case XLOG_GIST_PAGE_UPDATE:
		case XLOG_GIST_NEW_ROOT:
			{
				int i;
				PageUpdateRecord rec;
				decodePageUpdateRecord(&rec, record);

				printf("%s: rel=(%u/%u/%u) blk=%u key=(%d,%d) add=%d ntodelete=%d\n",
					info == XLOG_GIST_PAGE_UPDATE ? "page_update" : "new_root",
					rec.data->node.spcNode, rec.data->node.dbNode,
					rec.data->node.relNode,
					rec.data->blkno,
					ItemPointerGetBlockNumber(&rec.data->key),
					rec.data->key.ip_posid,
					rec.len,
					rec.data->ntodelete
				);
				for (i = 0; i < rec.len; i++)
				{
					printf("  itup[%d] points (%d, %d)\n",
						i,
						ItemPointerGetBlockNumber(&rec.itup[i]->t_tid),
						rec.itup[i]->t_tid.ip_posid
					);
				}
				for (i = 0; i < rec.data->ntodelete; i++)
				{
					printf("  todelete[%d] offset %d\n", i, rec.todelete[i]);
				}
				free(rec.itup);
			}
			break;
		case XLOG_GIST_PAGE_SPLIT:
			{
				int i;
				PageSplitRecord rec;

				decodePageSplitRecord(&rec, record);
				printf("page_split: orig %u key (%d,%d)\n",
					rec.data->origblkno,
					ItemPointerGetBlockNumber(&rec.data->key),
					rec.data->key.ip_posid
				);
				for (i = 0; i < rec.data->npage; i++)
				{
					printf("  page[%d] block %u tuples %d\n",
						i,
						rec.page[i].header->blkno,
						rec.page[i].header->num
					);
#if 0
					for (int j = 0; j < rec.page[i].header->num; j++)
					{
						NewPage *newpage = rec.page + i;
						printf("   itup[%d] points (%d,%d)\n",
							j,
							BlockIdGetBlockNumber(&newpage->itup[j]->t_tid.ip_blkid),
							newpage->itup[j]->t_tid.ip_posid
						);
					}
#endif
					free(rec.page[i].itup);
				}
			}
			break;
		case XLOG_GIST_INSERT_COMPLETE:
			{
				printf("insert_complete: \n");
			}
			break;
		case XLOG_GIST_CREATE_INDEX:
			printf("create_index: \n");
			break;
		case XLOG_GIST_PAGE_DELETE:
			printf("page_delete: \n");
			break;
	}
}

