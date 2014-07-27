#include "postgres.h"

#include <fcntl.h>
#include <getopt_long.h>
#include <time.h>
#include <unistd.h>

#include "access/clog.h"
#include "access/gist_private.h"
#include "access/htup.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/transam.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "commands/dbcommands.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "utils/pg_crc.h"
#include "utils/relmapper.h"

#include "pg_crc32_table.h"

#include "libpq-fe.h"
#include "pg_config.h"
#include "pqexpbuffer.h"

#include "strlcat.h"
#include "xlogtranslate.h"

PG_MODULE_MAGIC;

/* prototypes */
static bool readXLogPage(void);
static bool RecordIsValid(XLogRecord *, XLogRecPtr);
static bool ReadRecord(void);
static void dumpXLogRecord(XLogRecord *, bool);
void print_rmgr_heap(XLogRecPtr, XLogRecord *, uint8);

struct transInfo
{
	TransactionId		xid;
	uint32			tot_len;
	int			status;
	struct transInfo	*next;
};

typedef struct transInfo transInfo;
typedef struct transInfo *transInfoPtr;

static int		logFd;	       /* kernel FD for current input file */
static TimeLineID	logTLI;	       /* current log file timeline */
static uint32		logId;	       /* current log file id */
static uint32		logSeg;	       /* current log file segment */
static int32		logPageOff;    /* offset of current page in file */
static int		logRecOff;     /* offset of next record in page */
static char		pageBuffer[XLOG_BLCKSZ];	/* current page */
static XLogRecPtr	curRecPtr;     /* logical address of current record */
static XLogRecPtr	prevRecPtr;    /* logical address of previous record */
static char		*readRecordBuf = NULL; /* ReadRecord result area */
static uint32		readRecordBufSize = 0;
static uint32 lastOffset = 0;

/* struct to aggregate transactions */
transInfoPtr		transactionsInfo = NULL;

void
print_rmgr_heap(XLogRecPtr cur, XLogRecord *record, uint8 info)
{
	char type = 'x'; // I/U/D/p
	int space = 0, db = 0, relation = 0;
	uint32 fromBlk = 0, fromOff = 0, toBlk = 0, toOff = 0;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
		{
			xl_heap_insert xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			type = 'I';
			space = xlrec.target.node.spcNode;
			db = xlrec.target.node.dbNode;
			relation = xlrec.target.node.relNode;
			toBlk = ItemPointerGetBlockNumber(&xlrec.target.tid);
			toOff = ItemPointerGetOffsetNumber(&xlrec.target.tid);
			break;
		}
		case XLOG_HEAP_DELETE:
		{
			xl_heap_delete xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			type = 'D';
			space = xlrec.target.node.spcNode;
			db = xlrec.target.node.dbNode;
			relation = xlrec.target.node.relNode;
			toBlk = ItemPointerGetBlockNumber(&xlrec.target.tid);
			toOff = ItemPointerGetOffsetNumber(&xlrec.target.tid);
			break;
		}
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_HOT_UPDATE:
		{
			xl_heap_update xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			type = 'U';
			space = xlrec.target.node.spcNode;
			db = xlrec.target.node.dbNode;
			relation = xlrec.target.node.relNode;
			fromBlk = ItemPointerGetBlockNumber(&xlrec.target.tid);
			fromOff = ItemPointerGetOffsetNumber(&xlrec.target.tid);
			toBlk = ItemPointerGetBlockNumber(&xlrec.newtid);
			toOff = ItemPointerGetOffsetNumber(&xlrec.newtid);
			break;
		}

		case XLOG_HEAP_INPLACE:
		{
			xl_heap_inplace xlrec;

			memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));

			type = 'P';
			space = xlrec.target.node.spcNode;
			db = xlrec.target.node.dbNode;
			relation = xlrec.target.node.relNode;
			fromBlk = ItemPointerGetBlockNumber(&xlrec.target.tid);
			fromOff = ItemPointerGetOffsetNumber(&xlrec.target.tid);
			toBlk = ItemPointerGetBlockNumber(&xlrec.target.tid);
			toOff = ItemPointerGetOffsetNumber(&xlrec.target.tid);
			break;
		}

		default:
			return;
	}

	if (cur.xrecoff > lastOffset) {
		printf("%c,%u,%u,%u,%d,%d,%d,%u,%u,%u,%u\n",
		 type, cur.xlogid, cur.xrecoff, record->xl_xid, space, db, relation, fromBlk, fromOff, toBlk, toOff);
	}
}

/*
 * Attempt to read an XLOG record into readRecordBuf.
 */
static bool
ReadRecord(void)
{
	char	   *buffer;
	XLogRecord *record;
	XLogContRecord *contrecord;
	uint32		len,
				total_len;
	int			retries = 0;

restart:
	while (logRecOff <= 0 || logRecOff > XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		/* Need to advance to new page */
		if (! readXLogPage())
			return false;
		logRecOff = XLogPageHeaderSize((XLogPageHeader) pageBuffer);
		if ((((XLogPageHeader) pageBuffer)->xlp_info & ~XLP_LONG_HEADER) != 0)
		{
			// printf("Unexpected page info flags %04X at offset %X\n",
			// 	   ((XLogPageHeader) pageBuffer)->xlp_info, logPageOff);
			/* Check for a continuation record */
			if (((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD)
			{
				// printf("Skipping unexpected continuation record at offset %X\n",
				// 	   logPageOff);
				contrecord = (XLogContRecord *) (pageBuffer + logRecOff);
				logRecOff += MAXALIGN(contrecord->xl_rem_len + SizeOfXLogContRecord);
			}
		}
	}

	curRecPtr.xlogid = logId;
	curRecPtr.xrecoff = logSeg * XLogSegSize + logPageOff + logRecOff;
	record = (XLogRecord *) (pageBuffer + logRecOff);

	if (record->xl_len == 0)
	{
		/* Stop if XLOG_SWITCH was found. */
		if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
		{
			dumpXLogRecord(record, false);
			return false;
		}

		// printf("ReadRecord: record with zero len at %u/%08X\n",
		//    curRecPtr.xlogid, curRecPtr.xrecoff);

		/* Attempt to recover on new page, but give up after a few... */
		logRecOff = 0;
		if (++retries > 4)
			return false;
		goto restart;
	}
	if (record->xl_tot_len < SizeOfXLogRecord + record->xl_len ||
		record->xl_tot_len > SizeOfXLogRecord + record->xl_len +
		XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ))
	{
		// printf(
		// 	"invalid record length(expected %lu ~ %lu, actual %d) at %X/%X\n",
		// 	(unsigned long) (SizeOfXLogRecord + record->xl_len),
		// 	(unsigned long) (SizeOfXLogRecord + record->xl_len +
		// 					 XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ)),
		// 	record->xl_tot_len,
		// 	curRecPtr.xlogid, curRecPtr.xrecoff);
		// printf("HINT: Make sure you're using the correct xlogdump binary built against\n"
		//        "      the same architecture and version of PostgreSQL where the WAL file\n"
		//        "      comes from.\n");
		return false;
	}
	total_len = record->xl_tot_len;

	/*
	 * Allocate or enlarge readRecordBuf as needed.  To avoid useless
	 * small increases, round its size to a multiple of XLOG_BLCKSZ, and make
	 * sure it's at least 4*BLCKSZ to start with.  (That is enough for all
	 * "normal" records, but very large commit or abort records might need
	 * more space.)
	 */
	if (total_len > readRecordBufSize)
	{
		uint32		newSize = total_len;

		newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
		newSize = Max(newSize, 4 * XLOG_BLCKSZ);
		if (readRecordBuf)
			free(readRecordBuf);
		readRecordBuf = (char *) malloc(newSize);
		if (!readRecordBuf)
		{
			readRecordBufSize = 0;
			/* We treat this as a "bogus data" condition */
			// fprintf(stderr, "record length %u at %X/%X too long\n",
			// 		total_len, curRecPtr.xlogid, curRecPtr.xrecoff);
			return false;
		}
		readRecordBufSize = newSize;
	}

	buffer = readRecordBuf;
	len = XLOG_BLCKSZ - curRecPtr.xrecoff % XLOG_BLCKSZ; /* available in block */
	if (total_len > len)
	{
		/* Need to reassemble record */
		uint32			gotlen = len;

		memcpy(buffer, record, len);
		record = (XLogRecord *) buffer;
		buffer += len;
		for (;;)
		{
			uint32	pageHeaderSize;

			if (! readXLogPage())
			{
				/* XXX ought to be able to advance to new input file! */
				// fprintf(stderr, "Unable to read continuation page?\n");
				dumpXLogRecord(record, true);
				return false;
			}
			if (!(((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				// printf("ReadRecord: there is no ContRecord flag in logfile %u seg %u off %u\n",
				// 	   logId, logSeg, logPageOff);
				return false;
			}
			pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) pageBuffer);
			contrecord = (XLogContRecord *) (pageBuffer + pageHeaderSize);
			if (contrecord->xl_rem_len == 0 || 
				total_len != (contrecord->xl_rem_len + gotlen))
			{
				// printf("ReadRecord: invalid cont-record len %u in logfile %u seg %u off %u\n",
				// 	   contrecord->xl_rem_len, logId, logSeg, logPageOff);
				return false;
			}
			len = XLOG_BLCKSZ - pageHeaderSize - SizeOfXLogContRecord;
			if (contrecord->xl_rem_len > len)
			{
				memcpy(buffer, (char *)contrecord + SizeOfXLogContRecord, len);
				gotlen += len;
				buffer += len;
				continue;
			}
			memcpy(buffer, (char *) contrecord + SizeOfXLogContRecord,
				   contrecord->xl_rem_len);
			logRecOff = MAXALIGN(pageHeaderSize + SizeOfXLogContRecord + contrecord->xl_rem_len);
			break;
		}
		if (!RecordIsValid(record, curRecPtr))
			return false;
		return true;
	}
	/* Record is contained in this page */
	memcpy(buffer, record, total_len);
	record = (XLogRecord *) buffer;
	logRecOff += MAXALIGN(total_len);
	if (!RecordIsValid(record, curRecPtr))
		return false;
	return true;
}

static void
dumpXLogRecord(XLogRecord *record, bool header_only)
{
	uint8	info = record->xl_info & ~XLR_INFO_MASK;

	if (header_only)
	{
		// printf(" ** maybe continues to next segment **\n");
		// return;
	}

	if (record->xl_rmid == RM_HEAP_ID) {
		print_rmgr_heap(curRecPtr, record, info);
	}
}

static bool readXLogPage(void) {
	if (read(logFd, pageBuffer, XLOG_BLCKSZ) == XLOG_BLCKSZ) {
		logPageOff += XLOG_BLCKSZ;
		return true;
	}
	return false;
}

/*
 * Routines needed if headers were configured for ASSERT
 */
#ifndef assert_enabled
bool		assert_enabled = true;

int
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	// fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
	// 		errorType, conditionName,
	// 		fileName, lineNumber);

	abort();
	return 0;
}

#endif /* assert_enabled */

/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record has been read into memory at *record.
 */
static bool
RecordIsValid(XLogRecord *record, XLogRecPtr recptr)
{
	pg_crc32	crc;
	int			i;
	uint32		len = record->xl_len;
	BkpBlock	bkpb;
	char	   *blk;

	/* First the rmgr data */
	INIT_CRC32(crc);
	COMP_CRC32(crc, XLogRecGetData(record), len);

	/* Add in the backup blocks, if any */
	blk = (char *) XLogRecGetData(record) + len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		uint32	blen;

		if (!(record->xl_info & XLR_SET_BKP_BLOCK(i)))
			continue;

		memcpy(&bkpb, blk, sizeof(BkpBlock));
		if (bkpb.hole_offset + bkpb.hole_length > BLCKSZ)
		{
			// printf("incorrect hole size in record at %X/%X\n",
			// 	   recptr.xlogid, recptr.xrecoff);
			return false;
		}
		blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;
		COMP_CRC32(crc, blk, blen);
		blk += blen;
	}

	/* skip total xl_tot_len check if physical log has been removed. */
	if (!(record->xl_info & XLR_BKP_REMOVABLE) ||
		record->xl_info & XLR_BKP_BLOCK_MASK)
	{
		/* Check that xl_tot_len agrees with our calculation */
		if (blk != (char *) record + record->xl_tot_len)
		{
			// printf("incorrect total length in record at %X/%X\n",
			// 	   recptr.xlogid, recptr.xrecoff);
			return false;
		}
	}

	/* Finally include the record header */
	COMP_CRC32(crc, (char *) record + sizeof(pg_crc32),
			   SizeOfXLogRecord - sizeof(pg_crc32));
	FIN_CRC32(crc);

	if (!EQ_CRC32(record->xl_crc, crc))
	{
		// printf("incorrect resource manager data checksum in record at %X/%X\n",
		// 	   recptr.xlogid, recptr.xrecoff);
		return false;
	}

	return true;
}

void parseWAL(char* fname) {
	logFd = open(fname, O_RDONLY | PG_BINARY, 0);

	if (logFd >= 0) {
		char	*fnamebase;

		// printf("\n%s:\n\n", fname);

		fnamebase = strrchr(fname, '/');

		if (fnamebase) fnamebase++;
		else fnamebase = fname;

		if (sscanf(fnamebase, "%8x%8x%8x", &logTLI, &logId, &logSeg) != 3) {
			// Can't recognize logfile name (fnamebase)
			logTLI = logId = logSeg = 0;
		}

		logPageOff = -XLOG_BLCKSZ;		/* so 1st increment in readXLogPage gives 0 */
		logRecOff = 0;

		while (ReadRecord()) {
			dumpXLogRecord((XLogRecord *) readRecordBuf, false);

			prevRecPtr = curRecPtr;
		}

		close(logFd);
	}
}

int
main(int argc, char** argv)
{
	char *fname = argv[1];
	if (argc > 2) {
		lastOffset = atoi(argv[2]);
	}

	parseWAL(fname);

	exit(0);
	
	/* just to avoid a warning */
	return 0;
}