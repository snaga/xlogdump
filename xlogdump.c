/*
 * xlogdump.c
 *
 * A tool for extracting data from the PostgreSQL's write ahead logs.
 *
 * Satoshi Nagayasu <satoshi.nagayasu@gmail.com>
 * Diogo Biazus <diogob@gmail.com>
 * Based on the original xlogdump code by Tom Lane
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE AUTHOR OR DISTRIBUTORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR AND DISTRIBUTORS SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE AUTHOR AND DISTRIBUTORS HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */
#include "postgres.h"

#include <fcntl.h>
#include <getopt_long.h>
#include <time.h>
#include <unistd.h>

#include "access/tupmacs.h"
#include "access/nbtree.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "utils/pg_crc.h"

#include "pg_crc32_table.h"

#include "libpq-fe.h"
#include "pg_config.h"
#include "pqexpbuffer.h"

#include "strlcat.h"
#include "xlogdump.h"
#include "xlogdump_rmgr.h"

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

/* command-line parameters */
static bool		hideTimestamps = false; /* remove timestamp from dump used for testing */

/* struct to aggregate transactions */
transInfoPtr		transactionsInfo = NULL;


/* prototypes */
static bool readXLogPage(void);
void exit_gracefuly(int);
static bool RecordIsValid(XLogRecord *, XLogRecPtr);
static bool ReadRecord(void);

static void dumpXLogRecord(XLogRecord *, bool);

static void dumpXLog(char *);
static void help(void);

/* Read another page, if possible */
static bool
readXLogPage(void)
{
	size_t nread = read(logFd, pageBuffer, XLOG_BLCKSZ);

	if (nread == XLOG_BLCKSZ)
	{
		logPageOff += XLOG_BLCKSZ;
		if (((XLogPageHeader) pageBuffer)->xlp_magic != XLOG_PAGE_MAGIC)
		{
			printf("Bogus page magic number %04X at offset %X\n",
				   ((XLogPageHeader) pageBuffer)->xlp_magic, logPageOff);
		}

		/*
		 * FIXME: check xlp_magic here.
		 */
		printf("[page:%d, xlp_info:%d, xlp_tli:%d, xlp_pageaddr:%X/%X] ",
		       logPageOff / XLOG_BLCKSZ,
		       ((XLogPageHeader) pageBuffer)->xlp_info,
		       ((XLogPageHeader) pageBuffer)->xlp_tli,
		       ((XLogPageHeader) pageBuffer)->xlp_pageaddr.xlogid,
		       ((XLogPageHeader) pageBuffer)->xlp_pageaddr.xrecoff);
		
		if ( (((XLogPageHeader)pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD) )
			printf("XLP_FIRST_IS_CONTRECORD ");
		if ((((XLogPageHeader)pageBuffer)->xlp_info & XLP_LONG_HEADER) )
			printf("XLP_LONG_HEADER ");
		
		printf("\n");

		return true;
	}
	if (nread != 0)
	{
		fprintf(stderr, "Partial page of %d bytes ignored\n",
			(int) nread);
	}
	return false;
}

/* 
 * Exit closing active database connections
 */
void
exit_gracefuly(int status)
{
	close(logFd);
	exit(status);
}

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
			printf("incorrect hole size in record at %X/%X\n",
				   recptr.xlogid, recptr.xrecoff);
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
			printf("incorrect total length in record at %X/%X\n",
				   recptr.xlogid, recptr.xrecoff);
			return false;
		}
	}

	/* Finally include the record header */
	COMP_CRC32(crc, (char *) record + sizeof(pg_crc32),
			   SizeOfXLogRecord - sizeof(pg_crc32));
	FIN_CRC32(crc);

	if (!EQ_CRC32(record->xl_crc, crc))
	{
		printf("incorrect resource manager data checksum in record at %X/%X\n",
			   recptr.xlogid, recptr.xrecoff);
		return false;
	}

	return true;
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
			printf("Unexpected page info flags %04X at offset %X\n",
				   ((XLogPageHeader) pageBuffer)->xlp_info, logPageOff);
			/* Check for a continuation record */
			if (((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD)
			{
				printf("Skipping unexpected continuation record at offset %X\n",
					   logPageOff);
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

		printf("ReadRecord: record with zero len at %u/%08X\n",
		   curRecPtr.xlogid, curRecPtr.xrecoff);

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
		printf(
			"invalid record length(expected %lu ~ %lu, actual %d) at %X/%X\n",
			(unsigned long) (SizeOfXLogRecord + record->xl_len),
			(unsigned long) (SizeOfXLogRecord + record->xl_len +
							 XLR_MAX_BKP_BLOCKS * (sizeof(BkpBlock) + BLCKSZ)),
			record->xl_tot_len,
			curRecPtr.xlogid, curRecPtr.xrecoff);
		printf("HINT: Make sure you're using the correct xlogdump binary built against\n"
		       "      the same architecture and version of PostgreSQL where the WAL file\n"
		       "      comes from.\n");
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
			fprintf(stderr, "record length %u at %X/%X too long\n",
					total_len, curRecPtr.xlogid, curRecPtr.xrecoff);
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
				fprintf(stderr, "Unable to read continuation page?\n");
				dumpXLogRecord(record, true);
				return false;
			}
			if (!(((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				printf("ReadRecord: there is no ContRecord flag in logfile %u seg %u off %u\n",
					   logId, logSeg, logPageOff);
				return false;
			}
			pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) pageBuffer);
			contrecord = (XLogContRecord *) (pageBuffer + pageHeaderSize);
			if (contrecord->xl_rem_len == 0 || 
				total_len != (contrecord->xl_rem_len + gotlen))
			{
				printf("ReadRecord: invalid cont-record len %u in logfile %u seg %u off %u\n",
					   contrecord->xl_rem_len, logId, logSeg, logPageOff);
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

#ifdef NOT_USED
	printf("%u/%08X: prv %u/%08X",
		   curRecPtr.xlogid, curRecPtr.xrecoff,
		   record->xl_prev.xlogid, record->xl_prev.xrecoff);

	if (!XLByteEQ(record->xl_prev, prevRecPtr))
		printf("(?)");

	printf("; xid %u; ", record->xl_xid);

	if (record->xl_rmid <= RM_MAX_ID)
		printf("%s", RM_names[record->xl_rmid]);
	else
		printf("RM %2d", record->xl_rmid);

	printf(" info %02X len %u tot_len %u\n", record->xl_info,
		   record->xl_len, record->xl_tot_len);
#endif

	if (header_only)
	{
		printf(" ** maybe continues to next segment **\n");
		return;
	}

	/*
	 * See rmgr.h for more details about the built-in resource managers.
	 */
	switch (record->xl_rmid)
	{
		case RM_HEAP2_ID:
			print_rmgr_heap2(curRecPtr, record, info);
			break;
		case RM_HEAP_ID:
			print_rmgr_heap(curRecPtr, record, info);
			break;
		// case RM_SEQ_ID:
		// 	print_rmgr_seq(curRecPtr, record, info);
		// 	break;
		// default:
		// 	fprintf(stderr, "Unknown RMID %d.\n", record->xl_rmid);
		// 	break;
	}
}

static void
dumpXLog(char* fname)
{
	char	*fnamebase;

	printf("\n%s:\n\n", fname);
	/*
	 * Extract logfile id and segment from file name
	 */
	fnamebase = strrchr(fname, '/');
	if (fnamebase)
		fnamebase++;
	else
		fnamebase = fname;
	if (sscanf(fnamebase, "%8x%8x%8x", &logTLI, &logId, &logSeg) != 3)
	{
		fprintf(stderr, "Can't recognize logfile name '%s'\n", fnamebase);
		logTLI = logId = logSeg = 0;
	}
	logPageOff = -XLOG_BLCKSZ;		/* so 1st increment in readXLogPage gives 0 */
	logRecOff = 0;
	while (ReadRecord())
	{
		dumpXLogRecord((XLogRecord *) readRecordBuf, false);

		prevRecPtr = curRecPtr;
	}
}

static void
help(void)
{
	int i;

	printf("xlogdump version %s\n\n", VERSION_STR);
	printf("Usage:\n");
	printf("  xlogdump [OPTION]... [segment file(s)]\n");
	printf("\nOptions:\n");
	printf("  -r, --rmid=RMID           Outputs only the transaction log records\n"); 
	printf("                            containing the specified operation.\n");
	printf("                            RMID:Resource Manager\n");
	for (i=0 ; i<RM_MAX_ID+1 ; i++)
		printf("                              %2d:%s\n", i, RM_names[i]);
	printf("  -x, --xid=XID             Outputs only the transaction log records\n"); 
	printf("                            containing the specified transaction id.\n");
	printf("  -S, --stats               Collects and shows statistics of the transaction\n");
	printf("                            log records from the xlog segments.\n");
	printf("  -T, --hide-timestamps     Do not print timestamps.\n");
	printf("  -?, --help                Show this help.\n");
	printf("\n");
	printf("Report bugs to <satoshi.nagayasu@gmail.com>.\n");
	exit(0);
}

int
main(int argc, char** argv)
{
	int	c, i, optindex;

	static struct option long_options[] = {
		{"stats", no_argument, NULL, 'S'},
		{"hide-timestamps", no_argument, NULL, 'T'},	
		{"rmid", required_argument, NULL, 'r'},
		{"xid", required_argument, NULL, 'x'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	
	if (argc == 1 || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))
		help();

	while ((c = getopt_long(argc, argv, "sStTngr:x:h:p:U:d:f:",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'T':			/* hide timestamps (used for testing) */
				hideTimestamps = true;
				break;
			default:
				fprintf(stderr, "Try \"xlogdump --help\" for more information.\n");
				exit(1);
		}
	}

	for (i = optind; i < argc; i++)
	{
		char *fname = argv[i];
		logFd = open(fname, O_RDONLY | PG_BINARY, 0);

		if (logFd < 0)
		{
			perror(fname);
			continue;
		}
		dumpXLog(fname);
	}

	exit_gracefuly(0);
	
	/* just to avoid a warning */
	return 0;
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
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType, conditionName,
			fileName, lineNumber);

	abort();
	return 0;
}

#endif /* assert_enabled */
