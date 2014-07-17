/*
 * xlogparse.c
 *
 * A collection of utility functions allowing clients to programmatically parse
 * records from a given WAL file.
 */

#include <unistd.h>
#include "xlogparse.h"
#include <catalog/pg_control.h>

/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record has been read into memory at *record.
 */
static bool
RecordIsValid(const XLogRecord *record, const XLogRecPtr *recptr)
{
	pg_crc32	crc;
	int		i;
	uint32		len = record->xl_len;
	BkpBlock	bkpb;
	char	       *blk;

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
				   recptr->xlogid, recptr->xrecoff);
			return false;
		}
		blen = sizeof(BkpBlock) + BLCKSZ - bkpb.hole_length;
		COMP_CRC32(crc, blk, blen);
		blk += blen;
	}

	/* skip total xl_tot_len check if physical log has been removed. */
#if PG_VERSION_NUM < 80300 || PG_VERSION_NUM >= 90200
	if (record->xl_info & XLR_BKP_BLOCK_MASK)
#else
	if (!(record->xl_info & XLR_BKP_REMOVABLE) ||
	    record->xl_info & XLR_BKP_BLOCK_MASK)
#endif
	{
		/* Check that xl_tot_len agrees with our calculation */
		if (blk != (char *) record + record->xl_tot_len)
		{
			printf("incorrect total length in record at %X/%X\n",
			       recptr->xlogid, recptr->xrecoff);
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
		       recptr->xlogid, recptr->xrecoff);
		return false;
	}

	return true;
}


/* Read another page, if possible */
static bool
readXLogPage(int logFd, char * pageBuffer, int * logPageOff, bool dump_pageinfo)
{
	size_t nread = read(logFd, pageBuffer, XLOG_BLCKSZ);

	if (nread == XLOG_BLCKSZ)
	{
		*logPageOff += XLOG_BLCKSZ;
		if (((XLogPageHeader) pageBuffer)->xlp_magic != XLOG_PAGE_MAGIC)
		{
			printf("Bogus page magic number %04X at offset %X\n",
			       ((XLogPageHeader) pageBuffer)->xlp_magic, *logPageOff);
		}

		/*
		 * FIXME: check xlp_magic here.
		 */
		if (dump_pageinfo)
		{
			printf("[page:%d, xlp_info:%d, xlp_tli:%d, xlp_pageaddr:%X/%X] ",
			       *logPageOff / XLOG_BLCKSZ,
			       ((XLogPageHeader) pageBuffer)->xlp_info,
			       ((XLogPageHeader) pageBuffer)->xlp_tli,
			       ((XLogPageHeader) pageBuffer)->xlp_pageaddr.xlogid,
			       ((XLogPageHeader) pageBuffer)->xlp_pageaddr.xrecoff);

			if ( (((XLogPageHeader)pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD) )
				fputs("XLP_FIRST_IS_CONTRECORD ", stdout);
			if ((((XLogPageHeader)pageBuffer)->xlp_info & XLP_LONG_HEADER) )
				fputs("XLP_LONG_HEADER ", stdout);
#if PG_VERSION_NUM >= 90200
			if ((((XLogPageHeader)pageBuffer)->xlp_info & XLP_BKP_REMOVABLE) )
				fputs("XLP_BKP_REMOVABLE ", stdout);
#endif

			putchar('\n');
		}

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
 * Attempt to read an XLOG record into readRecordBuf.
 */
enum xlp_ReadState
xlp_ReadRecord(int logFd, int * logRecOff, int * logPageOff,
	       char ** readRecordBuf, unsigned int *readRecordBufSize,
	       char * pageBuffer, XLogRecPtr * curRecPtr, uint32_t logSeg)
{
	char	   *buffer;
	XLogRecord *record;
	XLogContRecord *contrecord;
	uint32		len, total_len;

	while (*logRecOff <= 0 || *logRecOff > XLOG_BLCKSZ - SizeOfXLogRecord)
	{
		/* Need to advance to new page */
		if (! readXLogPage(logFd, pageBuffer, logPageOff, true))
			return XL_PARSE_FAILED;
		*logRecOff = XLogPageHeaderSize((XLogPageHeader) pageBuffer);
		if ((((XLogPageHeader) pageBuffer)->xlp_info & ~XLP_LONG_HEADER) != 0)
		{
			printf("Unexpected page info flags %04X at offset %X\n",
				   ((XLogPageHeader) pageBuffer)->xlp_info, *logPageOff);
			/* Check for a continuation record */
			if (((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD)
			{
				printf("Skipping unexpected continuation record at offset %X\n",
					   *logPageOff);
				contrecord = (XLogContRecord *) (pageBuffer + *logRecOff);
				*logRecOff += MAXALIGN(contrecord->xl_rem_len + SizeOfXLogContRecord);
			}
		}
	}

	curRecPtr->xrecoff = logSeg * XLogSegSize + *logPageOff + *logRecOff;
	record = (XLogRecord *) ((char *)pageBuffer + *logRecOff);

	if (record->xl_len == 0)
	{
		/* Stop if XLOG_SWITCH was found. */
		if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH)
		{
			return XL_PARSE_SWITCH;
		}
		return XL_PARSE_EOL;
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
			curRecPtr->xlogid, curRecPtr->xrecoff);
		puts("HINT: Make sure you're using the correct xlogdump binary built against\n"
		     "      the same architecture and version of PostgreSQL where the WAL file\n"
		     "      comes from.");
		return XL_PARSE_FAILED;
	}
	total_len = record->xl_tot_len;

	/*
	 * Allocate or enlarge readRecordBuf as needed.  To avoid useless
	 * small increases, round its size to a multiple of XLOG_BLCKSZ, and make
	 * sure it's at least 4*BLCKSZ to start with.  (That is enough for all
	 * "normal" records, but very large commit or abort records might need
	 * more space.)
	 */
	if (total_len > *readRecordBufSize)
	{
		uint32 newSize = total_len;

		newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
		newSize = Max(newSize, 4 * XLOG_BLCKSZ);
		if (*readRecordBuf)
			free(*readRecordBuf);
		*readRecordBuf = (char *) malloc(newSize);
		if (!*readRecordBuf)
		{
			*readRecordBufSize = 0;
			/* We treat this as a "bogus data" condition */
			fprintf(stderr, "record length %u at %X/%X too long\n",
					total_len, curRecPtr->xlogid, curRecPtr->xrecoff);
			return XL_PARSE_FAILED;
		}
		*readRecordBufSize = newSize;
	}

	buffer = *readRecordBuf;
	len = XLOG_BLCKSZ - curRecPtr->xrecoff % XLOG_BLCKSZ; /* available in block */
	if (total_len > len)
	{
		/* Need to reassemble record */
		uint32 gotlen = len;

		memcpy(buffer, record, len);
		record = (XLogRecord *) buffer;
		buffer += len;
		for (;;)
		{
			uint32 pageHeaderSize;

			if (! readXLogPage(logFd, pageBuffer, logPageOff, true))
			{
				/* XXX ought to be able to advance to new input file! */
				fputs("Unable to read continuation page?\n", stderr);
				return XL_PARSE_FAILED;
			}
			if (!(((XLogPageHeader) pageBuffer)->xlp_info & XLP_FIRST_IS_CONTRECORD))
			{
				printf("ReadRecord: there is no ContRecord flag in logfile %u seg %u off %u\n",
				       curRecPtr->xlogid, logSeg, *logPageOff);
				return XL_PARSE_FAILED;
			}
			pageHeaderSize = XLogPageHeaderSize((XLogPageHeader) pageBuffer);
			contrecord = (XLogContRecord *) (pageBuffer + pageHeaderSize);
			if (contrecord->xl_rem_len == 0 ||
				total_len != (contrecord->xl_rem_len + gotlen))
			{
				printf("ReadRecord: invalid cont-record len %u in logfile %u seg %u off %u\n",
				       contrecord->xl_rem_len, curRecPtr->xlogid, logSeg, *logPageOff);
				return XL_PARSE_FAILED;
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
			*logRecOff = MAXALIGN(pageHeaderSize + SizeOfXLogContRecord + contrecord->xl_rem_len);
			break;
		}
		if (!RecordIsValid(record, curRecPtr))
			return XL_PARSE_FAILED;
		return XL_PARSE_OK;
	}
	/* Record is contained in this page */
	memcpy(buffer, record, total_len);
	record = (XLogRecord *) buffer;
	*logRecOff += MAXALIGN(total_len);
	if (!RecordIsValid(record, curRecPtr))
		return XL_PARSE_FAILED;
	return XL_PARSE_OK;
}
