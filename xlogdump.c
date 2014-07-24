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

#include "access/transam.h"
#include "access/xact.h"
#include "pg_config.h"

#include "xlogdump.h"
#include "xlogdump_rmgr.h"
#include "xlogdump_statement.h"
#include "xlogdump_oid2name.h"
#include "xlogparse.h"

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
static bool		transactions = false;	/* when true we just aggregate transaction info */
static bool		statements = false;	/* when true we try to rebuild fake sql statements with the xlog data */
static bool		hideTimestamps = false; /* remove timestamp from dump used for testing */
static bool		enable_stats = false;	/* collect and show statistics */
static int		rmid = -1;		/* print all RM's xlog records if rmid has negative value. */
static TransactionId	xid = InvalidTransactionId;

/* Buffers to hold objects names */
static char		spaceName[NAMEDATALEN] = "";
static char		dbName[NAMEDATALEN]    = "";
static char		relName[NAMEDATALEN]   = "";


struct xlog_stats_t {
	int rmgr_count[RM_MAX_ID+1];
	int rmgr_len[RM_MAX_ID+1];
	int bkpblock_count;
	int bkpblock_len;
};

struct xlog_stats_t xlogstats;

/* struct to aggregate transactions */
transInfoPtr		transactionsInfo = NULL;


/* prototypes */
static void print_xlog_stats();

void exit_gracefuly(int);

static void dumpXLogRecord(XLogRecord *, bool);
static void print_backup_blocks(XLogRecPtr, XLogRecord *);

static void addTransaction(XLogRecord *);
static void dumpTransactions();
static void dumpXLog(char *);
static void help(void);

static void
print_xlog_stats()
{
	int i;
	double avg = 0;

	printf("---------------------------------------------------------------\n");
	printf("TimeLineId: %d, LogId: %d, LogSegment: %d\n", logTLI, logId, logSeg);
	printf("\n");

	printf("Resource manager stats: \n");
	for (i=0 ; i<RM_MAX_ID+1 ; i++)
	{
		avg = 0;

		if ( xlogstats.rmgr_count[i]>0 )
			avg = (double)xlogstats.rmgr_len[i] / (double)xlogstats.rmgr_count[i];
		  
		printf("  [%d]%-10s: %d record%s, %d byte%s (avg %.1f byte%s)\n",
		       i, RM_names[i],
		       xlogstats.rmgr_count[i], (xlogstats.rmgr_count[i]>1) ? "s" : "",
		       xlogstats.rmgr_len[i], (xlogstats.rmgr_len[i]>1) ? "s" : "",
		       avg, (avg>1) ? "s" : "");

		print_xlog_rmgr_stats(i);
	}

	avg = 0;
	if ( xlogstats.bkpblock_count>0 )
		avg = (double)xlogstats.bkpblock_len / (double)xlogstats.bkpblock_count;

	printf("\nBackup block stats: %d block%s, %d byte%s (avg %.1f byte%s)\n",
	       xlogstats.bkpblock_count, (xlogstats.bkpblock_count>1) ? "s" : "",
	       xlogstats.bkpblock_len,  (xlogstats.bkpblock_len>1) ? "s" : "",
	       avg, (avg>1) ? "s" : "");

	printf("\n");
}


/* 
 * Exit closing active database connections
 */
void
exit_gracefuly(int status)
{
	DBDisconnect();

	close(logFd);
	exit(status);
}


static void
dumpXLogRecord(XLogRecord *record, bool header_only)
{
	uint8	info = record->xl_info & ~XLR_INFO_MASK;

	/* check if the user wants a specific rmid */
	if (rmid>=0 && record->xl_rmid!=rmid)
		return;

	if (xid!=InvalidTransactionId && xid!=record->xl_xid)
		return;

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
	xlogstats.rmgr_count[record->xl_rmid]++;
	xlogstats.rmgr_len[record->xl_rmid] += record->xl_len;
	switch (record->xl_rmid)
	{
		case RM_XLOG_ID:
			print_rmgr_xlog(curRecPtr, record, info, hideTimestamps);
			break;
		case RM_XACT_ID:
			print_rmgr_xact(curRecPtr, record, info, hideTimestamps);
			break;
		case RM_SMGR_ID:
			print_rmgr_smgr(curRecPtr, record, info);
			break;
		case RM_CLOG_ID:
			print_rmgr_clog(curRecPtr, record, info);
			break;
		case RM_DBASE_ID:
			print_rmgr_dbase(curRecPtr, record, info);
			break;
		case RM_TBLSPC_ID:
			print_rmgr_tblspc(curRecPtr, record, info);
			break;
		case RM_MULTIXACT_ID:
			print_rmgr_multixact(curRecPtr, record, info);
			break;
#if PG_VERSION_NUM >= 90000
		case RM_RELMAP_ID:
			print_rmgr_relmap(curRecPtr, record, info);
			break;
		case RM_STANDBY_ID:
			print_rmgr_standby(curRecPtr, record, info);
			break;
#endif
		case RM_HEAP2_ID:
			print_rmgr_heap2(curRecPtr, record, info);
			break;
		case RM_HEAP_ID:
			print_rmgr_heap(curRecPtr, record, info, statements);
			break;
		case RM_BTREE_ID:
			print_rmgr_btree(curRecPtr, record, info);
			break;
		case RM_HASH_ID:
			print_rmgr_hash(curRecPtr, record, info);
			break;
		case RM_GIN_ID:
			print_rmgr_gin(curRecPtr, record, info);
			break;
		case RM_GIST_ID:
			print_rmgr_gist(curRecPtr, record, info);
			break;
		case RM_SEQ_ID:
			print_rmgr_seq(curRecPtr, record, info);
			break;
		default:
			fprintf(stderr, "Unknown RMID %d.\n", record->xl_rmid);
			break;
	}

	/*
	 * print info about backup blocks.
	 */
	print_backup_blocks(curRecPtr, record);
}

static void
print_backup_blocks(XLogRecPtr cur, XLogRecord *rec)
{
	char *blk;
	int i;
	char buf[1024];

	/*
	 * backup blocks by full_page_write
	 */
	blk = (char*)XLogRecGetData(rec) + rec->xl_len;
	for (i = 0; i < XLR_MAX_BKP_BLOCKS; i++)
	{
		BkpBlock  bkb;

		if (!(rec->xl_info & (XLR_SET_BKP_BLOCK(i))))
			continue;
		memcpy(&bkb, blk, sizeof(BkpBlock));
		getSpaceName(bkb.node.spcNode, spaceName, sizeof(spaceName));
		getDbName(bkb.node.dbNode, dbName, sizeof(dbName));
		getRelName(bkb.node.relNode, relName, sizeof(relName));
		snprintf(buf, sizeof(buf), "bkpblock[%d]: s/d/r:%s/%s/%s blk:%u hole_off/len:%u/%u\n", 
				i+1, spaceName, dbName, relName,
				bkb.block, bkb.hole_offset, bkb.hole_length);
		blk += sizeof(BkpBlock) + (BLCKSZ - bkb.hole_length);

		if (!enable_stats)
		{
			PRINT_XLOGRECORD_HEADER(cur, rec);
			printf("%s", buf);
		}

		xlogstats.bkpblock_count++;
		xlogstats.bkpblock_len += (BLCKSZ - bkb.hole_length);
	}
}


/*
 * Adds a transaction to a linked list of transactions
 * If the transactions xid already is on the list it sums the total len and check for a status change
 */
static void
addTransaction(XLogRecord *record)
{
	uint8	info = record->xl_info & ~XLR_INFO_MASK;
	int	status = 0;
	if(record->xl_rmid == RM_XACT_ID)
	{
		if (info == XLOG_XACT_COMMIT)
			status = 1;
		else if (info == XLOG_XACT_ABORT)
			status = 2;
	}

	if(transactionsInfo != NULL)
	{
		transInfoPtr element = transactionsInfo;
		while (element->next != NULL || element->xid == record->xl_xid)
		{
			if(element->xid == record->xl_xid)
			{
				element->tot_len += record->xl_tot_len;
				if(element->status == 0)
					element->status = status;
				return;
			}
			element = element->next;
		}
		element->next = (transInfoPtr) malloc(sizeof(transInfo));
		element = element->next;
		element->xid = record->xl_xid;
		element->tot_len = record->xl_tot_len;
		element->status = status;
		element->next = NULL;
		return;
	}
	else
	{
		transactionsInfo = (transInfoPtr) malloc(sizeof(transInfo));
		transactionsInfo->xid = record->xl_xid;
		transactionsInfo->tot_len = record->xl_tot_len;
		transactionsInfo->status = status;
		transactionsInfo->next = NULL;
	}
}

static void
dumpTransactions()
{
	transInfo * element = transactionsInfo;
	if(!element)
	{
		printf("\nCorrupt or incomplete transaction.\n");
		return;
	}

	while (element->next != NULL)
	{
		printf("\nxid: %u total length: %u status: %s", element->xid, element->tot_len, status_names[element->status]);
		element = element->next;
	}
	printf("\n");
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
	if (sscanf(fnamebase, "%8x%8x%8x", &logTLI, &curRecPtr.xlogid, &logSeg) != 3)
	{
		fprintf(stderr, "Can't recognize logfile name '%s'\n", fnamebase);
		logTLI = logId = logSeg = 0;
	}
	logPageOff = -XLOG_BLCKSZ;		/* so 1st increment in readXLogPage gives 0 */
	logRecOff = 0;
	xlp_SetPageinfoDump(true);
	while (xlp_ReadRecord(logFd, &logRecOff, &logPageOff,
			      &readRecordBuf, &readRecordBufSize, pageBuffer,
			      &curRecPtr, logSeg) == XL_PARSE_OK)
	{
		if(!transactions)
			dumpXLogRecord((XLogRecord *) readRecordBuf, false);
		else
			addTransaction((XLogRecord *) readRecordBuf);

		prevRecPtr = curRecPtr;
	}
	if(transactions)
		dumpTransactions();
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
	printf("  -t, --transactions        Outputs only transaction info: the xid,\n");
	printf("                            total length and status of each transaction.\n");
	printf("  -s, --statements          Tries to build fake statements that produce the\n");
	printf("                            physical changes found within the xlog segments.\n");
	printf("  -S, --stats               Collects and shows statistics of the transaction\n");
	printf("                            log records from the xlog segments.\n");
	printf("  -n, --oid2name            Show object names instead of OIDs with looking up\n");
	printf("                            the system catalogs or a cache file.\n");
	printf("  -g, --gen_oid2name        Generate an oid2name cache file (oid2name.out)\n");
	printf("                            by reading the system catalogs.\n");
	printf("  -T, --hide-timestamps     Do not print timestamps.\n");
	printf("  -?, --help                Show this help.\n");
	printf("\n");
	printf("oid2name supplimental options:\n");
	printf("  -h, --host=HOST           database server host or socket directory\n");
	printf("  -p, --port=PORT           database server port number\n");
	printf("  -U, --user=NAME           database user name to connect\n");
	printf("  -d, --dbname=NAME         database name to connect\n");
	printf("  -f, --file=FILE           file name to read oid2name cache\n");
	printf("\n");
	printf("Report bugs to <satoshi.nagayasu@gmail.com>.\n");
	exit(0);
}

int
main(int argc, char** argv)
{
	int	c, i, optindex;
	bool oid2name = false;
	bool oid2name_gen = false;
	char *pghost = NULL; /* connection host */
	char *pgport = NULL; /* connection port */
	char *pguser = NULL; /* connection username */
	char *dbname = NULL; /* connection database name */
	char *oid2name_file = NULL;

	static struct option long_options[] = {
		{"transactions", no_argument, NULL, 't'},
		{"statements", no_argument, NULL, 's'},
		{"stats", no_argument, NULL, 'S'},
		{"hide-timestamps", no_argument, NULL, 'T'},	
		{"rmid", required_argument, NULL, 'r'},
		{"oid2name", no_argument, NULL, 'n'},
		{"gen_oid2name", no_argument, NULL, 'g'},
		{"xid", required_argument, NULL, 'x'},
		{"host", required_argument, NULL, 'h'},
		{"port", required_argument, NULL, 'p'},
		{"user", required_argument, NULL, 'U'},
		{"dbname", required_argument, NULL, 'd'},
		{"file", required_argument, NULL, 'f'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	
	if (argc == 1 || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))
		help();

	pghost = strdup("localhost");
	pgport = strdup("5432");
	pguser = getenv("USER");
	dbname = strdup("postgres");
	oid2name_file = strdup(DATADIR "/contrib/" OID2NAME_FILE);

	while ((c = getopt_long(argc, argv, "sStTngr:x:h:p:U:d:f:",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 's':			/* show statements */
				statements = true;
				break;

			case 'S':			/* show statistics */
				enable_stats = true;
				enable_rmgr_dump(false);
				break;

			case 't':			
				transactions = true;	/* show only transactions */
				break;

			case 'T':			/* hide timestamps (used for testing) */
				hideTimestamps = true;
				break;
			case 'n':
				oid2name = true;
				break;
			case 'g':
				oid2name_gen = true;
				break;
			case 'r':			/* output only rmid passed */
			  	rmid = atoi(optarg);
				break;
			case 'x':			/* output only xid passed */
			  	xid = atoi(optarg);
				break;
			case 'h':			/* host for tranlsting oids */
				pghost = optarg;
				break;
			case 'p':			/* port for translating oids */
				pgport = optarg;
				break;
			case 'U':			/* username for translating oids */
				pguser = optarg;
				break;
			case 'd':			/* database name for translating oids */
				dbname = optarg;
				break;
			case 'f':
				oid2name_file = optarg;
				break;
			default:
				fprintf(stderr, "Try \"xlogdump --help\" for more information.\n");
				exit(1);
		}
	}

	if (statements && transactions)
	{
		fprintf(stderr, "options \"statements\" (-s) and \"transactions\" (-t) cannot be used together\n");
		exit(1);
	}

	if (rmid>=0 && transactions)
	{
		fprintf(stderr, "options \"rmid\" (-r) and \"transactions\" (-t) cannot be used together\n");
		exit(1);
	}

	if (oid2name)
	{
		if ( !oid2name_from_file(oid2name_file) )
		{
			/* if not found in share/contrib, read from the cwd. */
			oid2name_from_file(OID2NAME_FILE);
		}

		if ( !DBConnect(pghost, pgport, dbname, pguser) )
			fprintf(stderr, "WARNING: Database connection to lookup the system catalog is not available.\n");
	}

	/*
	 * Generate an oid2name cache file.
	 */
	if (oid2name_gen)
	{
		if ( !DBConnect(pghost, pgport, dbname, pguser) )
			exit_gracefuly(1);

		if (oid2name_to_file("oid2name.out"))
		{
			printf("oid2name.out successfully created.\n");
		}

		exit_gracefuly(0);
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

	if (enable_stats)
		print_xlog_stats();

	exit_gracefuly(0);
	
	/* just to avoid a warning */
	return 0;
}

/*
 * Routines needed if headers were configured for ASSERT
 */
#ifndef assert_enabled
bool		assert_enabled = true;

#if PG_VERSION_NUM < 80300
int
ExceptionalCondition(char *conditionName,
					 char *errorType,
					 char *fileName,
					 int lineNumber)
{
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType, conditionName,
			fileName, lineNumber);

	abort();
	return 0;
}
#elif PG_VERSION_NUM >= 80300 && PG_VERSION_NUM < 90200
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
#else
void
ExceptionalCondition(const char *conditionName,
		     const char *errorType,
		     const char *fileName,
		     int lineNumber)
{
	if (!PointerIsValid(conditionName)
	    || !PointerIsValid(fileName)
	    || !PointerIsValid(errorType))
		fprintf(stderr, "TRAP: ExceptionalCondition: bad arguments\n");
	else
	{
		fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			     errorType, conditionName,
			     fileName, lineNumber);
	}

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

#ifdef SLEEP_ON_ASSERT

	/*
	 * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
	 * minutes, which seems too short.
	 */
	sleep(1000000);
#endif

	abort();
}
#endif

#endif /* assert_enabled */
