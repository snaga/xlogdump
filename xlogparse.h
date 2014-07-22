/*
 * xlogparse.h
 *
 * Header file for the xlogparse library.
 */
#if !defined(__xlogparse_h)
#define __xlogparse_h

#include <postgres.h>
#include <access/xlog_internal.h>


#if defined(__cplusplus)
extern "C" {
#endif


struct bounded_text {
	const char *bytes;
	unsigned int len;
};

union anyVal {
	int16 int16_val;
	int32 int32_val;
	int64 int64_val;
	float4 float4_val;
	float8 float8_val;
	bool bool_val;
	Timestamp time_val;
	struct bounded_text text_val;
};

enum xlp_ReadState {
	XL_PARSE_FAILED,
	XL_PARSE_OK,
	XL_PARSE_EOL,
	XL_PARSE_SWITCH
};

extern enum xlp_ReadState
xlp_ReadRecord(int logFd, int * logRecOff, int * logPageOff,
	       char **readRecordBuf, unsigned int *readRecordBufSize,
	       char *pageBuffer, XLogRecPtr * curRecPtr, uint32_t logSeg);


extern int
xlp_DecodeValue(const char *tup, unsigned int offset,
		Oid atttypid, int attlen, int attalign, char attbyval,
		uint32 tuplen, union anyVal *v);


#if defined(__cplusplus)
}
#endif

#endif  /* if !defined(__xlogparse_h) */
