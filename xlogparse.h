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


#if defined(__cplusplus)
}
#endif

#endif  /* if !defined(__xlogparse_h) */
