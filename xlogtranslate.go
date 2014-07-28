package xlogtranslate

import "fmt"

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: ./libxlogtranslate.a
#include <stddef.h>
#include "xlogtranslate.h"
*/
import "C"

type WalEntry struct {
	entryType	rune
	xlogid		uint32
	xrecoff		uint32
	xid			uint32
	space		int32
	db			int32
	relation	int32
	fromBlk		uint32
	fromOff		uint32
	toBlk		uint32
	toOff		uint32
}

func parseWalFile(filename string, lastOffset int) ([]WalEntry) {
	entries := make([]WalEntry, 0)

	result := C.parseWalFile(C.CString(filename), C.uint32_t(lastOffset))

	current := result

	for current != nil {
		entry := WalEntry{
			rune(current.entryType),
			uint32(current.xlogid),
			uint32(current.xrecoff),
			uint32(current.xid),
			int32(current.space),
			int32(current.db),
			int32(current.relation),
			uint32(current.fromBlk),
			uint32(current.fromOff),
			uint32(current.toBlk),
			uint32(current.toOff),
		}

		entries = append(entries, entry)

		current = (*C.Result)(current.next)
	}

	C.freeWalResult(result);

	return entries
}
