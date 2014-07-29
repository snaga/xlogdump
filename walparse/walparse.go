package walparse

/*
#cgo CFLAGS: -I../xlogtranslate
#cgo LDFLAGS: libxlogtranslate.a
#include <stddef.h>
#include "xlogtranslate.h"
*/
import "C"

type WalEntry struct {
	EntryType	rune
	RmId		uint8
	Info		uint8
	XLogId		uint32
	XRecOff		uint32
	XId			uint32
	Space		int32
	DB			int32
	Relation	int32
	FromBlk		uint32
	FromOff		uint32
	ToBlk		uint32
	ToOff		uint32
}

const (
	RM_XLOG_ID		= 0
	RM_XACT_ID		= 1
	RM_SMGR_ID		= 2
	RM_CLOG_ID		= 3
	RM_DBASE_ID		= 4
	RM_TBLSPC_ID	= 5
	RM_MULTIXACT_ID	= 6
	RM_RELMAP_ID	= 7
	RM_STANDBY_ID	= 8
	RM_HEAP2_ID		= 9
	RM_HEAP_ID		= 10
	RM_BTREE_ID		= 11
	RM_HASH_ID		= 12
	RM_GIN_ID		= 13
	RM_GIST_ID		= 14
	RM_SEQ_ID		= 15
)

const (
	XLOG_XACT_COMMIT			= 0x00
	XLOG_XACT_PREPARE			= 0x10
	XLOG_XACT_ABORT				= 0x20
	XLOG_XACT_COMMIT_PREPARED	= 0x30
	XLOG_XACT_ABORT_PREPARED	= 0x40
	XLOG_XACT_ASSIGNMENT		= 0x50
)

func ParseWalFile(filename string, lastOffset int) ([]WalEntry) {
	entries := make([]WalEntry, 0)

	result := C.parseWalFile(C.CString(filename), C.uint32_t(lastOffset))

	current := result

	for current != nil {
		entry := WalEntry{
			rune(current.entryType),
			uint8(current.rmid),
			uint8(current.info),
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
