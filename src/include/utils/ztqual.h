/*-------------------------------------------------------------------------
 *
 * ztqual.h
 *	  POSTGRES "time qualification" definitions, ie, ztuple visibility rules.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/ztqual.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZTQUAL_H
#define ZTQUAL_H

#include "access/xlogdefs.h"
#include "access/zheap.h"

typedef struct ZHeapTupleTransInfo
{
	int			trans_slot;
	FullTransactionId epoch_xid;
	TransactionId xid;
	CommandId	cid;
	UndoRecPtr	urec_ptr;
} ZHeapTupleTransInfo;

/* Result codes for ZHeapTupleSatisfiesOldestXmin */
typedef enum
{
	ZHEAPTUPLE_DEAD,			/* tuple is dead and deletable */
	ZHEAPTUPLE_LIVE,			/* tuple is live (committed, no deleter) */
	ZHEAPTUPLE_RECENTLY_DEAD,	/* tuple is dead, but not deletable yet */
	ZHEAPTUPLE_INSERT_IN_PROGRESS,	/* inserting xact is still in progress */
	ZHEAPTUPLE_DELETE_IN_PROGRESS,	/* deleting xact is still in progress */
	ZHEAPTUPLE_ABORT_IN_PROGRESS	/* rollback is still pending */
} ZHTSV_Result;

extern void FetchTransInfoFromUndo(BlockNumber blocknum, OffsetNumber offnum,
								   TransactionId xid, ZHeapTupleTransInfo *zinfo,
								   ItemPointer new_ctid);
extern void ZHeapUpdateTransactionSlotInfo(int trans_slot, Buffer buffer,
										   OffsetNumber offnum,
										   ZHeapTupleTransInfo *zinfo);
extern void ZHeapTupleGetTransInfo(Buffer buf, OffsetNumber offnum,
								   ZHeapTupleTransInfo *zinfo);
extern TransactionId ZHeapTupleGetTransXID(ZHeapTuple zhtup, Buffer buf,
										   bool nobuflock);

/* Fetch CTID information stored in undo */
extern void ZHeapPageGetNewCtid(Buffer buffer, ItemPointer ctid,
								ZHeapTupleTransInfo *zinfo);

/* These are the "satisfies" test routines for the zheap. */
extern TM_Result ZHeapTupleSatisfiesUpdate(Relation rel, ItemPointer tid,
										   ZHeapTuple zhtup, CommandId curcid, Buffer buffer,
										   ItemPointer ctid, ZHeapTupleTransInfo *zinfo,
										   SubTransactionId *subxid, TransactionId *single_locker_xid,
										   int *single_locker_trans_slot, bool lock_allowed,
										   Snapshot snapshot, bool *in_place_updated_or_locked);
extern bool ZHeapTupleIsSurelyDead(ZHeapTuple zhtup, Buffer buffer,
								   OffsetNumber offnum);
extern ZHTSV_Result ZHeapTupleSatisfiesOldestXmin(ZHeapTuple zhtup,
												  TransactionId OldestXmin,
												  Buffer buffer, bool resolve_abort_in_progress,
												  ZHeapTuple *preabort_tuple,
												  TransactionId *xid, SubTransactionId *subxid);

extern bool ZHeapTupleFetch(Relation rel, Buffer buffer, OffsetNumber offnum,
							Snapshot snapshot, ZHeapTuple *visible_tuple,
							ItemPointer new_ctid);

extern bool ZHeapTupleHasSerializableConflictOut(bool visible, Relation relation,
												 ItemPointer tid, Buffer buffer,
												 TransactionId *xid);

#endif							/* ZTQUAL_H */
