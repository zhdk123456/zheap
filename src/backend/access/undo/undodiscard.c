/*-------------------------------------------------------------------------
 *
 * undodiscard.c
 *	  discard undo records
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undodiscard.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/undolog.h"
#include "access/undodiscard.h"
#include "access/undorequest.h"
#include "catalog/pg_tablespace.h"
#include "miscadmin.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/resowner.h"

/*
 * Discard the undo for the given log
 *
 * Search the undo log, get the start record for each transaction until we get
 * the transaction with xid >= xmin or an invalid xid.  Then call undolog
 * routine to discard up to that point and update the memory structure for the
 * log slot.  We set the hibernate flag if we do not have any undo data that
 * can be discarded, this flag is passed back to the discard worker wherein it
 * determines if the system is idle and it should sleep for some time.
 *
 * Return the oldest full_xid remaining in this undo log (which should be
 * >= xmin, since we'll discard everything older).  Returns
 * InvalidTransactionId, if the undo log is empty.
 */
static FullTransactionId
UndoDiscardOneLog(UndoLogSlot *slot, TransactionId xmin, bool *hibernate)
{
	UndoRecPtr	undo_recptr, next_insert;
	UndoRecPtr	next_urecptr = InvalidUndoRecPtr;
	UnpackedUndoRecord	*uur = NULL;
	bool	need_discard = false;
	bool	log_complete = false;
	TransactionId	undoxid = InvalidTransactionId;
	TransactionId	latest_discardxid = InvalidTransactionId;
	uint32	epoch = 0;
	UndoLogNumber logno;

	/*
	 * Currently we expect only one discard worker to be active at any time,
	 * but in future we might have more than one, and superuser maintenance
	 * functions might also discard data concurrently.  So we we have to
	 * assume that the given slot could be recycled underneath us any time we
	 * don't hold one of the locks that prevents that.  We'll detect that by
	 * the log number changing.
	 */
	LWLockAcquire(&slot->discard_lock, LW_SHARED);
	logno = slot->logno;
	if (UndoRecPtrIsValid(slot->oldest_data))
	{
		undo_recptr = slot->oldest_data;
		LWLockRelease(&slot->discard_lock);
	}
	else
	{
		LWLockRelease(&slot->discard_lock);
		undo_recptr = UndoLogGetOldestRecord(logno, NULL);
	}

	/* There might not be any undo log and hibernation might be needed. */
	*hibernate = true;

	StartTransactionCommand();

	/* Loop until we run out of discardable transactions in the given log. */
	do
	{
		bool pending_abort = false;

		next_insert = UndoLogGetNextInsertPtr(logno, InvalidTransactionId);

		if (next_insert == undo_recptr)
		{
			/*
			 * The caller of this function must have ensured that there is
			 * something to discard.
			 */
			Assert(undo_recptr != slot->oldest_data);

			/* Indicate that we have processed all the log. */
			log_complete = true;
		}
		else
		{
			/* Fetch the undo record for the given undo_recptr. */
			uur = UndoFetchRecord(undo_recptr, InvalidBlockNumber,
								  InvalidOffsetNumber, InvalidTransactionId,
								  NULL, NULL);

			if (uur != NULL)
			{
				/*
				 * Add the aborted transaction to the rollback request queues.
				 *
				 * If the undo actions for the aborted transaction is already
				 * applied then continue discarding the undo log, otherwise,
				 * discard till current point and stop processing this undo
				 * log.
				 *
				 * We can ignore the abort for transactions whose
				 * corresponding database doesn't exist.
				 *
				 * XXX: We've added the transaction-in-progress check to avoid
				 * xids of in-progress autovacuum as those are not computed
				 * for oldestxmin calculation.  See DiscardWorkerMain.
				 */
				if (!TransactionIdDidCommit(uur->uur_xid) &&
					!TransactionIdIsInProgress(uur->uur_xid) &&
					TransactionIdPrecedes(uur->uur_xid, xmin) &&
					uur->uur_progress == 0 &&
					dbid_exists(uur->uur_dbid))
				{
					FullTransactionId full_xid;

					full_xid = FullTransactionIdFromEpochAndXid(uur->uur_xidepoch,
																uur->uur_xid);
					(void) RegisterRollbackReq(InvalidUndoRecPtr,
											   undo_recptr,
											   uur->uur_dbid,
											   full_xid);

					pending_abort = true;
				}

				next_urecptr = uur->uur_next;
				undoxid = uur->uur_xid;
				epoch = uur->uur_xidepoch;

				UndoRecordRelease(uur);
				uur = NULL;
			}
		}

		/*
		 * We can discard upto this point when one of following conditions is
		 * met: (a) the next transaction is not all-visible. (b) there is no
		 * more log to process. (c) the transaction undo in current log is
		 * finished. (d) there is a pending abort.
		 */
		if ((TransactionIdIsValid(undoxid) &&
			 TransactionIdFollowsOrEquals(undoxid, xmin)) ||
			next_urecptr == InvalidUndoRecPtr ||
			log_complete ||
			UndoRecPtrGetLogNo(next_urecptr) != logno ||
			pending_abort)
		{
			/* Hey, I got some undo log to discard, can not hibernate now. */
			*hibernate = false;

			/*
			 * If the transaction id is smaller than the xmin, it means this
			 * must be the last transaction in this undo log, so we need to
			 * get the last insert point in this undo log and discard till
			 * that point.
			 *
			 * Also, if the transaction has pending abort, stop discarding
			 * further.
			 */
			if (TransactionIdPrecedes(undoxid, xmin) && !pending_abort)
			{
				UndoRecPtr	next_insert = InvalidUndoRecPtr;

				/*
				 * If more undo has been inserted since we checked last, then
				 * we can process that as well.
				 */
				next_insert = UndoLogGetNextInsertPtr(logno, undoxid);
				if (!UndoRecPtrIsValid(next_insert))
					continue;

				undo_recptr = next_insert;
				need_discard = true;
				epoch = 0;
				latest_discardxid = undoxid;
				undoxid = InvalidTransactionId;
			}

			/* Update the shared memory state. */
			LWLockAcquire(&slot->discard_lock, LW_EXCLUSIVE);

			/*
			 * If the slot has been recycling while we were thinking about it,
			 * we have to abandon the operation.
			 */
			if (slot->logno != logno)
			{
				LWLockRelease(&slot->discard_lock);
				return InvalidFullTransactionId;
			}

			/*
			 * If no more pending undo logs then set the oldest transaction to
			 * InvalidTransactionId.
			 */
			if (log_complete)
			{
				slot->oldest_xid = InvalidTransactionId;
				slot->oldest_xidepoch = 0;
			}
			else
			{
				slot->oldest_xid = undoxid;
				slot->oldest_xidepoch = epoch;
			}

			slot->oldest_data = undo_recptr;

			LWLockRelease(&slot->discard_lock);

			if (need_discard)
			{
				LWLockAcquire(&slot->discard_update_lock, LW_EXCLUSIVE);
				UndoLogDiscard(undo_recptr, latest_discardxid);
				LWLockRelease(&slot->discard_update_lock);
			}

			break;
		}

		/*
		 * This transaction is smaller than the xmin so lets jump to the next
		 * transaction.
		 */
		undo_recptr = next_urecptr;
		latest_discardxid = undoxid;

		Assert(uur == NULL);

		need_discard = true;
	} while (true);

	CommitTransactionCommand();

	return FullTransactionIdFromEpochAndXid(epoch, undoxid);
}

/*
 * Discard the undo for all the transactions whose xid is smaller than
 * oldestXmin
 */
void
UndoDiscard(TransactionId oldestXmin, bool *hibernate)
{
	FullTransactionId oldestXidHavingUndo;
	UndoLogSlot *slot = NULL;
	uint32	epoch;

	/*
	 * If all the undo logs are discarded, then oldestXidHavingUndo should be
	 * oldestXmin.  As of now, we don't allow more than 2 billion xids in the
	 * system, so we can rely on the epoch retrieved with GetEpochForXid.
	 */
	epoch = GetEpochForXid(oldestXmin);
	oldestXidHavingUndo = FullTransactionIdFromEpochAndXid(epoch, oldestXmin);

	/*
	 * Iterate through all the active logs and one-by-one try to discard the
	 * transactions that are old enough to matter.
	 *
	 * XXX Ideally we can arrange undo logs so that we can efficiently find
	 * those with oldest_xid < oldestXmin, but for now we'll just scan all of
	 * them.
	 */
	while ((slot = UndoLogNextSlot(slot)))
	{
		FullTransactionId oldest_xid = InvalidFullTransactionId;

		/*
		 * If the log is already discarded, then we are done.  It is important
		 * to first check this to ensure that tablespace containing this log
		 * doesn't get dropped concurrently.
		 */
		LWLockAcquire(&slot->mutex, LW_SHARED);
		/*
		 * We don't have to worry about slot recycling and check the logno
		 * here, since we don't care about the identity of this slot, we're
		 * visiting all of them.
		 */
		if (slot->meta.discard == slot->meta.unlogged.insert)
		{
			LWLockRelease(&slot->mutex);
			continue;
		}
		LWLockRelease(&slot->mutex);

		/* We can't process temporary undo logs. */
		if (slot->meta.persistence == UNDO_TEMP)
			continue;

		/*
		 * If the first xid of the undo log is smaller than the xmin the try
		 * to discard the undo log.
		 */
		if (!TransactionIdIsValid(slot->oldest_xid) ||
			TransactionIdPrecedes(slot->oldest_xid, oldestXmin))
		{
			/* Process the undo log. */
			oldest_xid = UndoDiscardOneLog(slot, oldestXmin, hibernate);
		}

		if (FullTransactionIdIsValid(oldest_xid) &&
			FullTransactionIdPrecedes(oldest_xid, oldestXidHavingUndo))
			oldestXidHavingUndo = oldest_xid;
	}

	/*
	 * Update the oldestFullXidHavingUndo in the shared memory.
	 *
	 * XXX: In future, if multiple workers can perform discard then we may
	 * need to use compare and swap for updating the shared memory value.
	 */
	if (FullTransactionIdIsValid(oldestXidHavingUndo))
		pg_atomic_write_u64(&ProcGlobal->oldestFullXidHavingUndo,
							U64FromFullTransactionId(oldestXidHavingUndo));
}

/*
 * Discard all the logs.  This is particularly required in single user mode
 * where at the commit time we discard all the undo logs.
 */
void
UndoLogDiscardAll(void)
{
	UndoLogSlot *slot = NULL;

	Assert(!IsUnderPostmaster);

	/*
	 * No locks are required for discard, since this called only in single
	 * user mode.
	 */
	while ((slot = UndoLogNextSlot(slot)))
	{
		/* If the log is already discarded, then we are done. */
		if (slot->meta.discard == slot->meta.unlogged.insert)
			continue;

		/*
		 * Process the undo log.
		 */
		UndoLogDiscard(MakeUndoRecPtr(slot->logno, slot->meta.unlogged.insert),
					   InvalidTransactionId);
	}

}

/*
 * Discard the undo logs for temp tables.
 */
void
TempUndoDiscard(UndoLogNumber logno)
{
	UndoLogSlot *slot = UndoLogGetSlot(logno, false);

	/*
	 * Discard the undo log for temp table only. Ensure that there is
	 * something to be discarded there.
	 */
	Assert (slot->meta.persistence == UNDO_TEMP);

	/*
	 * If the log is already discarded, then we are done.  It is important
	 * to first check this to ensure that tablespace containing this log
	 * doesn't get dropped concurrently.
	 */
	LWLockAcquire(&slot->mutex, LW_SHARED);
	if (slot->meta.discard == slot->meta.unlogged.insert)
	{
		LWLockRelease(&slot->mutex);
		return;
	}
	LWLockRelease(&slot->mutex);

	/* Process the undo log. */
	UndoLogDiscard(MakeUndoRecPtr(slot->logno, slot->meta.unlogged.insert),
				   InvalidTransactionId);
}
