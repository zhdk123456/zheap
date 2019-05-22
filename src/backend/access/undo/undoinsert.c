/*-------------------------------------------------------------------------
 *
 * undoinsert.c
 *	  entry points for inserting undo records
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undoinsert.c
 *
 * NOTES:
 * Undo record layout:
 *
 * Undo records are stored in sequential order in the undo log.  Each undo
 * record consists of a variable length header, tuple data, and payload
 * information.  The first undo record of each transaction contains a
 * transaction header that points to the next transaction's start header.
 * This allows us to discard the entire transaction's log at one-shot rather
 * than record-by-record.  The callers are not aware of transaction header,
 * this is entirely maintained and used by undo record layer.   See
 * undorecord.h for detailed information about undo record header.
 *
 * Multiple logs:
 *
 * It is possible that the undo records for a transaction spans multiple undo
 * logs.  We need some special handling while inserting them to ensure that
 * discard and rollbacks can work sanely.
 *
 * When the undo record for a transaction gets inserted in the next log then we
 * add a transaction header for the first record of the transaction in the new
 * log and connect this undo record to the first record of the transaction in
 * the previous log by updating the "uur_next" field.
 *
 * We will also keep a previous undo record pointer to the last undo record of
 * the transaction in the previous log, so that we can find the previous undo
 * record pointer during rollback.
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/subtrans.h"
#include "access/transam.h"
#include "access/undorecord.h"
#include "access/undoinsert.h"
#include "access/undolog_xlog.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"
#include "commands/tablecmds.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"

/* Default prepared undo space */
static PreparedUndoSpace def_prepared_undo[MAX_PREPARED_UNDO];

/*
 * Default undo buffers for prepared undo space and updating the previous
 * transaction's transaction info.
 */
static PreparedUndoBuffer def_prepared_undo_buffers[MAX_UNDO_BUFFERS];

/* Prototypes for static functions. */
static UnpackedUndoRecord *UndoGetOneRecord(UnpackedUndoRecord *urec,
				 UndoRecPtr urp, RelFileNode rnode,
				 UndoPersistence persistence,
				 Buffer *prevbuf);
static void UndoRecordPrepareTransInfo(UndoRecordInsertContext *context,
						   UndoRecPtr urecptr,
						   UndoRecPtr xact_urp);
static int UndoGetBufferSlot(UndoRecordInsertContext *context,
				  RelFileNode rnode, BlockNumber blk,
				  ReadBufferMode rbm);
static uint16 UndoGetPrevRecordLen(UndoRecPtr urp, Buffer input_buffer,
					 UndoPersistence upersistence);

/*
 * Prepare to update the previous transaction's next undo pointer.
 *
 * We want to update the uur_next pointer in the previous transaction's first
 * undo record.  So in prepare phase we will unpack that record and lock the
 * necessary buffera and store the unpacked record in the context.  Later,
 * UndoRecordUpdateTransInfo will overwrite the header of the undo record.
 *
 * xact_urp - undo record pointer of the previous transaction's header
 * urecptr - current transaction's undo record pointer which need to be set in
 *			 the previous transaction's header.
 */
static void
UndoRecordPrepareTransInfo(UndoRecordInsertContext *context, UndoRecPtr urecptr,
						   UndoRecPtr xact_urp)
{
	Buffer		buffer = InvalidBuffer;
	BlockNumber cur_blk;
	Page		page;
	RelFileNode rnode;
	UndoLogSlot *slot;
	UndoPackContext ucontext = {{0}};
	XactUndoRecordInfo *xact_info =
	&context->xact_urec_info[context->nxact_urec_info];
	int			starting_byte;
	int			bufidx;
	int			index = 0;

	/*
	 * The absence of previous transaction's undo indicate that this backend
	 * is preparing its first undo in which case we have nothing to update.
	 */
	if (!UndoRecPtrIsValid(xact_urp))
		return;

	/*
	 * We should only be updating transactions headers of transactions that
	 * are in a log that we are current attached to (because we're writing a
	 * new transaction immediately after it).  Therefore we can access the
	 * immutable persistence property without locking, and further down we
	 * don't have to worry about the slot being recycled before we acquire the
	 * mutex.
	 */
	slot = UndoLogGetSlot(UndoRecPtrGetLogNo(xact_urp), false);
	Assert(InRecovery || AmAttachedToUndoLogSlot(slot));

	/*
	 * Temporary undo logs are discarded on transaction commit so we don't
	 * need to do anything.
	 */
	if (slot->meta.persistence == UNDO_TEMP)
		return;

	/*
	 * Acquire the discard_update_lock before accessing the undo record so
	 * that discard worker can't remove the record while we are in process of
	 * reading it.
	 */
	LWLockAcquire(&slot->discard_update_lock, LW_SHARED);
	/* Check if it is already discarded. */
	if (UndoLogIsDiscarded(xact_urp))
	{
		/* Release lock and return. */
		LWLockRelease(&slot->discard_update_lock);
		return;
	}

	UndoRecPtrAssignRelFileNode(rnode, xact_urp);
	cur_blk = UndoRecPtrGetBlockNum(xact_urp);
	starting_byte = UndoRecPtrGetPageOffset(xact_urp);

	/* Initiate reading the undo record. */
	BeginUnpackUndo(&ucontext);
	while (1)
	{
		bufidx = UndoGetBufferSlot(context, rnode, cur_blk, RBM_NORMAL);
		xact_info->idx_undo_buffers[index++] = bufidx;
		buffer = context->prepared_undo_buffers[bufidx].buf;
		page = BufferGetPage(buffer);

		/* Do actual decoding. */
		UnpackUndoData(&ucontext, page, starting_byte);

		/* We just want to fetch upto transaction header so stop after that. */
		if (ucontext.stage > UNDO_PACK_STAGE_TRANSACTION)
			break;

		/* Could not fetch the complete header so go to the next block. */
		starting_byte = UndoLogBlockHeaderSize;
		cur_blk++;
	}

	FinishUnpackUndo(&ucontext, &xact_info->uur);

	/*
	 * Store undo record pointer of the previous transaction header in xact
	 * info and also update the previous transaction's header with the current
	 * transaction's undo record pointer.  Actual, write will be done by
	 * UndoRecordUpdateTransInfo.
	 */
	xact_info->uur.uur_next = urecptr;
	xact_info->urecptr = xact_urp;
	context->nxact_urec_info++;

	LWLockRelease(&slot->discard_update_lock);
}

/*
 * Update the progress of the undo record in the transaction header.
 */
void
PrepareUpdateUndoActionProgress(UndoRecordInsertContext *context,
								XLogReaderState *xlog_record,
								UndoRecPtr xact_urp, int progress)
{
	Buffer		buffer = InvalidBuffer;
	BlockNumber cur_blk;
	Page		page;
	RelFileNode rnode;
	UndoPersistence persistence;
	UndoPackContext ucontext = {{0}};
	XactUndoRecordInfo *xact_info =
	&context->xact_urec_info[context->nxact_urec_info];
	int			starting_byte;
	int			bufidx;
	int			index = 0;

	Assert(UndoRecPtrIsValid(xact_urp));

	persistence = UndoRecPtrGetPersistence(xact_urp);

	/*
	 * Temporary undo logs are discarded on transaction commit so we don't
	 * need to do anything.
	 */
	if (persistence == UNDO_TEMP)
		return;

	/* It shouldn't be discarded. */
	Assert(!UndoLogIsDiscarded(xact_urp));

	UndoRecPtrAssignRelFileNode(rnode, xact_urp);
	cur_blk = UndoRecPtrGetBlockNum(xact_urp);
	starting_byte = UndoRecPtrGetPageOffset(xact_urp);

	/* Initiate reading the undo record. */
	BeginUnpackUndo(&ucontext);
	while (1)
	{
		bufidx = UndoGetBufferSlot(context, rnode, cur_blk, RBM_NORMAL);
		xact_info->idx_undo_buffers[index++] = bufidx;
		buffer = context->prepared_undo_buffers[bufidx].buf;
		page = BufferGetPage(buffer);

		/* Do actual decoding. */
		UnpackUndoData(&ucontext, page, starting_byte);

		/* We just want to fetch upto transaction header so stop after that. */
		if (ucontext.stage > UNDO_PACK_STAGE_TRANSACTION)
			break;

		/* Could not fetch the complete header so go to the next block. */
		starting_byte = UndoLogBlockHeaderSize;
		cur_blk++;
	}

	FinishUnpackUndo(&ucontext, &xact_info->uur);

	xact_info->urecptr = xact_urp;
	xact_info->uur.uur_progress = progress;
	context->nxact_urec_info++;
}


/*
 * Overwrite the first undo record of the previous transaction to update its
 * next pointer.  This will just insert the already prepared record by
 * UndoRecordPrepareTransInfo.  This must be called under the critical section.
 * This will just overwrite the undo header not the data.
 */
void
UndoRecordUpdateTransInfo(UndoRecordInsertContext *context, int idx)
{
	Page		page = NULL;
	int			starting_byte;
	int			i = 0;
	UndoPackContext ucontext = {{0}};
	XactUndoRecordInfo *xact_info = &context->xact_urec_info[idx];

	/*
	 * We've pinned the undo buffers in UndoRecordPrepareTransInfo, so
	 * it shouldn't be discarded.
	 */
	Assert(!UndoLogIsDiscarded(xact_info->urecptr));

	/*
	 * Update the next transactions start urecptr in the transaction header.
	 */
	starting_byte = UndoRecPtrGetPageOffset(xact_info->urecptr);

	/* Initiate inserting the undo record. */
	BeginInsertUndo(&ucontext, &xact_info->uur);

	/* Main loop for updating the undo record. */
	while (1)
	{
		Buffer		buffer;
		int			buf_idx;

		buf_idx = xact_info->idx_undo_buffers[i];
		buffer = context->prepared_undo_buffers[buf_idx].buf;

		/*
		 * During recovery, there might be some blocks which are already
		 * removed by discard process, so we can just skip inserting into
		 * those blocks.
		 */
		if (!BufferIsValid(buffer))
		{
			Assert(InRecovery);

			/*
			 * Skip actual writing just update the context so that we have
			 * write offset for inserting into next blocks.
			 */
			SkipInsertingUndoData(&ucontext, BLCKSZ - starting_byte);
			if (ucontext.stage > UNDO_PACK_STAGE_TRANSACTION)
				break;
		}
		else
		{
			page = BufferGetPage(buffer);

			/* Overwrite the previously written undo record. */
			InsertUndoData(&ucontext, page, starting_byte);
			if (ucontext.stage > UNDO_PACK_STAGE_TRANSACTION)
			{
				MarkBufferDirty(buffer);
				break;
			}
			MarkBufferDirty(buffer);
		}

		starting_byte = UndoLogBlockHeaderSize;
		i++;

		Assert(idx < MAX_BUFFER_PER_UNDO);
	}
}

/*
 * Find the block number in undo buffer array
 *
 * If it is present then just return its index otherwise search the buffer and
 * insert an entry and lock the buffer in exclusive mode.
 *
 * Undo log insertions are append-only.  If the caller is writing new data
 * that begins exactly at the beginning of a page, then there cannot be any
 * useful data after that point.  In that case RBM_ZERO can be passed in as
 * rbm so that we can skip a useless read of a disk block.  In all other
 * cases, RBM_NORMAL should be passed in, to read the page in if it doesn't
 * happen to be already in the buffer pool.
 */
static int
UndoGetBufferSlot(UndoRecordInsertContext *context,
				  RelFileNode rnode,
				  BlockNumber blk,
				  ReadBufferMode rbm)
{
	int			i;
	Buffer		buffer;
	XLogRedoAction action = BLK_NEEDS_REDO;
	PreparedUndoBuffer *prepared_buffer;
	UndoPersistence persistence = context->alloc_context.persistence;

	/* Don't do anything, if we already have a buffer pinned for the block. */
	for (i = 0; i < context->nprepared_undo_buffer; i++)
	{
		prepared_buffer = &context->prepared_undo_buffers[i];

		/*
		 * It's not enough to just compare the block number because the
		 * undo_buffer might holds the undo from different undo logs (e.g when
		 * previous transaction start header is in previous undo log) so
		 * compare (logno + blkno).
		 */
		if ((blk == prepared_buffer->blk) &&
			(prepared_buffer->logno == rnode.relNode))
		{
			/* caller must hold exclusive lock on buffer */
			Assert(BufferIsLocal(prepared_buffer->buf) ||
				   LWLockHeldByMeInMode(BufferDescriptorGetContentLock(
																	   GetBufferDescriptor(prepared_buffer->buf - 1)),
										LW_EXCLUSIVE));
			return i;
		}
	}

	/*
	 * We did not find the block so allocate the buffer and insert into the
	 * undo buffer array.
	 */
	if (InRecovery)
		action = XLogReadBufferForRedoBlock(context->alloc_context.xlog_record,
											SMGR_UNDO,
											rnode,
											UndoLogForkNum,
											blk,
											rbm,
											false,
											&buffer);
	else
	{
		buffer = ReadBufferWithoutRelcache(SMGR_UNDO,
										   rnode,
										   UndoLogForkNum,
										   blk,
										   rbm,
										   NULL,
										   RelPersistenceForUndoPersistence(persistence));

		/* Lock the buffer */
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	}

	prepared_buffer =
		&context->prepared_undo_buffers[context->nprepared_undo_buffer];

	if (action == BLK_NOTFOUND)
	{
		Assert(InRecovery);

		prepared_buffer->buf = InvalidBuffer;
		prepared_buffer->blk = InvalidBlockNumber;
	}
	else
	{
		prepared_buffer->buf = buffer;
		prepared_buffer->blk = blk;
		prepared_buffer->logno = rnode.relNode;
		prepared_buffer->zero = rbm == RBM_ZERO;
	}

	context->nprepared_undo_buffer++;

	return i;
}

/*
 * This function must be called before all the undo records which are going to
 * get inserted under a single WAL record.
 */
void
BeginUndoRecordInsert(UndoRecordInsertContext *context,
					  UndoPersistence persistence,
					  int max_prepared,
					  XLogReaderState *xlog_record)
{
	/* Initialize undo log context. */
	UndoLogBeginInsert(&context->alloc_context, persistence, xlog_record);

	/* Initialize undo insert context. */
	context->max_prepared_undo = max_prepared;
	context->nprepared_undo = 0;
	context->nprepared_undo_buffer = 0;
	context->nxact_urec_info = 0;

	/*
	 * If max prepared count within the default number than point them to
	 * default static memory.  Otherwise, allocate separate memory for
	 * prepared undo and undo buffer information.
	 */
	if (max_prepared <= MAX_PREPARED_UNDO)
	{
		context->prepared_undo = def_prepared_undo;
		context->prepared_undo_buffers = def_prepared_undo_buffers;
	}
	else
	{
		context->prepared_undo = (PreparedUndoSpace *) palloc(max_prepared *
															  sizeof(PreparedUndoSpace));
		context->prepared_undo_buffers = palloc((max_prepared + 1) *
												MAX_BUFFER_PER_UNDO * sizeof(PreparedUndoBuffer));
	}
}

/*
 * Call PrepareUndoInsert to tell the undo subsystem about the undo record you
 * intended to insert.  Upon return, the necessary undo buffers are pinned and
 * locked.
 *
 * This should be done before any critical section is established, since it
 * can fail.
 *
 * In recovery, 'fxid' refers to the full transaction id stored in WAL,
 * otherwise, it refers to the top full transaction id.
 */
UndoRecPtr
PrepareUndoInsert(UndoRecordInsertContext *context,
				  UnpackedUndoRecord *urec,
				  FullTransactionId fxid)
{
	UndoRecordSize size;
	UndoRecPtr	urecptr;
	RelFileNode rnode;
	UndoRecordSize cur_size = 0;
	BlockNumber cur_blk;
	FullTransactionId txid;
	int			starting_byte;
	int			index = 0;
	int			bufidx;
	ReadBufferMode rbm;
	bool		need_xact_header;
	UndoRecPtr	last_xact_start;
	UndoRecPtr	prevlog_xact_start = InvalidUndoRecPtr;
	UndoRecPtr	prevlog_insert_urp = InvalidUndoRecPtr;
	UndoRecPtr	prevlogurp = InvalidUndoRecPtr;
	PreparedUndoSpace *prepared_undo;

	/* Already reached maximum prepared limit. */
	if (context->nprepared_undo == context->max_prepared_undo)
		elog(ERROR, "already reached the maximum prepared limit");

	if (!FullTransactionIdIsValid(fxid))
	{
		/* During recovery, we must have a valid transaction id. */
		Assert(!InRecovery);
		txid = GetTopFullTransactionId();
	}
	else
	{
		/*
		 * Assign the top transaction id because undo log only stores mapping
		 * for the top most transactions.
		 */
		Assert(InRecovery ||
			   FullTransactionIdEquals(fxid, GetTopFullTransactionId()));
		txid = fxid;
	}

	/*
	 * We don't yet know if this record needs a transaction header (ie is the
	 * first undo record for a given transaction in a given undo log), because
	 * you can only find out by allocating.  We'll resolve this circularity by
	 * allocating enough space for a transaction header.  We'll only advance
	 * by as many bytes as we turn out to need.
	 */
	urec->uur_next = InvalidUndoRecPtr;
	UndoRecordSetInfo(urec);
	urec->uur_info |= UREC_INFO_TRANSACTION;
	size = UndoRecordExpectedSize(urec);

	/* Allocate space for the record. */
	if (InRecovery)
	{
		/*
		 * We'll figure out where the space needs to be allocated by
		 * inspecting the xlog_record.
		 */
		Assert(context->alloc_context.persistence == UNDO_PERMANENT);
		urecptr = UndoLogAllocateInRecovery(&context->alloc_context,
											XidFromFullTransactionId(txid),
											size,
											&need_xact_header,
											&last_xact_start,
											&prevlog_xact_start,
											&prevlogurp);
	}
	else
	{
		urecptr = UndoLogAllocate(&context->alloc_context,
								  size,
								  &need_xact_header, &last_xact_start,
								  &prevlog_xact_start, &prevlog_insert_urp);
		if (UndoRecPtrIsValid(prevlog_xact_start))
		{
			uint16		prevlen;

			Assert(UndoRecPtrIsValid(prevlog_insert_urp));
			/* Fetch length of the last undo record of the previous log. */
			prevlen = UndoGetPrevRecordLen(prevlog_insert_urp, InvalidBuffer,
										   context->alloc_context.persistence);
			/* Compute the last record's undo record pointer. */
			prevlogurp =
				MakeUndoRecPtr(UndoRecPtrGetLogNo(prevlog_insert_urp),
							   (UndoRecPtrGetOffset(prevlog_insert_urp) - prevlen));

			/*
			 * Undo log switched so set prevlog info in current undo log.
			 *
			 * XXX can we do this directly in UndoLogAllocate ? but for that
			 * the UndoLogAllocate might need to read the length of the last
			 * undo record from the previous undo log but for that it might
			 * use callback?
			 */
			UndoLogSwitchSetPrevLogInfo(UndoRecPtrGetLogNo(urecptr),
										prevlog_xact_start, prevlogurp);
		}
	}

	urec->uur_prevurp = prevlogurp;

	/* Initialize transaction related members. */
	urec->uur_progress = 0;
	if (need_xact_header)
	{
		/*
		 * TODO: Should we set urec->uur_dbid automatically?  How can you do
		 * that, in recovery -- can we extract it from xlog_record?  For now
		 * assume that the caller set it explicitly.
		 */
	}
	else
	{
		urec->uur_dbid = 0;

		/* We don't need a transaction header after all. */
		urec->uur_info &= ~UREC_INFO_TRANSACTION;
		size = UndoRecordExpectedSize(urec);
	}

	/*
	 * If there is a physically preceding transaction in this undo log, and we
	 * are writing the first record for this transaction that is in this undo
	 * log (not necessarily the first ever for the transaction, because we
	 * could have switched logs), then we need to update the size of the
	 * preceding transaction.
	 */
	if (need_xact_header &&
		UndoRecPtrGetOffset(urecptr) > UndoLogBlockHeaderSize)
		UndoRecordPrepareTransInfo(context, urecptr, last_xact_start);

	/*
	 * If prevlog_xact_start is valid that means the transaction's undo are
	 * split across the undo logs.  So we need to  update our own transaction
	 * header in the previous log as well.
	 */
	if (UndoRecPtrIsValid(prevlog_xact_start))
	{
		Assert(UndoRecPtrIsValid(prevlogurp));
		UndoRecordPrepareTransInfo(context, urecptr, prevlog_xact_start);
	}

	cur_blk = UndoRecPtrGetBlockNum(urecptr);
	UndoRecPtrAssignRelFileNode(rnode, urecptr);
	starting_byte = UndoRecPtrGetPageOffset(urecptr);

	/*
	 * If we happen to be writing the very first byte into this page, then
	 * there is no need to read from disk.
	 */
	if (starting_byte == UndoLogBlockHeaderSize)
		rbm = RBM_ZERO;
	else
		rbm = RBM_NORMAL;

	prepared_undo = &context->prepared_undo[context->nprepared_undo];

	do
	{
		bufidx = UndoGetBufferSlot(context, rnode, cur_blk, rbm);
		if (cur_size == 0)
			cur_size = BLCKSZ - starting_byte;
		else
			cur_size += BLCKSZ - UndoLogBlockHeaderSize;

		/* undo record can't use buffers more than MAX_BUFFER_PER_UNDO. */
		Assert(index < MAX_BUFFER_PER_UNDO);

		/* Keep the track of the buffers we have pinned and locked. */
		prepared_undo->undo_buffer_idx[index++] = bufidx;

		/*
		 * If we need more pages they'll be all new so we can definitely skip
		 * reading from disk.
		 */
		rbm = RBM_ZERO;
		cur_blk++;
	} while (cur_size < size);

	UndoLogAdvance(&context->alloc_context, size);

	/*
	 * Save prepared undo record information into the context which will be
	 * used by InsertPreparedUndo to insert the undo record.
	 */
	prepared_undo->urec = urec;
	prepared_undo->urp = urecptr;
	prepared_undo->size = size;

	context->nprepared_undo++;

	return urecptr;
}

/*
 * Insert a previously-prepared undo records.
 *
 * This function will write the actual undo record into the buffers which are
 * already pinned and locked in PreparedUndoInsert, and mark them dirty.  This
 * step should be performed inside a critical section.
 */
void
InsertPreparedUndo(UndoRecordInsertContext *context)
{
	UndoPackContext ucontext = {{0}};
	PreparedUndoSpace *prepared_undo;
	Page		page = NULL;
	int			starting_byte;
	int			bufidx = 0;
	int			idx;
	int			i;

	/* There must be atleast one prepared undo record. */
	Assert(context->nprepared_undo > 0);

	/*
	 * This must be called under a critical section or we must be in recovery.
	 */
	Assert(InRecovery || CritSectionCount > 0);

	for (idx = 0; idx < context->nprepared_undo; idx++)
	{
		prepared_undo = &context->prepared_undo[idx];

		Assert(prepared_undo->size ==
			   UndoRecordExpectedSize(prepared_undo->urec));

		bufidx = 0;

		/*
		 * Compute starting offset of the page where to start inserting undo
		 * record.
		 */
		starting_byte = UndoRecPtrGetPageOffset(prepared_undo->urp);

		/* Initiate inserting the undo record. */
		BeginInsertUndo(&ucontext, prepared_undo->urec);

		/* Main loop for writing the undo record. */
		do
		{
			Buffer		buffer;

			buffer = context->prepared_undo_buffers[
													prepared_undo->undo_buffer_idx[bufidx]].buf;

			/*
			 * During recovery, there might be some blocks which are already
			 * deleted due to some discard command so we can just skip
			 * inserting into those blocks.
			 */
			if (!BufferIsValid(buffer))
			{
				Assert(InRecovery);

				/*
				 * Skip actual writing just update the context so that we have
				 * write offset for inserting into next blocks.
				 */
				SkipInsertingUndoData(&ucontext, BLCKSZ - starting_byte);
				if (ucontext.stage == UNDO_PACK_STAGE_DONE)
					break;
			}
			else
			{
				page = BufferGetPage(buffer);

				/*
				 * Initialize the page whenever we try to write the first
				 * record in page.  We start writing immediately after the
				 * block header.
				 */
				if (starting_byte == UndoLogBlockHeaderSize)
					PageInit(page, BLCKSZ, 0);

				/*
				 * Try to insert the record into the current page. If it
				 * doesn't succeed then recall the routine with the next page.
				 */
				InsertUndoData(&ucontext, page, starting_byte);
				if (ucontext.stage == UNDO_PACK_STAGE_DONE)
				{
					MarkBufferDirty(buffer);
					break;
				}
				MarkBufferDirty(buffer);
			}

			/* Insert remaining record in next block. */
			starting_byte = UndoLogBlockHeaderSize;
			bufidx++;

			/* undo record can't use buffers more than MAX_BUFFER_PER_UNDO. */
			Assert(bufidx < MAX_BUFFER_PER_UNDO);
		} while (true);

		/*
		 * Set the current undo location for a transaction.  This is required
		 * to perform rollback during abort of transaction.
		 */
		SetCurrentUndoLocation(prepared_undo->urp,
							   context->alloc_context.persistence);

		/* Advance the insert pointer past this record. */
		UndoLogAdvanceFinal(prepared_undo->urp, prepared_undo->size);
	}

	/* Update previously prepared transaction headers. */
	for (i = 0; i < context->nxact_urec_info; i++)
		UndoRecordUpdateTransInfo(context, i);
}

/*
 * Release all the memory and buffer pins hold for inserting the undo records.
 */
void
FinishUndoRecordInsert(UndoRecordInsertContext *context)
{
	int			i;

	/* Release buffer pins and lock. */
	for (i = 0; i < context->nprepared_undo_buffer; i++)
	{
		if (BufferIsValid(context->prepared_undo_buffers[i].buf))
			UnlockReleaseBuffer(context->prepared_undo_buffers[i].buf);
	}

	/*
	 * If we have allocated memory for prepare undo and prepared buffers then
	 * release the memory.
	 */
	if (context->max_prepared_undo > MAX_PREPARED_UNDO)
	{
		pfree(context->prepared_undo_buffers);
		pfree(context->prepared_undo);
	}
}

/*
 * Helper function for UndoFetchRecord and UndoBulkFetchRecord
 *
 * curbuf - If an input buffer is valid then this function will not release the
 * pin on that buffer.  If the buffer is not valid then it will assign curbuf
 * with the first buffer of the current undo record and also it will keep the
 * pin and lock on that buffer.
 */
static UnpackedUndoRecord *
UndoGetOneRecord(UnpackedUndoRecord *urec, UndoRecPtr urp, RelFileNode rnode,
				 UndoPersistence persistence, Buffer *curbuf)
{
	Page		page;
	int			starting_byte = UndoRecPtrGetPageOffset(urp);
	BlockNumber cur_blk;
	UndoPackContext ucontext = {{0}};
	Buffer		buffer = *curbuf;

	cur_blk = UndoRecPtrGetBlockNum(urp);

	/* Initiate unpacking one undo record. */
	BeginUnpackUndo(&ucontext);

	while (true)
	{
		/* If we already have a buffer then no need to allocate a new one. */
		if (!BufferIsValid(buffer))
		{
			buffer = ReadBufferWithoutRelcache(SMGR_UNDO,
											   rnode, UndoLogForkNum, cur_blk,
											   RBM_NORMAL, NULL,
											   RelPersistenceForUndoPersistence(persistence));

			/*
			 * Remember the first buffer where this undo started as next undo
			 * record what we fetch might fall on the same buffer.
			 */
			if (!BufferIsValid(*curbuf))
				*curbuf = buffer;

			/* Acquire shared lock on the buffer before reading undo from it. */
			LockBuffer(buffer, BUFFER_LOCK_SHARE);
		}

		page = BufferGetPage(buffer);

		UnpackUndoData(&ucontext, page, starting_byte);

		/*
		 * We are done if we have reached to the done stage otherwise move to
		 * next block and continue reading from there.
		 */
		if (ucontext.stage == UNDO_PACK_STAGE_DONE)
		{
			if (buffer != *curbuf)
				UnlockReleaseBuffer(buffer);
			break;
		}

		/*
		 * The record spans more than a page so we would have copied it (see
		 * UnpackUndoRecord).  In such cases, we can release the buffer.
		 */
		if (buffer != *curbuf)
			UnlockReleaseBuffer(buffer);
		buffer = InvalidBuffer;

		/* Go to next block. */
		cur_blk++;
		starting_byte = UndoLogBlockHeaderSize;
	}

	/* Final step of unpacking. */
	FinishUnpackUndo(&ucontext, urec);

	return urec;
}

/*
 * Helper function for UndoFetchRecord to reset the unpacked undo record.
 */
static void
ResetUndoRecord(UnpackedUndoRecord *urec, UndoRecPtr urp, RelFileNode *rnode,
				RelFileNode *prevrec_rnode, Buffer *buffer)
{
	/*
	 * If we have a valid buffer pinned then just ensure that we want to find
	 * the next tuple from the same block.  Otherwise release the buffer and
	 * set it invalid
	 */
	if (BufferIsValid(*buffer))
	{
		/*
		 * Undo buffer will be changed if the next undo record belongs to a
		 * different block or undo log.
		 */
		if ((UndoRecPtrGetBlockNum(urp) !=
			 BufferGetBlockNumber(*buffer)) ||
			(prevrec_rnode->relNode != rnode->relNode))
		{
			ReleaseBuffer(*buffer);
			*buffer = InvalidBuffer;
		}
	}

	if (urec->uur_payload.data)
		pfree(urec->uur_payload.data);
	if (urec->uur_tuple.data)
		pfree(urec->uur_tuple.data);

	/* Reset the urec before fetching the tuple */
	urec->uur_tuple.data = NULL;
	urec->uur_tuple.len = 0;
	urec->uur_payload.data = NULL;
	urec->uur_payload.len = 0;
}

/*
 * Fetch undo record for given urp
 *
 * Fetch the next undo record for given blkno, offset and transaction id (if
 * valid).  The same tuple can be modified by multiple transactions, so during
 * undo chain traversal sometimes we need to distinguish based on transaction
 * id.  Callers that don't have any such requirement can pass
 * InvalidTransactionId.
 *
 * Start the search from urp.  Caller need to call UndoRecordRelease to release the
 * resources allocated by this function.
 *
 * urec_ptr_out is undo record pointer of the qualified undo record if valid
 * pointer is passed.
 *
 * callback function decides whether particular undo record satisfies the
 * condition of caller.
 *
 * Returns the required undo record if found, otherwise, return NULL which
 * means either the record is already discarded or there is no such record
 * in the undo chain.
 */
UnpackedUndoRecord *
UndoFetchRecord(UndoRecPtr urp, BlockNumber blkno, OffsetNumber offset,
				TransactionId xid, UndoRecPtr *urec_ptr_out,
				SatisfyUndoRecordCallback callback)
{
	RelFileNode rnode,
				prevrec_rnode = {0};
	UnpackedUndoRecord *urec = NULL;
	Buffer		buffer = InvalidBuffer;
	int			logno;

	if (urec_ptr_out)
		*urec_ptr_out = InvalidUndoRecPtr;

	/*
	 * Allocate memory for holding the undo record, caller should be
	 * responsible for freeing this memory.
	 */
	urec = palloc0(sizeof(UnpackedUndoRecord));
	UndoRecPtrAssignRelFileNode(rnode, urp);

	/* Find the undo record pointer we are interested in. */
	while (true)
	{
		UndoLogSlot *slot;

		logno = UndoRecPtrGetLogNo(urp);
		slot = UndoLogGetSlot(logno, true);
		if (slot == NULL)
		{
			/*
			 * The undo log number is unknown.  Presumably it has been
			 * entirely discarded.
			 */
			urp = InvalidUndoRecPtr;
			break;
		}

		/*
		 * Prevent UndoDiscardOneLog() from discarding data while we try to
		 * read it.  Usually we would acquire log->mutex to read log->meta
		 * members, but in this case we know that discard can't move without
		 * also holding log->discard_lock.
		 *
		 * In Hot Standby mode log->oldest_data is never initialized because
		 * it's get updated by undo discard worker whereas in HotStandby undo
		 * logs are getting discarded using discard WAL.  So in HotStandby we
		 * can directly check whether the undo record pointer is discarded or
		 * not.  But, we can not do same for normal case because discard
		 * worker can concurrently discard the undo logs.
		 *
		 * XXX We can avoid this check by always initializing log->oldest_data
		 * in HotStandby mode as well whenever we apply discard WAL.  But, for
		 * doing that we need to acquire discard lock just for setting this
		 * variable?
		 */
		if (InHotStandby)
		{
			if (UndoLogIsDiscarded(urp))
			{
				urp = InvalidUndoRecPtr;
				break;
			}
		}
		else
		{
			LWLockAcquire(&slot->discard_lock, LW_SHARED);
			if (slot->logno != logno || urp < slot->oldest_data)
			{
				/*
				 * The slot has been recycled because the undo log was
				 * entirely discarded, or the pointer is before the oldest
				 * data.
				 */
				LWLockRelease(&slot->discard_lock);
				urp = InvalidUndoRecPtr;
				break;
			}
		}

		/* Fetch the current undo record. */
		UndoGetOneRecord(urec, urp, rnode, slot->meta.persistence, &buffer);

		/* Release the discard lock after fetching the record. */
		LWLockRelease(&slot->discard_lock);

		if (blkno == InvalidBlockNumber)
			break;

		/* Check whether the undo record satisfies conditions */
		if (callback(urec, blkno, offset, xid))
			break;

		urp = urec->uur_blkprev;
		prevrec_rnode = rnode;

		/* Get rnode for the current undo record pointer. */
		UndoRecPtrAssignRelFileNode(rnode, urp);

		/* Reset the current undo record before fetching the next. */
		ResetUndoRecord(urec, urp, &rnode, &prevrec_rnode, &buffer);
	}

	/*
	 * If we have not found any valid undo record that means it might have
	 * already got discarded so release the memory we allocated for unpacked
	 * undo record and set urec to NULL.
	 */
	if (!UndoRecPtrIsValid(urp))
	{
		pfree(urec);
		urec = NULL;
	}
	else if (urec_ptr_out != NULL)
		*urec_ptr_out = urp;

	/* Release the last buffer. */
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	return urec;
}

/*
 * Prefetch undo pages, if prefetch_pages are behind prefetch_target
 */
static void
PrefetchUndoPages(RelFileNode rnode, int prefetch_target, int *prefetch_pages,
				  BlockNumber to_blkno, BlockNumber from_blkno,
				  char persistence)
{
	int			nprefetch;
	BlockNumber startblock;
	BlockNumber lastprefetched;

	/* Calculate last prefetched page in the previous iteration. */
	lastprefetched = from_blkno - *prefetch_pages;

	/* We have already prefetched all the pages of the transaction's undo. */
	if (lastprefetched <= to_blkno)
		return;

	/* Calculate number of blocks to be prefetched. */
	nprefetch =
		Min(prefetch_target - *prefetch_pages, lastprefetched - to_blkno);

	/* Where to start prefetch. */
	startblock = lastprefetched - nprefetch;

	while (nprefetch--)
	{
		PrefetchBufferWithoutRelcache(SMGR_UNDO, rnode, MAIN_FORKNUM,
									  startblock++,
									  RelPersistenceForUndoPersistence(persistence));
		(*prefetch_pages)++;
	}
}

/*
 * Read undo records of the transaction in bulk
 *
 * Read undo records between from_urecptr and to_urecptr until we exhaust the
 * the memory size specified by undo_apply_size.  If we could not read all the
 * records till to_urecptr then the caller should consume current set of records
 * and call this function again.
 *
 * from_urecptr		- Where to start fetching the undo records.  If we can not
 *					  read all the records because of memory limit then this
 *					  will be set to the previous undo record pointer from where
 *					  we need to start fetching on next call. Otherwise it will
 *					  be set to InvalidUndoRecPtr.
 * to_urecptr		- Last undo record pointer to be fetched.
 * undo_apply_size	- Memory segment limit to collect undo records.
 * nrecords			- Number of undo records read.
 * one_page			- Caller is applying undo only for one block not for
 *					  complete transaction.  If this is set true then instead
 *					  of following transaction undo chain using prevlen we will
 *					  follow the block prev chain of the block so that we can
 *					  avoid reading many unnecessary undo records of the
 *					  transaction.
 */
UndoRecInfo *
UndoBulkFetchRecord(UndoRecPtr *from_urecptr, UndoRecPtr to_urecptr,
					int undo_apply_size, int *nrecords, bool one_page)
{
	RelFileNode rnode;
	UndoRecPtr	urecptr,
				prev_urec_ptr;
	BlockNumber blkno;
	BlockNumber to_blkno;
	Buffer		buffer = InvalidBuffer;
	UnpackedUndoRecord *uur = NULL;
	UndoRecInfo *urp_array;
	int			urp_array_size = 1024;
	int			urp_index = 0;
	int			prefetch_target = 0;
	int			prefetch_pages = 0;
	Size		total_size = 0;
	TransactionId xid = InvalidTransactionId;

	/*
	 * In one_page mode we are fetching undo only for one page instead of
	 * fetching all the undo of the transaction.  Basically, we are fetching
	 * interleaved undo records.  So it does not make sense to do any prefetch
	 * in that case.
	 */
	if (!one_page)
		prefetch_target = target_prefetch_pages;

	/*
	 * Allocate initial memory to hold the undo record info, we can expand it
	 * if needed.
	 */
	urp_array = (UndoRecInfo *) palloc(sizeof(UndoRecInfo) * urp_array_size);
	urecptr = *from_urecptr;

	prev_urec_ptr = InvalidUndoRecPtr;
	*from_urecptr = InvalidUndoRecPtr;

	/* Read undo chain backward until we reach to the first undo record.  */
	while (1)
	{
		BlockNumber from_blkno;
		UndoLogSlot *slot;
		UndoPersistence persistence;
		int			size;
		int			logno;

		logno = UndoRecPtrGetLogNo(urecptr);
		slot = UndoLogGetSlot(logno, true);
		if (slot == NULL)
		{
			if (BufferIsValid(buffer))
				UnlockReleaseBuffer(buffer);
			return NULL;
		}
		persistence = slot->meta.persistence;

		UndoRecPtrAssignRelFileNode(rnode, urecptr);
		to_blkno = UndoRecPtrGetBlockNum(to_urecptr);
		from_blkno = UndoRecPtrGetBlockNum(urecptr);

		/* Allocate memory for next undo record. */
		uur = palloc0(sizeof(UnpackedUndoRecord));

		/*
		 * If next undo record pointer to be fetched is not on the same block
		 * then release the old buffer and reduce the prefetch_pages count by
		 * one as we have consumed one page. Otherwise, just set the old
		 * buffer into the new undo record so that UndoGetOneRecord don't read
		 * the buffer again.
		 */
		blkno = UndoRecPtrGetBlockNum(urecptr);
		if (!UndoRecPtrIsValid(prev_urec_ptr) ||
			UndoRecPtrGetLogNo(prev_urec_ptr) != logno ||
			UndoRecPtrGetBlockNum(prev_urec_ptr) != blkno)
		{
			/* Release the previous buffer */
			if (BufferIsValid(buffer))
			{
				UnlockReleaseBuffer(buffer);
				buffer = InvalidBuffer;
			}

			if (prefetch_pages > 0)
				prefetch_pages--;
		}

		/*
		 * If prefetch_pages are half of the prefetch_target then it's time to
		 * prefetch again.
		 */
		if (prefetch_pages < prefetch_target / 2)
			PrefetchUndoPages(rnode, prefetch_target, &prefetch_pages, to_blkno,
							  from_blkno, persistence);

		/*
		 * In one_page mode it's possible that the undo of the transaction
		 * might have been applied by worker and undo got discarded. Prevent
		 * discard worker from discarding undo data while we are reading it.
		 * See detail comment in UndoFetchRecord.  In normal mode we are
		 * holding transaction undo action lock so it can not be discarded.
		 */
		if (one_page)
		{
			/* Refer comments in UndoFetchRecord. */
			if (InHotStandby)
			{
				if (UndoLogIsDiscarded(urecptr))
					break;
			}
			else
			{
				LWLockAcquire(&slot->discard_lock, LW_SHARED);
				if (slot->logno != logno || urecptr < slot->oldest_data)
				{
					/*
					 * The undo log slot has been recycled because it was
					 * entirely discarded, or the data has been discarded
					 * already.
					 */
					LWLockRelease(&slot->discard_lock);
					break;
				}
			}

			/* Read the undo record. */
			UndoGetOneRecord(uur, urecptr, rnode, persistence, &buffer);

			/* Release the discard lock after fetching the record. */
			LWLockRelease(&slot->discard_lock);
		}
		else
			UndoGetOneRecord(uur, urecptr, rnode, persistence, &buffer);

		/*
		 * As soon as the transaction id is changed we can stop fetching the
		 * undo record.  Ideally, to_urecptr should control this but while
		 * reading undo only for a page we don't know what is the end undo
		 * record pointer for the transaction.
		 */
		if (one_page)
		{
			if (!TransactionIdIsValid(xid))
				xid = uur->uur_xid;
			else if (xid != uur->uur_xid)
				break;
		}

		/* Remember the previous undo record pointer. */
		prev_urec_ptr = urecptr;

		/*
		 * Calculate the previous undo record pointer of the transaction.  If
		 * we are reading undo only for a page then follow the blkprev chain
		 * of the page.  Otherwise, calculate the previous undo record pointer
		 * using transaction's current undo record pointer and the prevlen.
		 */
		if (one_page)
			urecptr = uur->uur_blkprev;
		else if (prev_urec_ptr == to_urecptr ||
				 uur->uur_info & UREC_INFO_TRANSACTION)
			urecptr = InvalidUndoRecPtr;
		else if (UndoRecPtrIsValid(uur->uur_prevurp))
			urecptr = uur->uur_prevurp;
		else
			urecptr = UndoGetPrevUndoRecptr(prev_urec_ptr, buffer, persistence);

		/* We have consumed all elements of the urp_array so expand its size. */
		if (urp_index >= urp_array_size)
		{
			urp_array_size *= 2;
			urp_array =
				repalloc(urp_array, sizeof(UndoRecInfo) * urp_array_size);
		}

		/* Add entry in the urp_array */
		urp_array[urp_index].index = urp_index;
		urp_array[urp_index].urp = prev_urec_ptr;
		urp_array[urp_index].uur = uur;
		urp_index++;

		/* We have fetched all the undo records for the transaction. */
		if (!UndoRecPtrIsValid(urecptr) || (prev_urec_ptr == to_urecptr))
			break;

		/*
		 * Including current record, if we have crossed the memory limit then
		 * stop processing more records.  Remember to set the from_urecptr so
		 * that on next call we can resume fetching undo records where we left
		 * it.
		 */
		size = UnpackedUndoRecordSize(uur);
		total_size += size;

		if (total_size >= undo_apply_size)
		{
			*from_urecptr = urecptr;
			break;
		}
	}

	/* Release the last buffer. */
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	*nrecords = urp_index;

	return urp_array;
}

/*
 * Release the resources allocated by UndoFetchRecord.
 */
void
UndoRecordRelease(UnpackedUndoRecord *urec)
{
	if (urec->uur_payload.data)
		pfree(urec->uur_payload.data);
	if (urec->uur_tuple.data)
		pfree(urec->uur_tuple.data);

	pfree(urec);
}

/*
 * Register the undo buffers.
 */
void
RegisterUndoLogBuffers(UndoRecordInsertContext *context, uint8 first_block_id)
{
	int			idx;
	int			flags;

	for (idx = 0; idx < context->nprepared_undo_buffer; idx++)
	{
		flags = context->prepared_undo_buffers[idx].zero
			? REGBUF_KEEP_DATA_AFTER_CP | REGBUF_WILL_INIT
			: REGBUF_KEEP_DATA_AFTER_CP;
		XLogRegisterBuffer(first_block_id + idx,
						   context->prepared_undo_buffers[idx].buf, flags);
		UndoLogRegister(&context->alloc_context, first_block_id + idx,
						context->prepared_undo_buffers[idx].logno);
	}
}

/*
 * Set LSN on undo page.
*/
void
UndoLogBuffersSetLSN(UndoRecordInsertContext *context, XLogRecPtr recptr)
{
	int			idx;

	for (idx = 0; idx < context->nprepared_undo_buffer; idx++)
		PageSetLSN(BufferGetPage(context->prepared_undo_buffers[idx].buf),
				   recptr);
}

/*
 * Read length of the previous undo record.
 *
 * This function will take an undo record pointer as an input and read the
 * length of the previous undo record which is stored at the end of the previous
 * undo record.  If the undo record is split then this will add the undo block
 * header size in the total length.
 */
static uint16
UndoGetPrevRecordLen(UndoRecPtr urp, Buffer input_buffer,
					 UndoPersistence upersistence)
{
	UndoLogOffset page_offset = UndoRecPtrGetPageOffset(urp);
	BlockNumber cur_blk = UndoRecPtrGetBlockNum(urp);
	Buffer		buffer = input_buffer;
	Page		page = NULL;
	char	   *pagedata = NULL;
	char		prevlen[2];
	RelFileNode rnode;
	int			byte_to_read = sizeof(uint16);
	char		persistence;
	uint16		prev_rec_len = 0;

	/* Get relfilenode. */
	UndoRecPtrAssignRelFileNode(rnode, urp);
	persistence = RelPersistenceForUndoPersistence(upersistence);

	if (BufferIsValid(buffer))
	{
		page = BufferGetPage(buffer);
		pagedata = (char *) page;
	}

	/*
	 * Length if the previous undo record is store at the end of that record
	 * so just fetch last 2 bytes.
	 */
	while (byte_to_read > 0)
	{
		/* Read buffer if the current buffer is not valid. */
		if (!BufferIsValid(buffer))
		{
			buffer = ReadBufferWithoutRelcache(SMGR_UNDO, rnode, UndoLogForkNum,
											   cur_blk, RBM_NORMAL, NULL,
											   persistence);

			LockBuffer(buffer, BUFFER_LOCK_SHARE);

			page = BufferGetPage(buffer);
			pagedata = (char *) page;
		}

		page_offset -= 1;

		/*
		 * Read current prevlen byte from current block if page_offset hasn't
		 * reach to undo block header.  Otherwise, go to the previous block
		 * and continue reading from there.
		 */
		if (page_offset >= UndoLogBlockHeaderSize)
		{
			prevlen[byte_to_read - 1] = pagedata[page_offset];
			byte_to_read -= 1;
		}
		else
		{
			/*
			 * Release the current buffer if it is not provide by the caller.
			 */
			if (input_buffer != buffer)
				UnlockReleaseBuffer(buffer);

			/*
			 * Could not read complete prevlen from the current block so go to
			 * the previous block and start reading from end of the block.
			 */
			cur_blk -= 1;
			page_offset = BLCKSZ;

			/*
			 * Reset buffer so that we can read it again for the previous
			 * block.
			 */
			buffer = InvalidBuffer;
		}
	}

	prev_rec_len = *(uint16 *) (prevlen);

	/*
	 * If previous undo record is not completely stored in this page then add
	 * UndoLogBlockHeaderSize in total length so that the call can use this
	 * length to compute the undo record pointer of the previous undo record.
	 */
	if (UndoRecPtrGetPageOffset(urp) - UndoLogBlockHeaderSize < prev_rec_len)
		prev_rec_len += UndoLogBlockHeaderSize;

	/* Release the buffer if we have locally read it. */
	if (input_buffer != buffer)
		UnlockReleaseBuffer(buffer);

	return prev_rec_len;
}

/*
 * Calculate the previous undo record pointer for the transaction.
 *
 * This will take current undo record pointer of the transaction as an input
 * and calculate the previous undo record pointer of the transaction.
 */
UndoRecPtr
UndoGetPrevUndoRecptr(UndoRecPtr urp, Buffer buffer,
					  UndoPersistence upersistence)
{
	UndoLogNumber logno = UndoRecPtrGetLogNo(urp);
	UndoLogOffset offset = UndoRecPtrGetOffset(urp);
	uint16		prevlen;

	/* Read length of the previous undo record. */
	prevlen = UndoGetPrevRecordLen(urp, buffer, upersistence);

	/* calculate the previous undo record pointer */
	return MakeUndoRecPtr(logno, offset - prevlen);
}
