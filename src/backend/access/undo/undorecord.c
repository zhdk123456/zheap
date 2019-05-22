/*-------------------------------------------------------------------------
 *
 * undorecord.c
 *	  encode and decode undo records
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undorecord.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/subtrans.h"
#include "access/undorecord.h"
#include "catalog/pg_tablespace.h"
#include "storage/block.h"

/* Prototypes for static functions. */
static bool InsertUndoBytes(char *sourceptr, int sourcelen,
				char **writeptr, char *endptr,
				int *total_bytes_written, int *partial_write);
static bool ReadUndoBytes(char *destptr, int readlen,
			  char **readptr, char *endptr,
			  int *total_bytes_read, int *partial_read);

/*
 * Compute and return the expected size of an undo record.
 */
Size
UndoRecordExpectedSize(UnpackedUndoRecord *uur)
{
	Size		size;

	size = SizeOfUndoRecordHeader + sizeof(uint16);
	if ((uur->uur_info & UREC_INFO_FORK) != 0)
		size += sizeof(ForkNumber);
	if ((uur->uur_info & UREC_INFO_BLOCK) != 0)
		size += SizeOfUndoRecordBlock;
	if ((uur->uur_info & UREC_INFO_BLKPREV) != 0)
		size += sizeof(UndoRecPtr);
	if ((uur->uur_info & UREC_INFO_TRANSACTION) != 0)
		size += SizeOfUndoRecordTransaction;
	if ((uur->uur_info & UREC_INFO_PAYLOAD) != 0)
	{
		size += SizeOfUndoRecordPayload;
		size += uur->uur_payload.len;
		size += uur->uur_tuple.len;
	}

	return size;
}

/*
 * Compute size of the Unpacked undo record in memory
 */
Size
UnpackedUndoRecordSize(UnpackedUndoRecord *uur)
{
	Size		size;

	size = sizeof(UnpackedUndoRecord);

	/* Add payload size if record contains payload data. */
	if ((uur->uur_info & UREC_INFO_PAYLOAD) != 0)
	{
		size += uur->uur_payload.len;
		size += uur->uur_tuple.len;
	}

	return size;
}

/*
 * Initiate inserting an undo record.
 *
 * This function will initialize the context for inserting and undo record
 * which will be inserted by calling InsertUndoData.
 */
void
BeginInsertUndo(UndoPackContext *ucontext, UnpackedUndoRecord *uur)
{
	ucontext->stage = UNDO_PACK_STAGE_HEADER;
	ucontext->already_processed = 0;
	ucontext->partial_bytes = 0;

	/* Copy undo record header. */
	ucontext->urec_hd.urec_rmid = uur->uur_rmid;
	ucontext->urec_hd.urec_type = uur->uur_type;
	ucontext->urec_hd.urec_info = uur->uur_info;
	ucontext->urec_hd.urec_reloid = uur->uur_reloid;
	ucontext->urec_hd.urec_xid = uur->uur_xid;
	ucontext->urec_hd.urec_cid = uur->uur_cid;

	/* Copy undo record relation header if it is present. */
	if ((uur->uur_info & UREC_INFO_FORK) != 0)
		ucontext->urec_fork = uur->uur_fork;

	/* Copy undo record block header if it is present. */
	if ((uur->uur_info & UREC_INFO_BLOCK) != 0)
	{
		ucontext->urec_blk.urec_block = uur->uur_block;
		ucontext->urec_blk.urec_offset = uur->uur_offset;
	}

	/* Copy undo record block prev if it is present. */
	if ((uur->uur_info & UREC_INFO_BLKPREV) != 0)
	{
		ucontext->urec_blkprev = uur->uur_blkprev;
	}

	/* Copy undo record transaction header if it is present. */
	if ((uur->uur_info & UREC_INFO_TRANSACTION) != 0)
	{
		ucontext->urec_txn.urec_progress = uur->uur_progress;
		ucontext->urec_txn.urec_xidepoch = uur->uur_xidepoch;
		ucontext->urec_txn.urec_dbid = uur->uur_dbid;
		ucontext->urec_txn.urec_prevurp = uur->uur_prevurp;
		ucontext->urec_txn.urec_next = uur->uur_next;
	}

	/* Copy undo record payload header and data if it is present. */
	if ((uur->uur_info & UREC_INFO_PAYLOAD) != 0)
	{
		ucontext->urec_payload.urec_payload_len = uur->uur_payload.len;
		ucontext->urec_payload.urec_tuple_len = uur->uur_tuple.len;
		ucontext->urec_payloaddata = uur->uur_payload.data;
		ucontext->urec_tupledata = uur->uur_tuple.data;
	}

	ucontext->undo_len = UndoRecordExpectedSize(uur);
}

/*
 * Insert the undo record into the input page from the unpack undo context.
 *
 * Caller can  call this function multiple times until desired stage is reached.
 * This will write the undo record into the page.
 */
void
InsertUndoData(UndoPackContext *ucontext, Page page, int starting_byte)
{
	char	   *writeptr = (char *) page + starting_byte;
	char	   *endptr = (char *) page + BLCKSZ;

	switch (ucontext->stage)
	{
		case UNDO_PACK_STAGE_HEADER:
			/* Insert undo record header. */
			if (!InsertUndoBytes((char *) &ucontext->urec_hd,
								 SizeOfUndoRecordHeader, &writeptr, endptr,
								 &ucontext->already_processed,
								 &ucontext->partial_bytes))
				return;
			ucontext->stage = UNDO_PACK_STAGE_FORKNUM;
			/* fall through */

		case UNDO_PACK_STAGE_FORKNUM:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_FORK) != 0)
			{
				/* Insert undo record fork number. */
				if (!InsertUndoBytes((char *) &ucontext->urec_fork,
									 sizeof(ForkNumber),
									 &writeptr, endptr,
									 &ucontext->already_processed,
									 &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_BLOCK;
			/* fall through */

		case UNDO_PACK_STAGE_BLOCK:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLOCK) != 0)
			{
				/* Insert undo record block header. */
				if (!InsertUndoBytes((char *) &ucontext->urec_blk,
									 SizeOfUndoRecordBlock,
									 &writeptr, endptr,
									 &ucontext->already_processed,
									 &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_BLOCKPREV;
			/* fall through */

		case UNDO_PACK_STAGE_BLOCKPREV:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLKPREV) != 0)
			{
				/* Insert undo record blkprev. */
				if (!InsertUndoBytes((char *) &ucontext->urec_blkprev,
									 sizeof(UndoRecPtr),
									 &writeptr, endptr,
									 &ucontext->already_processed,
									 &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_TRANSACTION;
			/* fall through */

		case UNDO_PACK_STAGE_TRANSACTION:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_TRANSACTION) != 0)
			{
				/* Insert undo record transaction header. */
				if (!InsertUndoBytes((char *) &ucontext->urec_txn,
									 SizeOfUndoRecordTransaction,
									 &writeptr, endptr,
									 &ucontext->already_processed,
									 &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD;
			/* fall through */

		case UNDO_PACK_STAGE_PAYLOAD:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_PAYLOAD) != 0)
			{
				/* Insert undo record payload header. */
				if (!InsertUndoBytes((char *) &ucontext->urec_payload,
									 SizeOfUndoRecordPayload,
									 &writeptr, endptr,
									 &ucontext->already_processed,
									 &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD_DATA;
			/* fall through */

		case UNDO_PACK_STAGE_PAYLOAD_DATA:
			{
				int			len = ucontext->urec_payload.urec_payload_len;

				if (len > 0)
				{
					/* Insert payload data. */
					if (!InsertUndoBytes((char *) ucontext->urec_payloaddata,
										 len, &writeptr, endptr,
										 &ucontext->already_processed,
										 &ucontext->partial_bytes))
						return;
				}
				ucontext->stage = UNDO_PACK_STAGE_TUPLE_DATA;
			}
			/* fall through */

		case UNDO_PACK_STAGE_TUPLE_DATA:
			{
				int			len = ucontext->urec_payload.urec_tuple_len;

				if (len > 0)
				{
					/* Insert tuple data. */
					if (!InsertUndoBytes((char *) ucontext->urec_tupledata,
										 len, &writeptr, endptr,
										 &ucontext->already_processed,
										 &ucontext->partial_bytes))
						return;
				}
				ucontext->stage = UNDO_PACK_STAGE_UNDO_LENGTH;
			}
			/* fall through */

		case UNDO_PACK_STAGE_UNDO_LENGTH:
			/* Insert undo length. */
			if (!InsertUndoBytes((char *) &ucontext->undo_len,
								 sizeof(uint16), &writeptr, endptr,
								 &ucontext->already_processed,
								 &ucontext->partial_bytes))
				return;

			ucontext->stage = UNDO_PACK_STAGE_DONE;
			/* fall through */

		case UNDO_PACK_STAGE_DONE:
			/* Nothing to be done. */
			break;

		default:
			Assert(0);			/* Invalid stage */
	}
}

/*
 * Skip inserting undo record
 *
 * Don't insert the actual undo record instead just update the context data
 * so that if we need to insert the remaining partial record to the next
 * block then we have right context.
 */
void
SkipInsertingUndoData(UndoPackContext *ucontext, int bytes_to_skip)
{
	switch (ucontext->stage)
	{
		case UNDO_PACK_STAGE_HEADER:
			if (bytes_to_skip < SizeOfUndoRecordHeader)
			{
				ucontext->partial_bytes = bytes_to_skip;
				return;
			}
			bytes_to_skip -= SizeOfUndoRecordHeader;
			ucontext->stage = UNDO_PACK_STAGE_FORKNUM;
			/* fall through */

		case UNDO_PACK_STAGE_FORKNUM:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_FORK) != 0)
			{
				if (bytes_to_skip < sizeof(ForkNumber))
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= sizeof(ForkNumber);
			}

			ucontext->stage = UNDO_PACK_STAGE_BLOCK;
			/* fall through */

		case UNDO_PACK_STAGE_BLOCK:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLOCK) != 0)
			{
				if (bytes_to_skip < SizeOfUndoRecordBlock)
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= SizeOfUndoRecordBlock;
			}
			ucontext->stage = UNDO_PACK_STAGE_BLOCKPREV;
			/* fall through */

		case UNDO_PACK_STAGE_BLOCKPREV:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLKPREV) != 0)
			{
				if (bytes_to_skip < sizeof(UndoRecPtr))
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= sizeof(UndoRecPtr);
			}
			ucontext->stage = UNDO_PACK_STAGE_TRANSACTION;
			/* fall through */

		case UNDO_PACK_STAGE_TRANSACTION:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_TRANSACTION) != 0)
			{
				if (bytes_to_skip < SizeOfUndoRecordTransaction)
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= SizeOfUndoRecordTransaction;
			}

			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD;
			/* fall through */

		case UNDO_PACK_STAGE_PAYLOAD:
			/* Skip payload header. */
			if ((ucontext->urec_hd.urec_info & UREC_INFO_PAYLOAD) != 0)
			{
				if (bytes_to_skip < SizeOfUndoRecordPayload)
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= SizeOfUndoRecordPayload;
			}
			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD_DATA;
			/* fall through */

		case UNDO_PACK_STAGE_PAYLOAD_DATA:
			if (ucontext->urec_payload.urec_payload_len > 0)
			{
				if (bytes_to_skip < ucontext->urec_payload.urec_payload_len)
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= ucontext->urec_payload.urec_payload_len;
			}
			ucontext->stage = UNDO_PACK_STAGE_TUPLE_DATA;
			/* fall through */

		case UNDO_PACK_STAGE_TUPLE_DATA:
			if (ucontext->urec_payload.urec_tuple_len > 0)
			{
				if (bytes_to_skip < ucontext->urec_payload.urec_tuple_len)
				{
					ucontext->partial_bytes = bytes_to_skip;
					return;
				}
				bytes_to_skip -= ucontext->urec_payload.urec_tuple_len;
			}
			ucontext->stage = UNDO_PACK_STAGE_UNDO_LENGTH;
			/* fall through */

		case UNDO_PACK_STAGE_UNDO_LENGTH:
			ucontext->stage = UNDO_PACK_STAGE_DONE;
			 /* fall through */ ;

		case UNDO_PACK_STAGE_DONE:
			/* Nothing to be done. */
			break;

		default:
			Assert(0);			/* Invalid stage */
	}
}

/*
 * Write undo bytes from a particular source, but only to the extent that
 * they weren't written previously and will fit.
 *
 * 'sourceptr' points to the source data, and 'sourcelen' is the length of
 * that data in bytes.
 *
 * 'writeptr' points to the insertion point for these bytes, and is updated
 * for whatever we write.  The insertion point must not pass 'endptr', which
 * represents the end of the buffer into which we are writing.
 *
 * 'my_bytes_written' is a pointer to the count of previous-written bytes
 * from this and following structures in this undo record; that is, any
 * bytes that are part of previous structures in the record have already
 * been subtracted out.
 *
 * 'total_bytes_written' points to the count of all previously-written bytes,
 * and must it must be updated for the bytes we write.
 *
 * The return value is false if we ran out of space before writing all
 * the bytes, and otherwise true.
 */
static bool
InsertUndoBytes(char *sourceptr, int sourcelen, char **writeptr, char *endptr,
				int *total_bytes_written, int *partial_write)
{
	int			can_write;
	int			remaining;

	/* Compute number of bytes we can write. */
	remaining = sourcelen - *partial_write;
	can_write = Min(remaining, endptr - *writeptr);

	/* Bail out if no bytes can be written. */
	if (can_write == 0)
		return false;

	/* Copy the bytes we can write. */
	memcpy(*writeptr, sourceptr + *partial_write, can_write);

	/* Update bookkeeping information. */
	*writeptr += can_write;
	*total_bytes_written += can_write;

	/* Could not read whole data so set the partial_read. */
	if (can_write < remaining)
	{
		*partial_write += can_write;
		return false;
	}

	/* Return true only if we wrote the whole thing. */
	*partial_write = 0;
	return true;
}

/*
 * Initiate unpacking an undo record.
 *
 * This function will initialize the context for unpacking the undo record which
 * will be unpacked by calling UnpackUndoData.
 */
void
BeginUnpackUndo(UndoPackContext *ucontext)
{
	ucontext->stage = UNDO_PACK_STAGE_HEADER;
	ucontext->already_processed = 0;
	ucontext->partial_bytes = 0;
}

/*
 * Read the undo record from the input page to the unpack undo context.
 *
 * Caller can  call this function multiple times until desired stage is reached.
 * This will read the undo record from the page and store the data into unpack
 * undo context, which can be later copied to unpacked undo record by calling
 * FinishUnpackUndo.
 */
void
UnpackUndoData(UndoPackContext *ucontext, Page page, int starting_byte)
{
	char	   *readptr = (char *) page + starting_byte;
	char	   *endptr = (char *) page + BLCKSZ;

	switch (ucontext->stage)
	{
		case UNDO_PACK_STAGE_HEADER:
			if (!ReadUndoBytes((char *) &ucontext->urec_hd,
							   SizeOfUndoRecordHeader, &readptr, endptr,
							   &ucontext->already_processed,
							   &ucontext->partial_bytes))
				return;
			ucontext->stage = UNDO_PACK_STAGE_FORKNUM;
			/* fall through */
		case UNDO_PACK_STAGE_FORKNUM:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_FORK) != 0)
			{
				if (!ReadUndoBytes((char *) &ucontext->urec_fork,
								   sizeof(ForkNumber),
								   &readptr, endptr, &ucontext->already_processed,
								   &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_BLOCK;
			/* fall through */
		case UNDO_PACK_STAGE_BLOCK:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLOCK) != 0)
			{
				if (!ReadUndoBytes((char *) &ucontext->urec_blk,
								   SizeOfUndoRecordBlock,
								   &readptr, endptr, &ucontext->already_processed,
								   &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_BLOCKPREV;
			/* fall through */
		case UNDO_PACK_STAGE_BLOCKPREV:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_BLKPREV) != 0)
			{
				if (!ReadUndoBytes((char *) &ucontext->urec_blkprev,
								   sizeof(UndoRecPtr),
								   &readptr, endptr, &ucontext->already_processed,
								   &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_TRANSACTION;
			/* fall through */
		case UNDO_PACK_STAGE_TRANSACTION:
			if ((ucontext->urec_hd.urec_info & UREC_INFO_TRANSACTION) != 0)
			{
				if (!ReadUndoBytes((char *) &ucontext->urec_txn,
								   SizeOfUndoRecordTransaction,
								   &readptr, endptr, &ucontext->already_processed,
								   &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD;
			/* fall through */
		case UNDO_PACK_STAGE_PAYLOAD:
			/* Read payload header. */
			if ((ucontext->urec_hd.urec_info & UREC_INFO_PAYLOAD) != 0)
			{
				if (!ReadUndoBytes((char *) &ucontext->urec_payload,
								   SizeOfUndoRecordPayload,
								   &readptr, endptr, &ucontext->already_processed,
								   &ucontext->partial_bytes))
					return;
			}
			ucontext->stage = UNDO_PACK_STAGE_PAYLOAD_DATA;
			/* fall through */
		case UNDO_PACK_STAGE_PAYLOAD_DATA:
			{
				int			len = ucontext->urec_payload.urec_payload_len;

				/* Allocate memory for the payload data if not already done. */
				if (len > 0)
				{
					if (ucontext->urec_payloaddata == NULL)
						ucontext->urec_payloaddata = (char *) palloc(len);

					/* Read payload data. */
					if (!ReadUndoBytes((char *) ucontext->urec_payloaddata, len,
									   &readptr, endptr, &ucontext->already_processed,
									   &ucontext->partial_bytes))
						return;
				}
				ucontext->stage = UNDO_PACK_STAGE_TUPLE_DATA;
				/* fall through */
			}
		case UNDO_PACK_STAGE_TUPLE_DATA:
			{
				int			len = ucontext->urec_payload.urec_tuple_len;

				/* Allocate memory for the tuple data if not already done. */
				if (len > 0)
				{
					if (ucontext->urec_tupledata == NULL)
						ucontext->urec_tupledata = (char *) palloc(len);

					/* Read tuple data. */
					if (!ReadUndoBytes((char *) ucontext->urec_tupledata, len,
									   &readptr, endptr, &ucontext->already_processed,
									   &ucontext->partial_bytes))
						return;
				}

				ucontext->stage = UNDO_PACK_STAGE_DONE;
				/* fall through */
			}
		case UNDO_PACK_STAGE_DONE:
			/* Nothing to be done. */
			break;
		default:
			Assert(0);			/* Invalid stage */
	}

	return;
}

/*
 * Final step of unpacking the undo record.
 *
 * Copy the undo record data from the unpack undo context to the input unpacked
 * undo record.
 */
void
FinishUnpackUndo(UndoPackContext *ucontext, UnpackedUndoRecord *uur)
{
	/* Copy undo record header. */
	uur->uur_rmid = ucontext->urec_hd.urec_rmid;
	uur->uur_type = ucontext->urec_hd.urec_type;
	uur->uur_info = ucontext->urec_hd.urec_info;
	uur->uur_reloid = ucontext->urec_hd.urec_reloid;
	uur->uur_xid = ucontext->urec_hd.urec_xid;
	uur->uur_cid = ucontext->urec_hd.urec_cid;

	/* Copy undo record relation header if it is present. */
	if ((uur->uur_info & UREC_INFO_FORK) != 0)
		uur->uur_fork = ucontext->urec_fork;

	/* Copy undo record block header if it is present. */
	if ((uur->uur_info & UREC_INFO_BLOCK) != 0)
	{
		uur->uur_block = ucontext->urec_blk.urec_block;
		uur->uur_offset = ucontext->urec_blk.urec_offset;
	}

	/* Copy undo record block prev header if it is present. */
	if ((uur->uur_info & UREC_INFO_BLKPREV) != 0)
	{
		uur->uur_blkprev = ucontext->urec_blkprev;
	}

	/* Copy undo record transaction header if it is present. */
	if ((uur->uur_info & UREC_INFO_TRANSACTION) != 0)
	{
		uur->uur_progress = ucontext->urec_txn.urec_progress;
		uur->uur_xidepoch = ucontext->urec_txn.urec_xidepoch;
		uur->uur_dbid = ucontext->urec_txn.urec_dbid;
		uur->uur_prevurp = ucontext->urec_txn.urec_prevurp;
		uur->uur_next = ucontext->urec_txn.urec_next;
	}

	/* Copy undo record payload header and data if it is present. */
	if ((uur->uur_info & UREC_INFO_PAYLOAD) != 0)
	{
		uur->uur_payload.len = ucontext->urec_payload.urec_payload_len;
		uur->uur_tuple.len = ucontext->urec_payload.urec_tuple_len;

		/* Read payload data if its length is not 0. */
		if (uur->uur_payload.len != 0)
			uur->uur_payload.data = ucontext->urec_payloaddata;

		/* Read tuple data if its length is not 0. */
		if (uur->uur_tuple.len != 0)
			uur->uur_tuple.data = ucontext->urec_tupledata;
	}
}

/*
 * Read undo bytes into a particular destination,
 *
 * 'destptr' points to the source data, and 'readlen' is the length of
 * that data to be read in bytes.
 *
 * 'readptr' points to the read point for these bytes, and is updated
 * for how much we read.  The read point must not pass 'endptr', which
 * represents the end of the buffer from which we are reading.
 *
 * 'partial_read' is a pointer to the count of previous partial read bytes
 *
 * 'total_bytes_read' points to the count of all previously-read bytes,
 * and must likewise be updated for the bytes we read.
 *
 * nocopy if this flag is set true then it will just skip the readlen
 * size in undo but it will not copy into the buffer.
 *
 * The return value is false if we ran out of space before read all
 * the bytes, and otherwise true.
 */
static bool
ReadUndoBytes(char *destptr, int readlen, char **readptr, char *endptr,
			  int *total_bytes_read, int *partial_read)
{
	int			can_read;
	int			remaining;

	/* Compute number of bytes we can read. */
	remaining = readlen - *partial_read;
	can_read = Min(remaining, endptr - *readptr);

	/* Bail out if no bytes can be read. */
	if (can_read == 0)
		return false;

	/* Copy the bytes we can read. */
	memcpy(destptr + *partial_read, *readptr, can_read);

	/* Update bookkeeping information. */
	*readptr += can_read;
	*total_bytes_read += can_read;

	/* Could not read whole data so set the partial_read. */
	if (can_read < remaining)
	{
		*partial_read += can_read;
		return false;
	}

	/* Return true only if we wrote the whole thing. */
	*partial_read = 0;

	return true;
}

/*
 * Set uur_info for an UnpackedUndoRecord appropriately based on which
 * other fields are set.
 */
void
UndoRecordSetInfo(UnpackedUndoRecord *uur)
{
	if (uur->uur_fork != MAIN_FORKNUM)
		uur->uur_info |= UREC_INFO_FORK;
	if (uur->uur_block != InvalidBlockNumber)
		uur->uur_info |= UREC_INFO_BLOCK;
	if (uur->uur_blkprev != InvalidUndoRecPtr)
		uur->uur_info |= UREC_INFO_BLKPREV;
	if (uur->uur_next != InvalidUndoRecPtr)
		uur->uur_info |= UREC_INFO_TRANSACTION;
	if (uur->uur_payload.len || uur->uur_tuple.len)
		uur->uur_info |= UREC_INFO_PAYLOAD;
}
