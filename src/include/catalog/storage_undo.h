/*-------------------------------------------------------------------------
 *
 * storage_undo.h
 *	  prototypes for UNDO support for backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage_undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_UNDO_H
#define STORAGE_UNDO_H

#include "access/undoinsert.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

#define UNDO_SMGR_CREATE 0

extern bool smgr_undo(UndoRecInfo *urp_array,
					  int first_idx,
					  int last_idx,
					  Oid reloid,
					  FullTransactionId full_xid,
					  BlockNumber blkno,
					  bool blk_chain_complete);

extern void smgr_undo_desc(StringInfo buf, UnpackedUndoRecord *record);

#endif
