/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/index/indexam.c
 *
 * INTERFACE ROUTINES
 *		index_open		- open an index relation by relation OID
 *		index_close		- close an index relation
 *		index_beginscan - start a scan of an index with amgettuple
 *		index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *		index_rescan	- restart a scan of an index
 *		index_endscan	- end a scan
 *		index_insert	- insert an index tuple into a relation
 *		index_markpos	- mark a scan position
 *		index_restrpos	- restore a scan position
 *		index_getnext_tid	- get the next TID from a scan
 *		index_fetch_heap		- get the scan's next heap tuple
 *		index_getnext	- get the next heap tuple from a scan
 *		index_getbitmap - get all tuples from a scan
 *		index_bulk_delete	- bulk deletion of index tuples
 *		index_vacuum_cleanup	- post-deletion cleanup of an index
 *		index_can_return	- does index support index-only scans?
 *		index_getprocid - get a support procedure OID
 *		index_getprocinfo - get a support procedure's lookup info
 *
 * renata
 * TODO:
 * I need to add:
 * 		index_beginscan_smooth - to initialize structures for Smooth Scan
 * 		index_getnext_smooth - get next heap tuple from index scan!
 * potentially something else, if it's different from classical functions
 * COMPARE Index and bitmap - it may be clearer
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *
 * old comments
 *		Scans are implemented as follows:
 *
 *		`0' represents an invalid item pointer.
 *		`-' represents an unknown item pointer.
 *		`X' represents a known item pointers.
 *		`+' represents known or invalid item pointers.
 *		`*' represents any item pointers.
 *
 *		State is represented by a triple of these symbols in the order of
 *		previous, current, next.  Note that the case of reverse scans works
 *		identically.
 *
 *				State	Result
 *		(1)		+ + -	+ 0 0			(if the next item pointer is invalid)
 *		(2)				+ X -			(otherwise)
 *		(3)		* 0 0	* 0 0			(no change)
 *		(4)		+ X 0	X 0 0			(shift)
 *		(5)		* + X	+ X -			(shift, add unknown)
 *
 *		All other states cannot occur.
 *
 *		Note: It would be possible to cache the status of the previous and
 *			  next item pointer using the flags.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
/*renata */
#include "optimizer/cost.h"
#include "executor/execdebug.h"
#include "access/nbtree.h"
#include "access/valid.h"
#include "smooth/smoothscanopaque.h"
#include "executor/nodeIndexsmoothscan.h"
#include "executor/executor.h"

/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.	This helps to catch
 * things that don't work when reindexing system catalogs.  The assertion
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS \
( \
	AssertMacro(RelationIsValid(indexRelation)), \
	AssertMacro(PointerIsValid(indexRelation->rd_am)), \
	AssertMacro(!ReindexIsProcessingIndex(RelationGetRelid(indexRelation))) \
)

#define SCAN_CHECKS \
( \
	AssertMacro(IndexScanIsValid(scan)), \
	AssertMacro(RelationIsValid(scan->indexRelation)), \
	AssertMacro(PointerIsValid(scan->indexRelation->rd_am)) \
)

//#define SmoothProcessOnePage(scan, page, direction, no_orig_keys, orig_keys ) \
//( \
//	Page		dp; \
//	int			lines; \
//	OffsetNumber lineoff;  \
//	int			linesleft;  \
//	ItemId		lpp;  \
//	bool		valid;  \
//	bool		got_heap_tuple = false;  \
//
//	HeapTuple	tuple = &(scan->xs_ctup);
//	int itemIndex = 0;  \
//	SmoothScanOpaque  smoothDesc =  (SmoothScanOpaque)scan->smoothInfo;  \
//
//
//	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);  \
//	/* get page information */   \
//	dp = (Page) BufferGetPage(scan->xs_cbuf);  \
//
//	if (ScanDirectionIsForward(direction))  \
//	{ \
//		/* start from the first tuple */ \
//		lineoff = FirstOffsetNumber; \
//	} \
//	else \
//	{ \
//		lineoff =/* next offnum */ \
//				OffsetNumberNext(ItemPointerGetOffsetNumber(&(scan->xs_ctup.t_self))); \
//	} \
//
//	/* end is maximum offset for a page */
//	lines = PageGetMaxOffsetNumber(dp);  \
//	linesleft = lines - lineoff + 1; \
//
//	/*
//	 * advance the scan until we find a qualifying tuple or run out of stuff
//	 * to scan
//	 */ \
//	itemIndex = 0;  \
//
//	/* initialize tuple workspace to empty */
//	smoothDesc->currPos.nextTupleOffset = 0;  \
//
//	while (linesleft > 0) \
//	{ \
//		lpp = PageGetItemId(dp, lineoff); \
//
//		if (ItemIdIsNormal(lpp)) \
//		{ \
//			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp); \
//			tuple->t_len = ItemIdGetLength(lpp); \
//			tuple->t_tableOid = scan->heapRelation->rd_id; \
//			ItemPointerSet(&(tuple->t_self), page, lineoff); \
//
//			got_heap_tuple = HeapTupleSatisfiesVisibility(tuple, scan->xs_snapshot, scan->xs_cbuf); \
//			//CheckForSerializableConflictOut(got_heap_tuple, scan->heapRelation, &tuple,
//			//								scan->xs_cbuf, scan->xs_snapshot);
//			if (got_heap_tuple){ \
//				PredicateLockTuple(scan->heapRelation, tuple, scan->xs_snapshot); \
//				if (no_orig_keys > 0){ \
//					HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_keys, orig_keys, valid); \
//					/* next line is just for vtune (to see the code of a macro) */
//					//valid = HeapKeyTestSmooth(tuple, RelationGetDescr(scan->heapRelation), no_orig_keys, orig_keys);
//					if (valid){ \
//						_bt_saveheapitem(smoothDesc, itemIndex, lineoff, tuple); \
//						itemIndex++; \
//					} \
//				} \
//			} \
//
//
//
//		} /* if tuple is normal*/ \
//		/*
//		 * otherwise move to the next item on the page
//		 */
//		--linesleft; \
//
//		if (ScanDirectionIsForward(direction)) \
//		{ \
//			++lpp;			/* move forward in this page's ItemId array */ \
//			++lineoff; \
//		} \
//		else \
//		{ \
//			--lpp;			/* move back in this page's ItemId array */ \
//			--lineoff; \
//		} \
//		 \
//	} \
//
//	/* fetched all tuples from a page. Set index to 0 (as a starting position)*/
//	smoothDesc->currPos.firstHeapItem = 0; \
//	smoothDesc->currPos.lastHeapItem = itemIndex - 1; \
//	smoothDesc->currPos.itemHeapIndex = 0; \
//	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK); \
//
//
//
//	/* set first tuple for the found ones */
//	if(itemIndex > 0){ \
//
//		/* OK, itemIndex says what to return */
//		SmoothScanPosItem *currItem = &smoothDesc->currPos.items[smoothDesc->currPos.itemHeapIndex]; \
//
//		//scan->xs_ctup.t_self = currItem->heapTid;
//		scan->xs_ctup = *((HeapTuple) (smoothDesc->currTuples + currItem->tupleOffset)); \
//
//		if(itemIndex > 1){ \
//			smoothDesc->more_data_for_smooth = true; \
//		}else{ \
//			/* item is 1 - return it and make page as visited */
//			smoothDesc->more_data_for_smooth = false; \
//
//			/* static array option */
//			//smoothDesc->vispages[page] = (PageBitmap)1;
//
//			/* bitmap set option */
//			smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page); \
//			/* set next page to process in a sequential manner */
//			if (smoothDesc->prefetch_pages) \
//				smoothDesc->nextPageId = page + 1; \
//
//			/* 3. "clear" currPos space */
//			smoothDesc->currPos.firstHeapItem = 0; \
//			smoothDesc->currPos.lastHeapItem  = 0; \
//			smoothDesc->currPos.itemHeapIndex = 0; \
//			/* initialize tuple workspace to empty */
//			smoothDesc->currPos.nextTupleOffset = 0; \
//
//		} \
//
//		return &scan->xs_ctup; \
//	}else{ \
//
//		ItemPointerSetInvalid(&(tuple->t_self)); \
//		tuple->t_tableOid = InvalidOid; \
//
//		/* static array option */
//		//smoothDesc->vispages[page] = (PageBitmap)1;
//
//		/* bitmap set option */
//		smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page); \
//		if (smoothDesc->prefetch_pages) \
//			smoothDesc->nextPageId = page + 1; \
//		return NULL; \
//	} \
//)

#define GET_REL_PROCEDURE(pname) \
do { \
	procedure = &indexRelation->rd_aminfo->pname; \
	if (!OidIsValid(procedure->fn_oid)) \
	{ \
		RegProcedure	procOid = indexRelation->rd_am->pname; \
		if (!RegProcedureIsValid(procOid)) \
			elog(ERROR, "invalid %s regproc", CppAsString(pname)); \
		fmgr_info_cxt(procOid, procedure, indexRelation->rd_indexcxt); \
	} \
} while(0)

#define GET_SCAN_PROCEDURE(pname) \
do { \
	procedure = &scan->indexRelation->rd_aminfo->pname; \
	if (!OidIsValid(procedure->fn_oid)) \
	{ \
		RegProcedure	procOid = scan->indexRelation->rd_am->pname; \
		if (!RegProcedureIsValid(procOid)) \
			elog(ERROR, "invalid %s regproc", CppAsString(pname)); \
		fmgr_info_cxt(procOid, procedure, scan->indexRelation->rd_indexcxt); \
	} \
} while(0)

static IndexScanDesc index_beginscan_internal(Relation indexRelation, int nkeys, int norderbys, Snapshot snapshot);

/* ----------------------------------------------------------------
 *				   index_ interface functions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_open - open an index relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the index.	(Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		index already.)
 *
 *		An error is raised if the index does not exist.
 *
 *		This is a convenience routine adapted for indexscan use.
 *		Some callers may prefer to use relation_open directly.
 * ----------------
 */
Relation index_open(Oid relationId, LOCKMODE lockmode) {
	Relation r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind != RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not an index", RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		index_close - close an index relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond index_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void index_close(Relation relation, LOCKMODE lockmode) {
	LockRelId relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}

/* ----------------
 *		index_insert - insert an index tuple into a relation
 * ----------------
 */
bool index_insert(Relation indexRelation, Datum *values, bool *isnull, ItemPointer heap_t_ctid, Relation heapRelation,
		IndexUniqueCheck checkUnique) {
	FmgrInfo *procedure;

	RELATION_CHECKS;
	GET_REL_PROCEDURE(aminsert);

	if (!(indexRelation->rd_am->ampredlocks))
		CheckForSerializableConflictIn(indexRelation, (HeapTuple) NULL, InvalidBuffer);

	/*
	 * have the am's insert proc do all the work.
	 */
	return DatumGetBool(FunctionCall6(procedure,
					PointerGetDatum(indexRelation),
					PointerGetDatum(values),
					PointerGetDatum(isnull),
					PointerGetDatum(heap_t_ctid),
					PointerGetDatum(heapRelation),
					Int32GetDatum((int32) checkUnique)));
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc index_beginscan(Relation heapRelation, Relation indexRelation, Snapshot snapshot, int nkeys,
		int norderbys) {
	IndexScanDesc scan;

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;

	return scan;
}

/*
 * index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *
 * As above, caller had better be holding some lock on the parent heap
 * relation, even though it's not explicitly mentioned here.
 */
IndexScanDesc index_beginscan_bitmap(Relation indexRelation, Snapshot snapshot, int nkeys) {
	IndexScanDesc scan;

	scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->xs_snapshot = snapshot;

	return scan;
}

/*
 * index_beginscan_internal --- common code for index_beginscan variants
 */
static IndexScanDesc index_beginscan_internal(Relation indexRelation, int nkeys, int norderbys, Snapshot snapshot) {
	IndexScanDesc scan;
	FmgrInfo *procedure;

	RELATION_CHECKS;
	GET_REL_PROCEDURE(ambeginscan);

	if (!(indexRelation->rd_am->ampredlocks))
		PredicateLockRelation(indexRelation, snapshot);

	/*
	 * We hold a reference count to the relcache entry throughout the scan.
	 */
	RelationIncrementReferenceCount(indexRelation);

	/*
	 * Tell the AM to open a scan.
	 */
	scan = (IndexScanDesc) DatumGetPointer(FunctionCall3(procedure,
					PointerGetDatum(indexRelation),
					Int32GetDatum(nkeys),
					Int32GetDatum(norderbys)));

	return scan;
}

/* ----------------
 *		index_rescan  - (re)start a scan of an index
 *
 * During a restart, the caller may specify a new set of scankeys and/or
 * orderbykeys; but the number of keys cannot differ from what index_beginscan
 * was told.  (Later we might relax that to "must not exceed", but currently
 * the index AMs tend to assume that scan->numberOfKeys is what to believe.)
 * To restart the scan without changing keys, pass NULL for the key arrays.
 * (Of course, keys *must* be passed on the first call, unless
 * scan->numberOfKeys is zero.)
 * ----------------
 */
void index_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys) {
	FmgrInfo *procedure;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amrescan);

	Assert(nkeys == scan->numberOfKeys);
	Assert(norderbys == scan->numberOfOrderBys);

	/* Release any held pin on a heap page */
	if (BufferIsValid(scan->xs_cbuf)) {
		ReleaseBuffer(scan->xs_cbuf);
		scan->xs_cbuf = InvalidBuffer;
	}

	scan->xs_continue_hot = false;

	scan->kill_prior_tuple = false; /* for safety */

	FunctionCall5(procedure, PointerGetDatum(scan), PointerGetDatum(keys), Int32GetDatum(nkeys),
			PointerGetDatum(orderbys), Int32GetDatum(norderbys));
}

/* ----------------
 *		index_endscan - end a scan
 * ----------------
 */
void index_endscan(IndexScanDesc scan) {
	FmgrInfo *procedure;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amendscan);

	/* Release any held pin on a heap page */
	if (BufferIsValid(scan->xs_cbuf)) {
		ReleaseBuffer(scan->xs_cbuf);
		scan->xs_cbuf = InvalidBuffer;
	}

	/* End the AM's scan */
	FunctionCall1(procedure, PointerGetDatum(scan));

	/* Release index refcount acquired by index_beginscan */
	RelationDecrementReferenceCount(scan->indexRelation);

	/* Release the scan data structure itself */
	IndexScanEnd(scan);
}

/* ----------------
 *		index_markpos  - mark a scan position
 * ----------------
 */
void index_markpos(IndexScanDesc scan) {
	FmgrInfo *procedure;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(ammarkpos);

	FunctionCall1(procedure, PointerGetDatum(scan));
}

/* ----------------
 *		index_restrpos	- restore a scan position
 *
 * NOTE: this only restores the internal scan state of the index AM.
 * The current result tuple (scan->xs_ctup) doesn't change.  See comments
 * for ExecRestrPos().
 *
 * NOTE: in the presence of HOT chains, mark/restore only works correctly
 * if the scan's snapshot is MVCC-safe; that ensures that there's at most one
 * returnable tuple in each HOT chain, and so restoring the prior state at the
 * granularity of the index AM is sufficient.  Since the only current user
 * of mark/restore functionality is nodeMergejoin.c, this effectively means
 * that merge-join plans only work for MVCC snapshots.	This could be fixed
 * if necessary, but for now it seems unimportant.
 * ----------------
 */
void index_restrpos(IndexScanDesc scan) {
	FmgrInfo *procedure;

	Assert(IsMVCCSnapshot(scan->xs_snapshot));

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amrestrpos);

	scan->xs_continue_hot = false;

	scan->kill_prior_tuple = false; /* for safety */

	FunctionCall1(procedure, PointerGetDatum(scan));
}

/* ----------------
 * index_getnext_tid - get the next TID from a scan
 *
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * ----------------
 */
ItemPointer index_getnext_tid(IndexScanDesc scan, ScanDirection direction) {
	FmgrInfo *procedure;
	bool found;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amgettuple);

	Assert(TransactionIdIsValid(RecentGlobalXmin));

	/*
	 * The AM's amgettuple proc finds the next index entry matching the scan
	 * keys, and puts the TID into scan->xs_ctup.t_self.  It should also set
	 * scan->xs_recheck and possibly scan->xs_itup, though we pay no attention
	 * to those fields here.
	 */
	found = DatumGetBool(FunctionCall2(procedure,
					PointerGetDatum(scan),
					Int32GetDatum(direction)));

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;

	/* If we're out of index entries, we're done */
	if (!found) {
		/* ... but first, release any held pin on a heap page */
		if (BufferIsValid(scan->xs_cbuf)) {
			ReleaseBuffer(scan->xs_cbuf);
			scan->xs_cbuf = InvalidBuffer;
		}
		return NULL;
	}

	pgstat_count_index_tuples(scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	return &scan->xs_ctup.t_self;
}

/* ----------------
 * renata
 * index_smoothgetnext_tid -
 * get the next TID from a scan
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * BUT what is different: We don't release the held pin
 * since we want to do our work
 * todo
 * pin should be released AFTER all tuples are either placed in result cache (if we duplicate tuple)
 * IF we store a pointer to the tuple, tuple can be released only after #tuples in 'Page check' is 0,
 * meaning we have returned all the tuples that qualify from that page
 * This code should be called:
 * but first, release any held pin on a heap page
 * 		if (BufferIsValid(scan->xs_cbuf))
 *		{
 *			ReleaseBuffer(scan->xs_cbuf);
 *			scan->xs_cbuf = InvalidBuffer;
 *		}
 * ----------------
 */
ItemPointer smoothscan_getnext_tid(IndexScanDesc scan, ScanDirection direction) {
	FmgrInfo *procedure;
	bool found;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amgettuple);

	Assert(TransactionIdIsValid(RecentGlobalXmin));

	/*
	 * The AM's amgettuple proc finds the next index entry matching the scan
	 * keys, and puts the TID into scan->xs_ctup.t_self.  It should also set
	 * scan->xs_recheck and possibly scan->xs_itup, though we pay no attention
	 * to those fields here.
	 */
	found = DatumGetBool(FunctionCall2(procedure,
					PointerGetDatum(scan),
					Int32GetDatum(direction)));

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;

	/* If we're out of index entries, we're done */
	if (!found) {
		/* ... but first, release any held pin on a heap page */
		if (BufferIsValid(scan->xs_cbuf)) {
			ReleaseBuffer(scan->xs_cbuf);
			scan->xs_cbuf = InvalidBuffer;
		}
		return NULL;
	}

	pgstat_count_index_tuples(scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	return &scan->xs_ctup.t_self;
}

/* ----------------
 *		index_fetch_heap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
HeapTuple index_fetch_heap(IndexScanDesc scan) {
	ItemPointer tid = &scan->xs_ctup.t_self;
	bool all_dead = false;
	bool got_heap_tuple;

	/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
	if (!scan->xs_continue_hot) {
		/* Switch to correct buffer if we don't have it already */
		Buffer prev_buf = scan->xs_cbuf;

		scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, ItemPointerGetBlockNumber(tid));

		/*
		 * Prune page, but only if we weren't already on this page
		 */
		if (prev_buf != scan->xs_cbuf)
			heap_page_prune_opt(scan->heapRelation, scan->xs_cbuf, RecentGlobalXmin);
	}

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid, scan->heapRelation, scan->xs_cbuf, scan->xs_snapshot, &scan->xs_ctup,
			&all_dead, !scan->xs_continue_hot);
	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple) {
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		scan->xs_continue_hot = !IsMVCCSnapshot(scan->xs_snapshot);
		pgstat_count_heap_fetch(scan->indexRelation);
		return &scan->xs_ctup;
	}

	/* We've reached the end of the HOT chain. */
	scan->xs_continue_hot = false;

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).	We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;

	return NULL;
}

HeapTuple SmoothProcessOnePageOrder(IndexScanDesc scan, BlockNumber page, ScanDirection direction, int no_orig_keys,
		ScanKey orig_keys, bool prefetcher, List *target_list, List *qual_list, Index index, int no_orig_smooth_keys,
		ScanKey orig_smooth_keys, List *allqual, ExprContext *econtext, TupleTableSlot *slot) {
	Page dp;
	int lines;
	OffsetNumber lineoff;
	int linesleft;
	ItemId lpp;
	bool valid;
	bool pageHasOneResultTuple = false; // does page have at least one match
	bool got_heap_tuple = false;
	bool traversed = false; /* did we walk already hot chain for the index probe?*/
	bool backward = ScanDirectionIsBackward(direction);
	HeapTuple tuple = &(scan->xs_ctup);
	ItemPointerData originalTupleID = tuple->t_self; /*this is a copy*/
	HeapTuple copyTuple = NULL;
	int itemIndex = 0;

	SmoothScanOpaque smoothDesc = (SmoothScanOpaque) scan->smoothInfo;
	ResultCache *resultcache = smoothDesc->result_cache;

	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
	/* get page information */
	dp = (Page) BufferGetPage(scan->xs_cbuf);

	if (resultcache->status != SS_FULL) {

		if (ScanDirectionIsForward(direction)) {
			/* start from the first tuple */
			lineoff = FirstOffsetNumber;
			/* end is maximum offset for a page */
			lines = PageGetMaxOffsetNumber(dp);
			linesleft = lines - lineoff + 1;
		} else if (backward) {
			lines = PageGetMaxOffsetNumber(dp);
			lineoff = lines;
			linesleft = lineoff;
		}
		/*renata: this should not be the case */
		else {
			return NULL;
		}

		/*
		 * advance the scan until we find a qualifying tuple or run out of stuff
		 * to scan
		 */
		itemIndex = 0;

		while (linesleft > 0) {
			lpp = PageGetItemId(dp, lineoff);
			valid = false;
			if (ItemIdIsNormal(lpp)) {
				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				tuple->t_tableOid = scan->heapRelation->rd_id;
				ItemPointerSet(&(tuple->t_self), page, lineoff);
				/*this is a tuple for which we are doing an index probe*/
				if (lineoff == originalTupleID.ip_posid) {
					/* have to check if we have already walked hot chain - because if we did originaltupleID value has CHANGED
					 * and it is possible to enter this 2 times) for original and final version */
					/* Note: for prefetcher - we don't have the targeted tuple */
					if (!traversed && !prefetcher) {
						/* we have to traverse through hot chain */
						got_heap_tuple = heap_hot_search_buffer(&originalTupleID, scan->heapRelation, scan->xs_cbuf,
								scan->xs_snapshot, tuple, NULL, true);
						/* after this originalTupleID is updated to show to a new one */
						/* this is the one that needs to be returned */

						if (got_heap_tuple) {
							if (!enable_filterpushdown) {
								valid = true;
							} else {
								//filter pushdown
								//check whether other predicates qualify and only then return tuple
								if (allqual != NULL) {
									//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
									//for filter push down - we need to use allqual - these are all posible predicates on that table
									valid = false;
									ExecStoreTuple(tuple, /* tuple to store */
									slot, /* slot to store in */
									InvalidBuffer, /* buffer containing tuple */
									false); /* don't pfree */

									econtext->ecxt_scantuple = slot;
									ResetExprContext(econtext);
									if (ExecQual(allqual, econtext, false)) {
										valid = true;
									}
								} else {
									/* if no keys to check - every tuple is valid*/
									valid = true;
								}

							} // if filter push down
							if (valid) {
								copyTuple = heap_copytuple(tuple);
								smooth_resultcache_add_tuple(scan, page, lineoff, tuple,
																	RelationGetDescr(scan->heapRelation), target_list, qual_list, index,
																	&pageHasOneResultTuple);
							}
							//todo check whether traversed should be on even if didn't find match
							traversed = true;
						}
					}

				} else { // end if tuple for which we are doing index probe
					/* read only visible tuples - last tuples in hot chain*/
					got_heap_tuple = HeapTupleSatisfiesVisibility(tuple, scan->xs_snapshot, scan->xs_cbuf);
					if (got_heap_tuple) {
						//renata todo: maybe i could remove this
						PredicateLockTuple(scan->heapRelation, tuple, scan->xs_snapshot);

						/* check if we have produced tuples before smooth */
						if (num_tuples_switch >= 0) {
							//if yes check if this tuple is produced before smooth has started and only if not do predicate check
//							TID_SHORT formTid;
//							form_tuple_id_short(tuple, page, &formTid);
							TID formTid;
							form_tuple_id(tuple, page, &formTid);

							if (!smooth_tuplecache_find_tuple(smoothDesc->tupleID_cache, formTid)) {
								if (no_orig_smooth_keys > 0) {

									if (enable_smoothnestedloop) {
										HeapSmoothKeyTest(tuple, RelationGetDescr(scan->heapRelation),
												no_orig_smooth_keys, orig_smooth_keys, valid);
									} else {
										HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
												orig_smooth_keys, valid);
									}
									/* next line is just for vtune (to see the code of a macro) */
									//valid = HeapKeyTestSmooth(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys);
								} else {
									valid = true;
								}
								if (valid && enable_filterpushdown) {
									//FILTER PUSH DOWN
									if (allqual != NULL) {
										//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
										//for filter push down - we need to use allqual - these are all posible predicates on that table
										valid = false;
										ExecStoreTuple(tuple, /* tuple to store */
										slot, /* slot to store in */
										InvalidBuffer, /* buffer containing tuple */
										false); /* don't pfree */

										econtext->ecxt_scantuple = slot;
										ResetExprContext(econtext);
										if (ExecQual(allqual, econtext, false)) {
											valid = true;
										}
									} else {
										/* if no keys to check - every tuple is valid*/
										valid = true;
									}
								} //filter push down
							} else {
								//if find tuple
								;//nothing
							}
						} else {
//							// Start smooth from beginning
							if (no_orig_smooth_keys > 0) {

								if (enable_smoothnestedloop) {
									HeapSmoothKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
											orig_smooth_keys, valid);
								} else {
									HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
											orig_smooth_keys, valid);
								}
								//valid = HeapKeyTestSmooth(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys);
							} else {
								/* if no keys to check - every tuple is valid*/
								valid = true;
							}
							if (valid && enable_filterpushdown) {
								// filter pushdown
								if (allqual != NULL) {
									//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
									//for filter push down - we need to use allqual - these are all posible predicates on that table
									valid = false;
									ExecStoreTuple(tuple, /* tuple to store */
									slot, /* slot to store in */
									InvalidBuffer, /* buffer containing tuple */
									false); /* don't pfree */

									econtext->ecxt_scantuple = slot;
									ResetExprContext(econtext);
									if (ExecQual(allqual, econtext, false)) {
										valid = true;
									}
								} else {
									/* if no keys to check - every tuple is valid*/
									valid = true;
								}
							} //filter pushwdown

						}
						if (valid) {
							/* this is a randomly found tuple - store it in the result cache since we will need it later */
							if (smooth_resultcache_add_tuple(scan, page, lineoff, tuple,
									RelationGetDescr(scan->heapRelation), target_list, qual_list, index,
									&pageHasOneResultTuple)) {

								itemIndex++;

							} else {
								if(resultcache->status == SS_FULL)
									break;
							}

						}

					}

				}

			} /* if tuple is normal*/
			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;

			if (ScanDirectionIsForward(direction)) {
				++lpp; /* move forward in this page's ItemId array */
				++lineoff;
			} else {
				--lpp; /* move back in this page's ItemId array */
				--lineoff;
			}

		}

		/* we have processed all tuples from this page */
		if (!linesleft && !bms_is_member(page,smoothDesc->bs_vispages)) {
			smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page);
			smoothDesc->num_vispages++;
			/* set next page to process in a sequential manner */
			if (smoothDesc->prefetch_pages)
				smoothDesc->nextPageId = page + 1;

		} else {
			//hash is full - even if we have a prefetcher - have to set it to 0
			smoothDesc->prefetch_pages = 0;
		}

		/*we have obtained the tuple we needed*/
		if (copyTuple) {
//			int batchno = -1;
//			ExecResultCacheGetBatch(scan, tuple, &batchno);
//							if (smoothDesc->result_cache->curbatch != batchno) {
//
//								//	print_tuple(RelationGetDescr(scan->heapRelation),heapTuple);
//								ExecHashJoinNewBatch(scan, batchno);
//							}

			/* copy tuple is the one we need to return */
			heap_copytuple_with_tuple(copyTuple, tuple);
			heap_freetuple(copyTuple);
			//print_slot(slot);

			//fflush(stdout);


			/* free copy, since we don't need it anymore */

			LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

		} else {
			/*we haven't obtained tuple we needed
			 * two cases: tuple is old (updated) in that case return null
			 * else: hash table is full and we jumped out of fetching tuples before we reached required tuple*/
			if (smoothDesc->result_cache->status == SS_FULL && linesleft && !prefetcher) {
				if (!traversed) {
					/* we have to traverse through hot chain */
					got_heap_tuple = heap_hot_search_buffer(&originalTupleID, scan->heapRelation, scan->xs_cbuf,
							scan->xs_snapshot, tuple, NULL, true);
					LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
					/* after this originalTupleID is updated to show to a new one */
					/* this is the one that needs to be returned */

					if (got_heap_tuple) {
						if (!enable_filterpushdown) {
							return tuple;
						} else {
							//check remaining predicates and only then return tuple if all predicates qualify
							//otherwise return NULL
							//filter pushdown
							//check whether other predicates qualify and only then return tuple
							if (allqual != NULL) {
								//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
								//for filter push down - we need to use allqual - these are all posible predicates on that table
								valid = false;
								ExecStoreTuple(tuple, /* tuple to store */
								slot, /* slot to store in */
								InvalidBuffer, /* buffer containing tuple */
								false); /* don't pfree */

								econtext->ecxt_scantuple = slot;
								ResetExprContext(econtext);
								if (ExecQual(allqual, econtext, false)) {
									valid = true;
								}
							} // no quals
							else {
								/* if no keys to check - every tuple is valid*/
								valid = true;
							}
							if (valid) {
								return tuple;
							} else {
								//additional filtering didn't pass
								ItemPointerSetInvalid(&(tuple->t_self));
								tuple->t_tableOid = InvalidOid;
								return NULL;
							}
						} //filtering push down
					} else { // didn't get any tuple
						ItemPointerSetInvalid(&(tuple->t_self));
						tuple->t_tableOid = InvalidOid;
						return NULL;
					}
				}

			}
			/*I am sure that in the case of prefetcher I will jump here and return nothing*/
			LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
			ItemPointerSetInvalid(&(tuple->t_self));
			tuple->t_tableOid = InvalidOid;
			return NULL;

		} // if we have copy tuple

		return tuple;
	} else {
		/* we are already in the full state
		 * just return a normal tuple */
		if (!prefetcher) {
			got_heap_tuple = heap_hot_search_buffer(&originalTupleID, scan->heapRelation, scan->xs_cbuf,
					scan->xs_snapshot, tuple, NULL, true);
			LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
			/* after this originalTupleID is updated to show to a new one */
			/* this is the one that needs to be returned */

			if (got_heap_tuple) {
				if (!enable_filterpushdown) {
					return tuple;
				} //else filtering pushdown
				else {
					//check remaining predicates and only then return tuple if all predicates qualify
					//otherwise return NULL
					//filter pushdown
					//check whether other predicates qualify and only then return tuple
					if (allqual != NULL) {
						//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
						//for filter push down - we need to use allqual - these are all posible predicates on that table
						valid = false;
						ExecStoreTuple(tuple, /* tuple to store */
						slot, /* slot to store in */
						InvalidBuffer, /* buffer containing tuple */
						false); /* don't pfree */

						econtext->ecxt_scantuple = slot;
						ResetExprContext(econtext);
						if (ExecQual(allqual, econtext, false)) {
							valid = true;
						}
					} // no quals
					else {
						/* if no keys to check - every tuple is valid*/
						valid = true;
					}
					if (valid) {
						return tuple;
					} else {
						//additional filtering didn't pass
						ItemPointerSetInvalid(&(tuple->t_self));
						tuple->t_tableOid = InvalidOid;
						return NULL;
					}
				} //end filtering pushdown
			} else { //we didn't get any tuple
				ItemPointerSetInvalid(&(tuple->t_self));
				tuple->t_tableOid = InvalidOid;
				return NULL;
			}
		} else {
			/* prefetcher kicked in - but our hash table became full!
			 * turn off prefether
			 * */
			LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
			smoothDesc->prefetch_pages = 0;
			smoothDesc->prefetch_target = 0;
			smoothDesc->nextPageId = InvalidBlockNumber;

			ItemPointerSetInvalid(&(tuple->t_self));
			tuple->t_tableOid = InvalidOid;
			return NULL;
		}
	}

}

HeapTuple SmoothProcessOnePage(IndexScanDesc scan, BlockNumber page, ScanDirection direction, int no_orig_keys,
		ScanKey orig_keys, int no_orig_smooth_keys, ScanKey orig_smooth_keys, List *allqual, ExprContext *econtext,
		TupleTableSlot *slot) {
	Page dp;
	int lines;
	OffsetNumber lineoff;
	int linesleft;
	ItemId lpp;
	bool valid;
	bool pageHasOneResultTuple = false; // does page have at least one match
	bool got_heap_tuple = false;
	bool backward = ScanDirectionIsBackward(direction);

	HeapTuple tuple = &(scan->xs_ctup);
	int itemIndex = 0;

	SmoothScanOpaque smoothDesc = (SmoothScanOpaque) scan->smoothInfo;

	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
	/* get page information */
	dp = (Page) BufferGetPage(scan->xs_cbuf);

	if (ScanDirectionIsForward(direction)) {
		/* start from the first tuple */
		lineoff = FirstOffsetNumber;
		/* end is maximum offset for a page */
		lines = PageGetMaxOffsetNumber(dp);
		linesleft = lines - lineoff + 1;
	} else if (backward) {
		lines = PageGetMaxOffsetNumber(dp);
		lineoff = lines;
		linesleft = lineoff;
	}
	/*renata: this should not be the case */
	else {
		return NULL;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	itemIndex = 0;

	/* initialize tuple workspace to empty */
	smoothDesc->currPos.nextTupleOffset = 0;

	while (linesleft > 0) {
		lpp = PageGetItemId(dp, lineoff);

		if (ItemIdIsNormal(lpp)) {
			tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
			tuple->t_len = ItemIdGetLength(lpp);
			tuple->t_tableOid = scan->heapRelation->rd_id;
			ItemPointerSet(&(tuple->t_self), page, lineoff);

			got_heap_tuple = HeapTupleSatisfiesVisibility(tuple, scan->xs_snapshot, scan->xs_cbuf);
			//CheckForSerializableConflictOut(got_heap_tuple, scan->heapRelation, &tuple,
			//								scan->xs_cbuf, scan->xs_snapshot);
			if (got_heap_tuple) {
				PredicateLockTuple(scan->heapRelation, tuple, scan->xs_snapshot);
				if (num_tuples_switch >= 0) {
					//if yes check if this tuple is produced before smooth has started and only if not do predicate check
//					TID_SHORT formTid;
//					form_tuple_id_short(tuple, page, &formTid);
					TID formTid;
					form_tuple_id(tuple, page, &formTid);

					if (!smooth_tuplecache_find_tuple(smoothDesc->tupleID_cache, formTid)) {
						if (no_orig_smooth_keys > 0) {
							//HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys, valid);
							/* next line is just for vtune (to see the code of a macro) */
							//valid = HeapKeyTestSmooth(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys);
							if (enable_smoothnestedloop) {
								HeapSmoothKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
										orig_smooth_keys, valid);
							} else {
								HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
										orig_smooth_keys, valid);
							}
						} else {
							valid = true;
						}
						if (valid && enable_filterpushdown) {
							//FILTER PUSH DOWN
							if (allqual != NULL) {
								//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
								//for filter push down - we need to use allqual - these are all posible predicates on that table
								valid = false;
								ExecStoreTuple(tuple, /* tuple to store */
								slot, /* slot to store in */
								InvalidBuffer, /* buffer containing tuple */
								false); /* don't pfree */

								econtext->ecxt_scantuple = slot;
								ResetExprContext(econtext);
								if (ExecQual(allqual, econtext, false)) {
									valid = true;
								}
							} else {
								/* if no keys to check - every tuple is valid*/
								valid = true;
							}
						} //filter push down
					} else {
						//if smooth_tuplecache_find_tuple
						;//nothing
					}
				} else {
					//started smooth from the beginning
					// Start smooth from beginning
					if (no_orig_smooth_keys > 0) {

						//HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys, valid);
						//valid = HeapKeyTestSmooth(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys, orig_smooth_keys);
						if (enable_smoothnestedloop) {
							HeapSmoothKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
									orig_smooth_keys, valid);
						} else {
							HeapKeyTest(tuple, RelationGetDescr(scan->heapRelation), no_orig_smooth_keys,
									orig_smooth_keys, valid);
						}
					} else {
						/* if no keys to check - every tuple is valid*/
						valid = true;
					}
					if (valid && enable_filterpushdown) {

						// filter pushdown
						if (allqual != NULL) {
							//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
							//for filter push down - we need to use allqual - these are all posible predicates on that table
							valid = false;
							ExecStoreTuple(tuple, /* tuple to store */
							slot, /* slot to store in */
							InvalidBuffer, /* buffer containing tuple */
							false); /* don't pfree */

							econtext->ecxt_scantuple = slot;
							ResetExprContext(econtext);
							if (ExecQual(allqual, econtext, false)) {
								valid = true;
							}
						} else {
							/* if no keys to check - every tuple is valid*/
							valid = true;
						}
					} //filter pushwdown

				} //if num tuples switch
				if (valid) {
					_bt_saveheapitem(smoothDesc, itemIndex, lineoff, tuple);
					smoothDesc->prefetch_counter++;
					smoothDesc->smooth_counter++;
					//17.02.2014
					//increase the counter just for the first time we calculate this page
					if (!pageHasOneResultTuple) {
						smoothDesc->global_qualifying_pages++;
						smoothDesc->local_qualifying_pages++;
						pageHasOneResultTuple = true;
					}

					itemIndex++;
				}

			}

		} /* if tuple is normal*/
		/*
		 * otherwise move to the next item on the page
		 */
		--linesleft;

		if (ScanDirectionIsForward(direction)) {
			++lpp; /* move forward in this page's ItemId array */
			++lineoff;
		} else {
			--lpp; /* move back in this page's ItemId array */
			--lineoff;
		}

	}

	/* fetched all tuples from a page. Set index to 0 (as a starting position)*/
	smoothDesc->currPos.firstHeapItem = 0;
	smoothDesc->currPos.lastHeapItem = itemIndex - 1;
	smoothDesc->currPos.itemHeapIndex = 0;
	LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	/* set first tuple for the found ones */
	if (itemIndex > 0) {

		/* OK, itemIndex says what to return */
		SmoothScanPosItem *currItem = &smoothDesc->currPos.items[smoothDesc->currPos.itemHeapIndex];

		//scan->xs_ctup.t_self = currItem->heapTid;
		scan->xs_ctup = *((HeapTuple) (smoothDesc->currTuples + currItem->tupleOffset));

		if (itemIndex > 1) {
			smoothDesc->more_data_for_smooth = true;
		} else {
			/* item is 1 - return it and make page as visited */
			smoothDesc->more_data_for_smooth = false;

			/* static array option */
			//smoothDesc->vispages[page] = (PageBitmap)1;
			/* bitmap set option */
			smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page);
			smoothDesc->num_vispages++;
			/* set next page to process in a sequential manner */
			if (smoothDesc->prefetch_pages)
				smoothDesc->nextPageId = page + 1;

			/* 3. "clear" currPos space */
			smoothDesc->currPos.firstHeapItem = 0;
			smoothDesc->currPos.lastHeapItem = 0;
			smoothDesc->currPos.itemHeapIndex = 0;
			/* initialize tuple workspace to empty */
			smoothDesc->currPos.nextTupleOffset = 0;

		}

		return &scan->xs_ctup;
	} else {

		ItemPointerSetInvalid(&(tuple->t_self));
		tuple->t_tableOid = InvalidOid;

		/* static array option */
		//smoothDesc->vispages[page] = (PageBitmap)1;
		/* bitmap set option */
		smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page);
		smoothDesc->num_vispages++;
		if (smoothDesc->prefetch_pages)
			smoothDesc->nextPageId = page + 1;
		return NULL;
	}
}

/* ----------------
 *		renata
 *		todo: Smooth Scan logic should go here !!!
 *		index_smoothfetch_heap - get the scan's next heap tuple
 *		 * return only one tuple, BUT
 * 1. check the rest of tuples from the page
 * 2. store them in 'result cache ' (hash map: key (block_id, page_id, offset), value - tuple)
 * 3. add (block_id, page_id) in 'Page_check' hash map: key (block_id, page_id), value - #tuples that still qualify
 *
 * For index probe:
 * 1. First check 'Page check' cache - if page is already checked
 * 	  If (Yes){
 * 	  	1.1 Get tuple from 'Result cache'
 * 	  	1.2 Remove it from 'Result cache'
 * 	  	1.3 Decrease # tuples in 'Page check' cache
 * 	  }
 * 	  else (No){
 * 	  	1.1 return tuple for which we probed index
 * 	  	1.2 Scan the rest of the page
 * 	  	1.3 Put result(s) in 'Result cache'
 * 	  	1.4 Increase 'Page check' #tuples - first time add one row
 * 	  }
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */

HeapTuple index_smoothfetch_heap(IndexScanDesc scan, ScanDirection direction, double plan_rows, int no_orig_keys,
		ScanKey orig_keys, List *target_list, List *qual_list, Index index, int no_orig_smooth_keys,
		ScanKey orig_smooth_keys, List *allqual, ExprContext *econtext, TupleTableSlot *slot) {
	ItemPointer tid = &scan->xs_ctup.t_self;

	bool all_dead = false;
	bool got_heap_tuple = false;
	bool used_prefetcher = false;
	BlockNumber page;
	HeapTuple tuple = &(scan->xs_ctup);

	int max_number_of_pages = 0; /*maximal number of pages for prefetcher */
	SmoothScanOpaque smoothDesc = (SmoothScanOpaque) scan->smoothInfo;

	/* prefetching logic should start only after smooth scan is on */
	if (smoothDesc->start_smooth && (!enable_smoothscan)) {
		//NEEDE FOR SELECTIVITY DRIVEN POLICY in case of aggegation
		if (!smoothDesc->start_prefetch &&
		/* number_tuples_switch = 0, meaning switch when actual cardinality is higher than estimated */
		((!num_tuples_prefetch && smoothDesc->prefetch_counter > plan_rows)
		/* number_tuples_prefetch > 0, meaning switch when actual cardinality is equal 'number_tuples_prefetch' */
		|| (num_tuples_prefetch && smoothDesc->prefetch_counter >= num_tuples_prefetch)

		)) {
			smoothDesc->start_prefetch = true;
			//set it again to 0
			smoothDesc->prefetch_counter = 0;

		}

		if (smoothDesc->orderby) {
			int hotRegion = 0;
			/* we have to process next page from prefetcher*/
			if (BlockNumberIsValid(smoothDesc->nextPageId)) {
				page = smoothDesc->nextPageId; /*to do check here as well if page <=*/
				/* next time it should be invalid */
				smoothDesc->nextPageId = InvalidBlockNumber;
				used_prefetcher = true;

			} else {
				/* not in the prefetching mode */
				/* when order by logic is similar but we are using result cache instead of bitmap*/
				/* if the page is in the cache - fill tuple with it*/
				/* NO ORDER BY - return all tuples from a page */
				page = ItemPointerGetBlockNumber(tid);
				//Maybe we are already outside of the partitionM

				if (smooth_resultcache_find_tuple(scan, tuple, page)) {

					smoothDesc->num_result_cache_hits++;
					return tuple;
				}
			}

			/*check FOR CASE WHEN WE HAVE UPDATES - SINCE UPDATES KEEP OLD POINTERS TO NOTHING*/
			if (bms_is_member(page, smoothDesc->bs_vispages) ) {
				int batchno;
				/*page is already considered and all the qualifying tuples are already produced */
				// Or maybe find tuple return false because we are out of boud of the current batch

//				if (smoothDesc->result_cache->status != SS_EMPTY) {
//					Page dp;
//					ItemId lpp;
//					HeapTuple tup = &(scan->xs_ctup);
//					ItemPointerData originalTupleID = tup->t_self;
//					bool valid;
//					OffsetNumber offset;
//
//					//LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
//					/* get page information */
//					dp = (Page) BufferGetPage(scan->xs_cbuf);
//					offset = originalTupleID.ip_posid;
//
//					lpp = PageGetItemId(dp,offset );
//					valid = false;
//					if (ItemIdIsNormal(lpp)) {
//						tup->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
//						tup->t_len = ItemIdGetLength(lpp);
//						tup->t_tableOid = scan->heapRelation->rd_id;
//						ItemPointerSet(&(tup->t_self), page, offset);
//					}
//
//					int batchno = -1;
//					print_tuple(RelationGetDescr(scan->indexRelation), tup);
//
//					ExecResultCacheGetBatch(scan, tup, &batchno);
//					if (smoothDesc->result_cache->curbatch != batchno) {
//
//						//	print_tuple(RelationGetDescr(scan->heapRelation),heapTuple);
//						ExecHashJoinNewBatch(scan, batchno);
//					}
//				}


				ItemPointerSetInvalid(&(tuple->t_self));
				tuple->t_tableOid = InvalidOid;

				return NULL;
			}

			if (!scan->xs_continue_hot) {
				/* Switch to correct buffer if we don't have it already */
				Buffer prev_buf = scan->xs_cbuf;

				scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, page);
				/*
				 * Prune page, but only if we weren't already on this page
				 */
				if (prev_buf != scan->xs_cbuf)
					heap_page_prune_opt(scan->heapRelation, scan->xs_cbuf, RecentGlobalXmin);

				/* check if we are using prefetcher, if yes now we are one step closer (the distance is smaller)*/
				if (smoothDesc->prefetch_pages > 0) {
					/* The main iterator has closed the distance by one page */
					smoothDesc->prefetch_pages--;
				}

			}

			/* number of pages for prefetcher*/
			/* if hash table exist */
			if (smoothDesc->result_cache->maxentries > 0)
				/* -1 because we are about to process one page which will increase number of entries */
				max_number_of_pages = smoothDesc->result_cache->maxtuples - smoothDesc->result_cache->nentries - 1;
			else
				/* hash table not yet created*/
				max_number_of_pages = smooth_prefetch_target;

			/*return tuple from a page */
			/*when hash is full no prefetching */
			if (smoothDesc->start_prefetch && smooth_prefetch_target && max_number_of_pages > 0
					&& !smoothDesc->prefetch_pages && (!used_prefetcher || enable_skewcheck)) {

				/*
				 * Increase prefetch target if it's not yet at the max.  Note that
				 * we will increase it to zero after fetching the very first
				 * page/tuple, then to one after the second tuple is fetched, then
				 * it doubles as later pages are fetched.
				 */

				//17.02.2014 - starting a new cycle
				//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
				//smoothDesc->local_num_pages = 0;
				//smoothDesc->local_qualifying_tuples = 0;
				if (num_tuples_prefetch >= 0) {

					//new 2014 - part with SKEW CHECK
					if (!enable_skewcheck || !used_prefetcher) {
						if (smoothDesc->prefetch_counter >= num_tuples_prefetch) {
							//reset it
							smoothDesc->prefetch_counter = 0;
							if (smoothDesc->prefetch_target >= smooth_prefetch_target)
								/* don't increase any further */;
							else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
								smoothDesc->prefetch_target = smooth_prefetch_target;
							else if (smoothDesc->prefetch_target > 0)
								smoothDesc->prefetch_target *= 2;
							else
								smoothDesc->prefetch_target++;
						} else {
							//nothing prefetcher stays the same
							;
						}
						if (enable_benchmarking)
							printf("\n Normal Smooth Scan. Number of pages checked %ld. Prefetch target %ld  ",
									smoothDesc->num_vispages, smoothDesc->prefetch_target);


//							printf("\n Normal Smooth Scan. Number of pages checked %ld. Prefetch target %ld  ",
//									bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						//17.02.2014 - starting a new cycle
						//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
						smoothDesc->local_num_pages = 0;
						smoothDesc->local_qualifying_pages = 0;
					} else {
						//17.02.2014
						//enable_skewcheck
						double localSelectivity;
						double globalSelectivity;

						if (smoothDesc->local_num_pages)
							localSelectivity = (double) smoothDesc->local_qualifying_pages
									/ (double) smoothDesc->local_num_pages;
						else
							localSelectivity = 0;

						if (bms_num_members(smoothDesc->bs_vispages))
							globalSelectivity = (double) smoothDesc->global_qualifying_pages
									/ (double) bms_num_members(smoothDesc->bs_vispages);
						else
							globalSelectivity = 0;

						if (localSelectivity > (1.4 * globalSelectivity)) {
							//we are in the hot region
//									if (smoothDesc->prefetch_target >= smooth_prefetch_target)
//										/* don't increase any further */ ;
//									else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
//										smoothDesc->prefetch_target = smooth_prefetch_target;
//									else if (smoothDesc->prefetch_target > 0)
//										smoothDesc->prefetch_target *= 2;
//									else
//										smoothDesc->prefetch_target++;
							hotRegion = 2;
							//if(enable_benchmarking)
							//	printf("\n Hot region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);
						} else if ((localSelectivity) < globalSelectivity) {
							//check if we are in the cold region
							if (smoothDesc->prefetch_target > 1)
								smoothDesc->prefetch_target /= 4; // because it will be increased next time prefetcher kicks in
							else if (smoothDesc->prefetch_target >= 1)
								smoothDesc->prefetch_target -= 2;
							else
								smoothDesc->prefetch_target = -1;
							hotRegion = 0;
							//if(enable_benchmarking)
							//	printf("\n Cold region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						} else {
							//stay the same
							;//nothing
							hotRegion = 1;

							//smoothDesc->prefetch_target /= 2;	// because it will be increased next time prefetcher kicks in
							//if(enable_benchmarking)
							//	printf("\n Region the same. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						}

//								//17.02.2014 - starting a new cycle
//								//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
//								smoothDesc->local_num_pages = 0;
//								smoothDesc->local_qualifying_tuples = 0;

					} //enable_skewcheck
				} else { //num_tuple_prefetch < 0 (start from the beginning)
						 //classical increasing procedure  num_tuples_prefetch = -1
					if (!enable_skewcheck || !used_prefetcher) {
						//classical increasing procedure
						if (smoothDesc->prefetch_target >= smooth_prefetch_target)
							/* don't increase any further */;
						else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
							smoothDesc->prefetch_target = smooth_prefetch_target;
						else if (smoothDesc->prefetch_target > 0)
							smoothDesc->prefetch_target *= 2;
						else
							smoothDesc->prefetch_target++;

						if (enable_benchmarking)
							printf("\n Normal Smooth Scan. Number of pages checked %ld. Prefetch target %ld  ",
									bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						//17.02.2014 - starting a new cycle
						//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
						smoothDesc->local_num_pages = 0;
						smoothDesc->local_qualifying_pages = 0;
					} else {
						//enable skewcheck
						//17.02.2014
						//enable_skewcheck
						double localSelectivity;
						double globalSelectivity;

						if (smoothDesc->local_num_pages)
							localSelectivity = (double) smoothDesc->local_qualifying_pages
									/ (double) smoothDesc->local_num_pages;
						else
							localSelectivity = 0;

						if (bms_num_members(smoothDesc->bs_vispages))
							globalSelectivity = (double) smoothDesc->global_qualifying_pages
									/ (double) bms_num_members(smoothDesc->bs_vispages);
						else
							globalSelectivity = 0;

						if (localSelectivity > (1.4 * globalSelectivity)) {
							//we are in the hot region
//									if (smoothDesc->prefetch_target >= smooth_prefetch_target)
//										 /* don't increase any further */ ;
//									else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
//										smoothDesc->prefetch_target = smooth_prefetch_target;
//									else if (smoothDesc->prefetch_target > 0)
//										smoothDesc->prefetch_target *= 2;
//									else
//										smoothDesc->prefetch_target++;
							hotRegion = 2;
							//if(enable_benchmarking)
							//	printf("\n Hot region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						} else if ((localSelectivity) < globalSelectivity) {
							//check if we are in the cold region
							if (smoothDesc->prefetch_target > 1)
								smoothDesc->prefetch_target /= 4; // because it will be increased next time prefetcher kicks in
							else if (smoothDesc->prefetch_target >= 1)
								smoothDesc->prefetch_target -= 2; // because it will be increased next time prefetcher kicks in
							else
								smoothDesc->prefetch_target = -1;
							hotRegion = 0;
							//if(enable_benchmarking)
							//	printf("\n Cold region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						} else {
							//stay the same
							;//nothing
							hotRegion = 1;
							//smoothDesc->prefetch_target /= 2;// because it will be increased next time prefetcher kicks in
							//if(enable_benchmarking)
							//	printf("\n Region the same. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

						}
						//17.02.2014 - starting a new cycle
						//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
						//smoothDesc->local_num_pages = 0;
						//smoothDesc->local_qualifying_tuples = 0;

					}

				}

				/* start prefetching */
				//new 2014
				//IF HOT REGION >1 DO PREFETCHER
				if (!enable_skewcheck || (enable_skewcheck && (hotRegion == 2 || !used_prefetcher))) {
					while (smoothDesc->prefetch_pages < smoothDesc->prefetch_target) {
						smoothDesc->prefetch_pages++;
						smoothDesc->prefetch_cumul++;
						BlockNumber nextPage = page + smoothDesc->prefetch_pages;

						/* prefetch pages only until end of existing relation = NO AFTER and only until you encounter visited page */
						//before enable_skewcheck
						//if(nextPage >= smoothDesc->rel_nblocks || bms_is_member(nextPage, smoothDesc->bs_vispages)){
						//17.02.2014
						if (nextPage >= smoothDesc->rel_nblocks || (bms_is_member(nextPage, smoothDesc->bs_vispages))) {
							//19.02.2014
							//if(nextPage >= smoothDesc->rel_nblocks ){
							/*stop with prefetching */
							smoothDesc->prefetch_pages--;
							smoothDesc->prefetch_cumul--;
							//17.02.2014
							//smoothDesc->prefetch_target/=1.5;
							break;
						}

					}
					/* if we have something at least one page the prefetcher */
					if (smoothDesc->prefetch_pages) {
						//printf("\nPrefetcher size in this go %ld",smoothDesc->prefetch_pages );
						//17.02.2014
						if (hotRegion == 2)
							smoothDesc->local_num_pages += smoothDesc->prefetch_pages;
						else
							smoothDesc->local_num_pages = smoothDesc->prefetch_pages;
						SmoothPrefetchBuffers(scan->heapRelation, MAIN_FORKNUM, (page + 1), smoothDesc->prefetch_pages);
					}
					if (hotRegion == 2) {
						smoothDesc->prefetch_target *= 2;
					}
				} //if !enable_skewcheck...

			}
			return SmoothProcessOnePageOrder(scan, page, direction, no_orig_keys, orig_keys, used_prefetcher,
					target_list, qual_list, index, no_orig_smooth_keys, orig_smooth_keys, allqual, econtext, slot);

		} /* if order by is imposed*/
		else {

			/* NO ORDER BY - return all tuples from a page */
			/* we have to process next page from prefetcher*/
			if (BlockNumberIsValid(smoothDesc->nextPageId)) {
				page = smoothDesc->nextPageId; /*to do check here as well if page <=*/
				/* next time it should be invalid */
				smoothDesc->nextPageId = InvalidBlockNumber;
				used_prefetcher = true;

			} else {
				/* get the block number where the tuple belongs */
				page = ItemPointerGetBlockNumber(tid);
			}

			/* with array */
			//if (smoothDesc->vispages[page]){
			/* with bitmap set */
			if (bms_is_member(page, smoothDesc->bs_vispages)) {
				/*page is already considered and all the qualifying tuples are already produced */

				ItemPointerSetInvalid(&(tuple->t_self));
				tuple->t_tableOid = InvalidOid;

				return NULL;
			} else {
				/* check if we have buffered tuples - if yes returned the next one directly */
				if (++smoothDesc->currPos.itemHeapIndex <= smoothDesc->currPos.lastHeapItem) {
					/* OK, itemHeapIndex says what to return */
					SmoothScanPosItem *currItem = &smoothDesc->currPos.items[smoothDesc->currPos.itemHeapIndex];

					//scan->xs_ctup.t_self = currItem->heapTid;

					scan->xs_ctup = *((HeapTuple) (smoothDesc->currTuples + currItem->tupleOffset));
					/* returning last one */
					if (smoothDesc->currPos.itemHeapIndex == smoothDesc->currPos.lastHeapItem) {
						/* this means that we have produced tuples from this page and we have come to the end */

						/* 1. put pageID in pageCache */
						/* mark the page as visited */

						/* static array option */
						//smoothDesc->vispages[page] = (PageBitmap)1;
						/* bitmap set option */
						smoothDesc->bs_vispages = bms_add_member(smoothDesc->bs_vispages, page);
						smoothDesc->num_vispages++;

						/* this page is done */
						smoothDesc->more_data_for_smooth = false;

						/* set next page to process in a sequential manner */
						if (smoothDesc->prefetch_pages)
							smoothDesc->nextPageId = page + 1;

						/* 2. put currPos buffer to be invalid */
						if (SmoothScanPosIsValid(smoothDesc->currPos)) {
							ReleaseBuffer(smoothDesc->currPos.buf);
							smoothDesc->currPos.buf = InvalidBuffer;
						}

						/* 3. "clear" currPos space */
						smoothDesc->currPos.firstHeapItem = 0;
						smoothDesc->currPos.lastHeapItem = 0;
						smoothDesc->currPos.itemHeapIndex = 0;
						/* initialize tuple workspace to empty */
						smoothDesc->currPos.nextTupleOffset = 0;
					}

					return &scan->xs_ctup;

				} else {
					/* we have nothing buffered, meaning we are accessing the page for the first time */
					/*
					 * 	 2.1. collect all tuples for a page that  qualify
					 * 	 2.2. put them in smoothDesc items
					 * 	 2.3. set lastHeapItem to the size, firstHeapItem to 0 and itemHeapIndex to 0
					 * 	 2.4. return first item from items
					 *
					 * 	 If page is in the cache
					 * 	 2.1. increase itemHeapIndex
					 * 	 2.2. check that itemHeapIndex is not greater than lastHeapItem
					 * 	 2.3. return next item */

					/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
					//if (!scan->xs_continue_hot && !((SmoothScanOpaque)scan->smoothInfo)->more_data_for_smooth)
					int hotRegion = 0;
					if (!scan->xs_continue_hot) {
						/* Switch to correct buffer if we don't have it already */
						Buffer prev_buf = scan->xs_cbuf;

						scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, page);
						/*
						 * Prune page, but only if we weren't already on this page
						 */
						if (prev_buf != scan->xs_cbuf)
							heap_page_prune_opt(scan->heapRelation, scan->xs_cbuf, RecentGlobalXmin);

						/* check if we are using prefetcher, if yes now we are one step closer (the distance is smaller)*/
						if (smoothDesc->prefetch_pages > 0) {
							/* The main iterator has closed the distance by one page */
							smoothDesc->prefetch_pages--;
						}

					}
					/*if we are doing prefetching, but for now we have no pages prefetched in the buffer pool
					 * ONLY THEN trigger prefetching */
					//17.02.2014 - if prefetcher has reached 0, but we are still in prefetching mode - this is what we need for skewcheck
					if (smoothDesc->start_prefetch && smooth_prefetch_target && !smoothDesc->prefetch_pages
							&& (!used_prefetcher || enable_skewcheck)) {

						/*
						 * Increase prefetch target if it's not yet at the max.  Note that
						 * we will increase it to zero after fetching the very first
						 * page/tuple, then to one after the second tuple is fetched, then
						 * it doubles as later pages are fetched.
						 */

						if (num_tuples_prefetch >= 0) {
							if (!enable_skewcheck || !used_prefetcher) {
								if (smoothDesc->prefetch_counter >= num_tuples_prefetch) {
									//reset it
									smoothDesc->prefetch_counter = 0;
									if (smoothDesc->prefetch_target >= smooth_prefetch_target)
										/* don't increase any further */;
									else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
										smoothDesc->prefetch_target = smooth_prefetch_target;
									else if (smoothDesc->prefetch_target > 0)
										smoothDesc->prefetch_target *= 2;
									else
										smoothDesc->prefetch_target++;
								} else {
									//nothing prefetcher stays the same
									;
								}
								if (enable_benchmarking)
									printf("\n Normal Smooth Scan. Number of pages checked %ld. Prefetch target %ld  ",
											bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								//17.02.2014 - starting a new cycle
								//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
								smoothDesc->local_num_pages = 0;
								smoothDesc->local_qualifying_pages = 0;
							} else {
								//17.02.2014
								//enable_skewcheck
								double localSelectivity;
								double globalSelectivity;

								if (smoothDesc->local_num_pages)
									localSelectivity = (double) smoothDesc->local_qualifying_pages
											/ (double) smoothDesc->local_num_pages;
								else
									localSelectivity = 0;

								if (bms_num_members(smoothDesc->bs_vispages))
									globalSelectivity = (double) smoothDesc->global_qualifying_pages
											/ (double) bms_num_members(smoothDesc->bs_vispages);
								else
									globalSelectivity = 0;

								if (localSelectivity > (1.4 * globalSelectivity)) {
									//we are in the hot region
//									if (smoothDesc->prefetch_target >= smooth_prefetch_target)
//										/* don't increase any further */ ;
//									else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
//										smoothDesc->prefetch_target = smooth_prefetch_target;
//									else if (smoothDesc->prefetch_target > 0)
//										smoothDesc->prefetch_target *= 2;
//									else
//										smoothDesc->prefetch_target++;
									hotRegion = 2;
									//if(enable_benchmarking)
									//	printf("\n Hot region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);
								} else if ((localSelectivity) < globalSelectivity) {
									//check if we are in the cold region
									if (smoothDesc->prefetch_target > 1)
										smoothDesc->prefetch_target /= 4; // because it will be increased next time prefetcher kicks in
									else if (smoothDesc->prefetch_target >= 1)
										smoothDesc->prefetch_target -= 2;
									else
										smoothDesc->prefetch_target = -1;
									hotRegion = 0;
									//if(enable_benchmarking)
									//	printf("\n Cold region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								} else {
									//stay the same
									;//nothing
									hotRegion = 1;

									//smoothDesc->prefetch_target /= 2;	// because it will be increased next time prefetcher kicks in
									//if(enable_benchmarking)
									//	printf("\n Region the same. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								}

//								//17.02.2014 - starting a new cycle
//								//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
//								smoothDesc->local_num_pages = 0;
//								smoothDesc->local_qualifying_tuples = 0;

							} //enable_skewcheck
						} else {
							if (!enable_skewcheck || !used_prefetcher) {
								//classical increasing procedure
								if (smoothDesc->prefetch_target >= smooth_prefetch_target)
									/* don't increase any further */;
								else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
									smoothDesc->prefetch_target = smooth_prefetch_target;
								else if (smoothDesc->prefetch_target > 0)
									smoothDesc->prefetch_target *= 2;
								else
									smoothDesc->prefetch_target++;

								if (enable_benchmarking)
									printf("\n Normal Smooth Scan. Number of pages checked %ld. Prefetch target %ld  ",
											bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								//17.02.2014 - starting a new cycle
								//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
								smoothDesc->local_num_pages = 0;
								smoothDesc->local_qualifying_pages = 0;
							} else {
								//enable skewcheck
								//17.02.2014
								//enable_skewcheck
								double localSelectivity;
								double globalSelectivity;

								if (smoothDesc->local_num_pages)
									localSelectivity = (double) smoothDesc->local_qualifying_pages
											/ (double) smoothDesc->local_num_pages;
								else
									localSelectivity = 0;

								if (bms_num_members(smoothDesc->bs_vispages))
									globalSelectivity = (double) smoothDesc->global_qualifying_pages
											/ (double) bms_num_members(smoothDesc->bs_vispages);
								else
									globalSelectivity = 0;

								if (localSelectivity > (1.4 * globalSelectivity)) {
									//we are in the hot region
//									if (smoothDesc->prefetch_target >= smooth_prefetch_target)
//										 /* don't increase any further */ ;
//									else if (smoothDesc->prefetch_target >= smooth_prefetch_target / 2)
//										smoothDesc->prefetch_target = smooth_prefetch_target;
//									else if (smoothDesc->prefetch_target > 0)
//										smoothDesc->prefetch_target *= 2;
//									else
//										smoothDesc->prefetch_target++;
									hotRegion = 2;
									//if(enable_benchmarking)
									//	printf("\n Hot region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								} else if ((localSelectivity) < globalSelectivity) {
									//check if we are in the cold region
									if (smoothDesc->prefetch_target > 1)
										smoothDesc->prefetch_target /= 4; // because it will be increased next time prefetcher kicks in
									else if (smoothDesc->prefetch_target >= 1)
										smoothDesc->prefetch_target -= 2; // because it will be increased next time prefetcher kicks in
									else
										smoothDesc->prefetch_target = -1;
									hotRegion = 0;
									//if(enable_benchmarking)
									//	printf("\n Cold region. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								} else {
									//stay the same
									;//nothing
									hotRegion = 1;
									//smoothDesc->prefetch_target /= 2;// because it will be increased next time prefetcher kicks in
									//if(enable_benchmarking)
									//	printf("\n Region the same. Number of pages checked %ld. Prefetch target %ld  ", bms_num_members(smoothDesc->bs_vispages), smoothDesc->prefetch_target);

								}
								//17.02.2014 - starting a new cycle
								//printf("\nLocal number of tuples for prefetcher size %ld, is %ld, ", smoothDesc->local_num_pages, smoothDesc->local_qualifying_tuples);
								//smoothDesc->local_num_pages = 0;
								//smoothDesc->local_qualifying_tuples = 0;

							}
						}

						/* start prefetching */
						//17.02.2014
						//IF HOT REGION >1 DO PREFETCHER
						if (!enable_skewcheck || (enable_skewcheck && (hotRegion == 2 || !used_prefetcher))) {
							while (smoothDesc->prefetch_pages < smoothDesc->prefetch_target) {
								smoothDesc->prefetch_pages++;
								smoothDesc->prefetch_cumul++;
								BlockNumber nextPage = page + smoothDesc->prefetch_pages;

								/* prefetch pages only until end of existing relation = NO AFTER and only until you encounter visited page */
								//before enable_skewcheck
								//if(nextPage >= smoothDesc->rel_nblocks || bms_is_member(nextPage, smoothDesc->bs_vispages)){
								//17.02.2014
								if (nextPage >= smoothDesc->rel_nblocks
										|| (bms_is_member(nextPage, smoothDesc->bs_vispages))) {
									//19.02.2014
									//if(nextPage >= smoothDesc->rel_nblocks ){
									/*stop with prefetching */
									smoothDesc->prefetch_pages--;
									smoothDesc->prefetch_cumul--;
									//17.02.2014
									//smoothDesc->prefetch_target/=1.5;
									break;
								}

							}
							/* if we have something at least one page the prefetcher */
							if (smoothDesc->prefetch_pages) {
								//printf("\nPrefetcher size in this go %ld",smoothDesc->prefetch_pages );
								//17.02.2014
								if (hotRegion == 2)
									smoothDesc->local_num_pages += smoothDesc->prefetch_pages;
								else
									smoothDesc->local_num_pages = smoothDesc->prefetch_pages;
								SmoothPrefetchBuffers(scan->heapRelation, MAIN_FORKNUM, (page + 1),
										smoothDesc->prefetch_pages);
							}
							if (hotRegion == 2) {
								smoothDesc->prefetch_target *= 2;
							}
						} //if !enable_skewcheck...

					}

					return SmoothProcessOnePage(scan, page, direction, no_orig_keys, orig_keys, no_orig_smooth_keys,
							orig_smooth_keys, allqual, econtext, slot);

				}/* end of else that puts new tuples in currPos.items */
			}/* end of if page is visited */
		}/* end of check whether we have order by or not */
	} else { /* not index smooth sorting - do a regular procedure */
		/* We can skip the buffer-switching logic if we're in mid-HOT chain. */

		/* get the block number where the tuple belongs */
		page = ItemPointerGetBlockNumber(tid);

		if (!scan->xs_continue_hot) {
			/* Switch to correct buffer if we don't have it already */
			Buffer prev_buf = scan->xs_cbuf;

			scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, page);

			/*
			 * Prune page, but only if we weren't already on this page
			 */
			if (prev_buf != scan->xs_cbuf)
				heap_page_prune_opt(scan->heapRelation, scan->xs_cbuf, RecentGlobalXmin);
		}
		/* fetch just one tuple*/
		LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
		got_heap_tuple = heap_hot_search_buffer(tid, scan->heapRelation, scan->xs_cbuf, scan->xs_snapshot,
				&scan->xs_ctup, &all_dead, !scan->xs_continue_hot);
		LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);
	}
	if (got_heap_tuple) {
		bool valid = false;
		//check if enable_filterpushdown is on
		//todo 2014
		if (!enable_filterpushdown) {
			valid = true;
		} //else filtering pushdown
		else {
			//check remaining predicates and only then return tuple if all predicates qualify
			//otherwise return NULL
			//filter pushdown
			//check whether other predicates qualify and only then return tuple
			if (allqual != NULL) {
				//renata:todo 2014 HeapKeyTest can cover only predicates of type Var op Const
				//for filter push down - we need to use allqual - these are all posible predicates on that table
				valid = false;
				ExecStoreTuple(&scan->xs_ctup, /* tuple to store */
				slot, /* slot to store in */
				InvalidBuffer, /* buffer containing tuple */
				false); /* don't pfree */

				econtext->ecxt_scantuple = slot;
				ResetExprContext(econtext);
				if (ExecQual(allqual, econtext, false)) {
					valid = true;
				}
			} // no quals
			else {
				/* if no keys to check - every tuple is valid*/
				valid = true;
			}

		} //end filtering pushdown
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		if (valid) {
			scan->xs_continue_hot = !IsMVCCSnapshot(scan->xs_snapshot);
			pgstat_count_heap_fetch(scan->indexRelation);
			smoothDesc->prefetch_counter++;
			smoothDesc->smooth_counter++;
			//17.02.2014
			//02.05.2014 todo chech this I expect it never enters here. If it enter with normal Smooth Procedure - it can mess up results
			//smoothDesc->global_qualifying_pages++;

			/* renata: add page to list of produced tuples */
			if (num_tuples_switch >= 0) {
				//old with bitmapset structure
				//			TID_SHORT formTid;
				//			BlockNumber blknum = ItemPointerGetBlockNumber(tid);
				//			form_tuple_id_short(&scan->xs_ctup, blknum, &formTid);
				//			smoothDesc->bs_vistuples = bms_add_member(smoothDesc->bs_vistuples, formTid);

				TID formTid;
				BlockNumber blknum = ItemPointerGetBlockNumber(tid);
				form_tuple_id(&scan->xs_ctup, blknum, &formTid);

				/* renata: here hash table is create TID is key, value is 0 -1 valid or not */
				//tbm_add_tuples(smoothDesc->tbm_vistuples, &scan->xs_ctup.t_self, 1, false);
				smooth_tuplecache_add_tuple(smoothDesc->tupleID_cache, formTid);
			}

			/* for Smooth Scan we have 3 policies:
			 * 1. num_tuples_switch = -1  - robustness policy - start immediately with Smooth Scan (as of the first tuple)
			 * 2. num_tuples_switch = 0   - cardinality driven policy - trigger Smooth when number of tuples is higher than estimated
			 * 3. num_tuples_switch = X   - SLA driven - start when the actual number of tuples is equal to X */
			if (enable_indexsmoothscan && !smoothDesc->start_smooth &&
			/* number_tuples_switch = 0, meaning switch when actual cardinality is higher than estimated */
			((!num_tuples_switch && smoothDesc->smooth_counter > plan_rows)
			/* number_tuples_switch > 0, meaning switch when actual cardinality is equal 'number_tuples_switch' */
			|| (num_tuples_switch && smoothDesc->smooth_counter >= num_tuples_switch)

			)) {
				smoothDesc->start_smooth = true;
				smoothDesc->smooth_counter = 0;

			}
			//parameter meaning is the same as with num_tuples_switch - but here we are turning on prefetcher
			if (enable_indexsmoothscan && !smoothDesc->start_prefetch &&
			/* number_tuples_switch = 0, meaning switch when actual cardinality is higher than estimated */
			((!num_tuples_prefetch && smoothDesc->prefetch_counter > plan_rows)
			/* number_tuples_prefetch > 0, meaning switch when actual cardinality is equal 'number_tuples_prefetch' */
			|| (num_tuples_prefetch && smoothDesc->prefetch_counter >= num_tuples_prefetch)

			)) {
				smoothDesc->start_prefetch = true;
				//set it again to 0
				smoothDesc->prefetch_counter = 0;
			}

			return &scan->xs_ctup;
		} //if valid
		else {
			//not valid return null
			//additional filtering didn't pass
			ItemPointerSetInvalid(&(scan->xs_ctup.t_self));
			scan->xs_ctup.t_tableOid = InvalidOid;

			return NULL;

		}
	} //if we got hot tuple

	/* We've reached the end of the HOT chain. */
	scan->xs_continue_hot = false;

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).	We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;

	return NULL;
}

/* ----------------
 *		index_getnext - get the next heap tuple from a scan
 *
 * The result is the next heap tuple satisfying the scan keys and the
 * snapshot, or NULL if no more matching tuples exist.
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
HeapTuple index_getnext(IndexScanDesc scan, ScanDirection direction) {
	HeapTuple heapTuple;
	ItemPointer tid;

	for (;;) {
		if (scan->xs_continue_hot) {
			/*
			 * We are resuming scan of a HOT chain after having returned an
			 * earlier member.	Must still hold pin on current heap page.
			 */
			Assert(BufferIsValid(scan->xs_cbuf));
			Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
		} else {
			/* Time to fetch the next TID from the index */
			tid = index_getnext_tid(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
				break;
		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */

		heapTuple = index_fetch_heap(scan);
		if (heapTuple != NULL)
			return heapTuple;
	}

	return NULL; /* failure exit */
}

/* ----------------
 * 	renata
 * 	TODO: here the logic comes - in the way how I fetch next tuple
 * 	This has to be changed
 *		index_getnext - get the next heap tuple from a scan
 *
 * The result is the next heap tuple satisfying the scan keys and the
 * snapshot, or NULL if no more matching tuples exist.
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
HeapTuple indexsmooth_getnext(IndexScanDesc scan, ScanDirection direction, double plan_rows, int no_orig_keys,
		ScanKey orig_keys, List *target_list, List *qual_list, Index index, int no_orig_smooth_keys,
		ScanKey orig_smooth_keys, List *allqual, ExprContext *econtext, TupleTableSlot *slot) {
	HeapTuple heapTuple;
	ItemPointer tid;
	SmoothScanOpaque sso = (SmoothScanOpaque) scan->smoothInfo;

	//int batchno;

	for (;;) { /* we are either processing current page, or have more pages in prefetcher
	 In any case, I should not fetch next TID from the index, until I don't consume all prefetched pages
	 */
		if (scan->xs_continue_hot || ((SmoothScanOpaque) scan->smoothInfo)->more_data_for_smooth
				|| ((SmoothScanOpaque) scan->smoothInfo)->prefetch_pages) {
			/*
			 * We are resuming scan of a HOT chain after having returned an
			 * earlier member.	Must still hold pin on current heap page.
			 */
			if (scan->xs_continue_hot) {
				Assert(BufferIsValid(scan->xs_cbuf));
				Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
			}
		} else {
			HeapTuple tuple;
			Buffer buf =  InvalidBuffer;
			/* Time to fetch the next TID from the index */
			tid = index_getnext_tid(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
					break;

			if (!visibilitymap_test(scan->heapRelation,
											ItemPointerGetBlockNumber(tid),
											&buf)) {
				/*
				 * Rats, we have to visit the heap to check visibility.
				 */

				tuple = index_fetch_heap(scan);
				/* no visible tuple, try next index entry */

			}





		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */
		/* renata
		 * in this method we will fetch heaptuple -but we will also fill scan->smoothdescriptor->items with the rest of tuples
		 *
		 * */
		//	print_tuple(RelationGetDescr(scan->indexRelation), scan->xs_itup);
				ExecResultCacheSwitchPartition(scan,sso,scan->xs_itup);
		heapTuple = index_smoothfetch_heap(scan, direction, plan_rows, no_orig_keys, orig_keys, target_list, qual_list,
				index, no_orig_smooth_keys, orig_smooth_keys, allqual, econtext, slot);
		if (heapTuple != NULL) {

			sso->num_result_tuples++;
			return heapTuple;
		}
	}

	return NULL; /* failure exit */
}
/* ----------------
 *		smoothscan_getnext - get the next heap tuple from a scan
 *
 * The result is the next heap tuple satisfying the scan keys and the
 * snapshot, or NULL if no more matching tuples exist.
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
HeapTuple smoothscan_getnext(IndexScanDesc scan, ScanDirection direction) {
	HeapTuple heapTuple;
	ItemPointer tid;

	for (;;) {
		if (scan->xs_continue_hot) {
			/*
			 * We are resuming scan of a HOT chain after having returned an
			 * earlier member.	Must still hold pin on current heap page.
			 */
			Assert(BufferIsValid(scan->xs_cbuf));
			Assert(ItemPointerGetBlockNumber(&scan->xs_ctup.t_self) == BufferGetBlockNumber(scan->xs_cbuf));
		} else {
			/* Time to fetch the next TID from the index */

			tid = smoothscan_getnext_tid(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
				break;
		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */
		/* renata todo:
		 * return only this tuple, BUT
		 * 1. check the rest of tuples from the page
		 * 2. store them in 'result cache ' (hash map: key (block_id, page_id, offset), value - tuple)
		 * 3. add (block_id, page_id) in 'Page_check' hash map: key (block_id, page_id), value - #tuples that still qualify
		 *
		 * For index probe:
		 * 1. First check 'Page check' cache - if page is already checked
		 * 	  If (Yes){
		 * 	  	1.1 Get tuple from 'Result cache'
		 * 	  	1.2 Remove it from 'Result cache'
		 * 	  	1.3 Decrease # tuples in 'Page check' cache
		 * 	  }
		 * 	  else (No){
		 * 	  	1.1 return tuple for which we probed index
		 * 	  	1.2 Scan the rest of the page
		 * 	  	1.3 Put result(s) in 'Result cache'
		 * 	  	1.4 Increase 'Page check' #tuples - first time add one row
		 * 	  }
		 *  */

		heapTuple = index_smoothfetch_heap(scan, direction, 0, 0, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL);
		if (heapTuple != NULL) {
			((SmoothScanOpaque) scan->smoothInfo)->num_result_tuples++;
			return heapTuple;
		}
	}

	return NULL; /* failure exit */
}

/* ----------------
 *		index_getbitmap - get all tuples at once from an index scan
 *
 * Adds the TIDs of all heap tuples satisfying the scan keys to a bitmap.
 * Since there's no interlock between the index scan and the eventual heap
 * access, this is only safe to use with MVCC-based snapshots: the heap
 * item slot could have been replaced by a newer tuple by the time we get
 * to it.
 *
 * Returns the number of matching tuples found.  (Note: this might be only
 * approximate, so it should only be used for statistical purposes.)
 * ----------------
 */
int64 index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap) {
	FmgrInfo *procedure;
	int64 ntids;
	Datum d;

	SCAN_CHECKS;
	GET_SCAN_PROCEDURE(amgetbitmap);

	/* just make sure this is false... */
	scan->kill_prior_tuple = false;

	/*
	 * have the am's getbitmap proc do all the work.
	 */
	d = FunctionCall2(procedure,
			PointerGetDatum(scan),
			PointerGetDatum(bitmap));

	ntids = DatumGetInt64(d);

	/* If int8 is pass-by-ref, must free the result to avoid memory leak */
#ifndef USE_FLOAT8_BYVAL
	pfree(DatumGetPointer(d));
#endif

	pgstat_count_index_tuples(scan->indexRelation, ntids);

	return ntids;
}

/* ----------------
 *		index_bulk_delete - do mass deletion of index entries
 *
 *		callback routine tells whether a given main-heap tuple is
 *		to be deleted
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_bulk_delete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
		void *callback_state) {
	Relation indexRelation = info->index;
	FmgrInfo *procedure;
	IndexBulkDeleteResult *result;

	RELATION_CHECKS;
	GET_REL_PROCEDURE(ambulkdelete);

	result = (IndexBulkDeleteResult *) DatumGetPointer(FunctionCall4(procedure,
					PointerGetDatum(info),
					PointerGetDatum(stats),
					PointerGetDatum((Pointer) callback),
					PointerGetDatum(callback_state)));

	return result;
}

/* ----------------
 *		index_vacuum_cleanup - do post-deletion cleanup of an index
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_vacuum_cleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) {
	Relation indexRelation = info->index;
	FmgrInfo *procedure;
	IndexBulkDeleteResult *result;

	RELATION_CHECKS;
	GET_REL_PROCEDURE(amvacuumcleanup);

	result = (IndexBulkDeleteResult *) DatumGetPointer(FunctionCall2(procedure,
					PointerGetDatum(info),
					PointerGetDatum(stats)));

	return result;
}

/* ----------------
 *		index_can_return - does index support index-only scans?
 * ----------------
 */
bool index_can_return(Relation indexRelation) {
	FmgrInfo *procedure;

	RELATION_CHECKS;

	/* amcanreturn is optional; assume FALSE if not provided by AM */
	if (!RegProcedureIsValid(indexRelation->rd_am->amcanreturn))
		return false;

	GET_REL_PROCEDURE(amcanreturn);

	return DatumGetBool(FunctionCall1(procedure,
					PointerGetDatum(indexRelation)));
}

/* ----------------
 *		index_getprocid
 *
 *		Index access methods typically require support routines that are
 *		not directly the implementation of any WHERE-clause query operator
 *		and so cannot be kept in pg_amop.  Instead, such routines are kept
 *		in pg_amproc.  These registered procedure OIDs are assigned numbers
 *		according to a convention established by the access method.
 *		The general index code doesn't know anything about the routines
 *		involved; it just builds an ordered list of them for
 *		each attribute on which an index is defined.
 *
 *		As of Postgres 8.3, support routines within an operator family
 *		are further subdivided by the "left type" and "right type" of the
 *		query operator(s) that they support.  The "default" functions for a
 *		particular indexed attribute are those with both types equal to
 *		the index opclass' opcintype (note that this is subtly different
 *		from the indexed attribute's own type: it may be a binary-compatible
 *		type instead).	Only the default functions are stored in relcache
 *		entries --- access methods can use the syscache to look up non-default
 *		functions.
 *
 *		This routine returns the requested default procedure OID for a
 *		particular indexed attribute.
 * ----------------
 */
RegProcedure index_getprocid(Relation irel, AttrNumber attnum, uint16 procnum) {
	RegProcedure *loc;
	int nproc;
	int procindex;

	nproc = irel->rd_am->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	loc = irel->rd_support;

	Assert(loc != NULL);

	return loc[procindex];
}

/* ----------------
 *		index_getprocinfo
 *
 *		This routine allows index AMs to keep fmgr lookup info for
 *		support procs in the relcache.	As above, only the "default"
 *		functions for any particular indexed attribute are cached.
 *
 * Note: the return value points into cached data that will be lost during
 * any relcache rebuild!  Therefore, either use the callinfo right away,
 * or save it only after having acquired some type of lock on the index rel.
 * ----------------
 */
FmgrInfo *
index_getprocinfo(Relation irel, AttrNumber attnum, uint16 procnum) {
	FmgrInfo *locinfo;
	int nproc;
	int procindex;

	nproc = irel->rd_am->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	locinfo = irel->rd_supportinfo;

	Assert(locinfo != NULL);

	locinfo += procindex;

	/* Initialize the lookup info if first time through */
	if (locinfo->fn_oid == InvalidOid) {
		RegProcedure *loc = irel->rd_support;
		RegProcedure procId;

		Assert(loc != NULL);

		procId = loc[procindex];

		/*
		 * Complain if function was not found during IndexSupportInitialize.
		 * This should not happen unless the system tables contain bogus
		 * entries for the index opclass.  (If an AM wants to allow a support
		 * function to be optional, it can use index_getprocid.)
		 */
		if (!RegProcedureIsValid(procId))
			elog(ERROR, "missing support function %d for attribute %d of index \"%s\"", procnum, attnum,
					RelationGetRelationName(irel));

		fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);
	}

	return locinfo;
}
