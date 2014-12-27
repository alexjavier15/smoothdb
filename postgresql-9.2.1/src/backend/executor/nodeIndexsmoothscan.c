/*-------------------------------------------------------------------------
 * renata
 * nodeIndexsmoothscan.c
 *	  Routines to support indexed scans of relations
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeIndexsmoothscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecIndexSmoothScan			scans a relation using an index
 *		IndexSmoothNext				retrieve next tuple using index
 *		ExecInitIndexSmoothScan		creates and initializes state info.
 *		ExecReScanIndexSmoothScan	rescans the indexed relation.
 *		ExecEndIndexSmoothScan		releases all storage.
 *		ExecIndexSmoothMarkPos		marks scan position.
 *		ExecIndexSmoothRestrPos		restores scan position.
 */
#include "postgres.h"

#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "executor/execdebug.h"
#include "executor/nodeIndexsmoothscan.h"
#include "optimizer/clauses.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "miscadmin.h"
#include "optimizer/cost.h"

static TupleTableSlot *IndexSmoothNext(IndexSmoothScanState *node);

/* renata
 * decladation of additional methods for index smooth scan
 * (mostly for dealing with result hash table)
 *
 * */
//ResultCache *
//smooth_resultcache_create_empty(long maxbytes);
//
//static void
//smooth_resultcache_create(ResultCache *res_cache);
//
//void
//smooth_resultcache_free(ResultCache *cache);
//
//
//static ResultCacheEntry *
//smooth_resultcache_get_resultentry(ResultCache *cache, BlockNumber pageno);
//
//static  ResultCacheEntry *
//smooth_resultcache_find_resultentry(ResultCache *cache, BlockNumber pageno);
/***************************************************************************************/

//bool is_target_attribute(Form_pg_attribute att, List *target_list)
//{
//	ListCell   *tl;
//
//	foreach(tl, target_list)
//	{
//		GenericExprState *gstate = (GenericExprState *) lfirst(tl);
//		Var		   *variable = (Var *) gstate->arg->expr;
//
//		if (variable != NULL &&
//			IsA(variable, Var) &&
//			variable->varattno > 0 && variable->varattno == (AttrNumber)att->attnum)
//		{
//			return true;
//		}
//
//	}
//	return false;
//}
#define  is_target_attribute(att, target_list, found) \
ListCell   *tl; \
List 	* __target_list = (target_list); \
Form_pg_attribute _att = (att); \
 \
 (found) = false; \
foreach(tl, __target_list) \
{ \
	GenericExprState *gstate = (GenericExprState *) lfirst(tl); \
	Var		   *variable = (Var *) gstate->arg->expr; \
 \
	if (variable->varattno == (AttrNumber)_att->attnum) \
		/*(variable != NULL &&*/ \
		/*IsA(variable, Var) &&*/ \
		/* variable->varattno > 0 &&*/ \
	{ \
		(found) =  true; \
		break ; \
	} \
 \
}

//bad code!!!
bool is_qual_attribute(Form_pg_attribute att, List *qual_list) {
	ListCell *tl;

	foreach(tl, qual_list) {
		ExprState *exprstate = (ExprState *) lfirst(tl);
		Var *variable = (Var *) exprstate->expr;

		if (variable != NULL && IsA(variable, Var) && variable->varattno > 0
				&& variable->varattno == (AttrNumber) att->attnum) {
			return true;
		}

	}
	return false;
}
//
//#define  is_qual_attribute(att, qual_list, found) \
//ListCell   *tl2; \
//List 	* __qual_list = (qual_list); \
//Form_pg_attribute _search_att = (att); \
// \
// (found) = false; \
//foreach(tl2, __qual_list) \
//{ \
//	GenericExprState *gstate = (GenericExprState *) lfirst(tl2); \
//	Var		   *variable = (Var *) gstate->arg->expr; \
// \
//	if (variable->varattno == (AttrNumber)_search_att->attnum) \
//		/*(variable != NULL &&*/ \
//		/*IsA(variable, Var) &&*/ \
//		/* variable->varattno > 0 &&*/ \
//	{ \
//		(found) =  true; \
//		break ; \
//	} \
// \
//}

//static
//TID form_tuple_id(HeapTuple tpl, BlockNumber blk);

ResultCache *
smooth_resultcache_create_empty(long maxbytes, int numatt);
TupleIDCache *
smooth_tuplecache_create_empty();

static void
smooth_resultcache_create(ResultCache *res_cache, uint32 tup_length);

void
smooth_resultcache_free(ResultCache *cache);
void
smooth_tuplecache_free(TupleIDCache *cache);

static ResultCacheEntry *
smooth_resultcache_get_resultentry(ResultCache *cache, HeapTuple tpl, BlockNumber blknum);

static ResultCacheEntry *
smooth_resultcache_find_resultentry(ResultCache *cache, ResultCacheKey tid);

HeapTuple project_tuple(const HeapTuple tuple, const TupleDesc tupleDesc, List *target_list, List *qual_list,
		Index index, Datum *values, bool * isnull);
//
///*Smooth Operators
// * Result Cache methods*/
///* renata
// * This function only prepares for creating hash_table, while the hash table is actually created
// * in the function smooth_create_resultcache */
//
//ResultCache *
//smooth_resultcache_create_empty(long maxbytes)
//{
//	ResultCache  *result;
//	long		nbuckets;
//
//	/* Create ResultCache*/
//	result = makeNode(ResultCache);
//
//	result->mcxt = CurrentMemoryContext;
//	result->status = SS_EMPTY;
//
//	/*
//	 * Estimate number of hashtable entries we can have within maxbytes. This
//	 * estimates the hash overhead at MAXALIGN(sizeof(HASHELEMENT)) plus a
//	 * pointer per hash entry, which is crude but good enough for our purpose.
//	 * Also count an extra Pointer per entry for the arrays created during
//	 * iteration readout.
//	 */
//	/* to do - I should calculate the size of ResultCacheEntry by hand */
//	nbuckets = maxbytes /
//		(MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(sizeof(ResultCacheEntry))
//		 + sizeof(Pointer) + sizeof(Pointer));
//	nbuckets = Min(nbuckets, INT_MAX - 1);		/* safety limit */
//	nbuckets = Max(nbuckets, 16);		/* sanity limit */
//	result->maxentries = (int) nbuckets;
//
//	return result;
//}
//
///*
// *
// * Actually create the hashtable.  Since this is a moderately expensive
// * proposition, we don't do it until we have to.
// */
//
//static void
//smooth_resultcache_create(ResultCache *res_cache)
//{
//	HASHCTL		hash_ctl;
//
//	Assert(res_cache != NULL);
//
//	/* Create the hashtable proper */
//	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
//	hash_ctl.keysize = sizeof(BlockNumber);
//	hash_ctl.entrysize = sizeof(ResultCacheEntry);
//	hash_ctl.hash = tag_hash;
//	hash_ctl.hcxt = res_cache->mcxt;
//	res_cache->hashtable = hash_create("ResultCache",
//											 128,	/* start small and extend */
//											 &hash_ctl,
//											 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_SMOOTH);
//
//
//	res_cache->status = SS_HASH;
//}
//
///*
// * tbm_add_tuples - add some tuple IDs to a TIDBitmap
// *
// * If recheck is true, then the recheck flag will be set in the
// * TBMIterateResult when any of these tuples are reported out.
// */
//
//
//bool
//smooth_resultcache_add_tuple(ResultCache *cache, const BlockNumber blk, const OffsetNumber off, const HeapTuple tuple)
//{
//		ResultCacheEntry *page;
//		Size tupleSize;
//
//		/* safety check to ensure we don't overrun bit array bounds */
//		if (off < 1 || off > MaxHeapTuplesPerPage)
//			elog(ERROR, "tuple offset out of range: %u", off);
//
//
//		page = smooth_resultcache_get_resultentry(cache, blk);
//		if(page){
//			/* add tuple */
//			/* 1. get tuple size */
//			tupleSize = tuple->t_len;
//
//			/* 2. mark the offset of this tuple*/
//			page->tupleInfo[page->numTuples].tupleOffset = page->nextTupleOffset;
//			page->tupleInfo[page->numTuples].heapTID = tuple->t_self;
//
//			/* 3. copy tuple in the page cache (this is a bucket for the hash table)*/
//			memcpy(page->tuples + page->nextTupleOffset, tuple->t_data, tupleSize);
//			/* 4. note where next tuple should start */
//			page->nextTupleOffset += MAXALIGN(tupleSize);
//
//			/* 5. increase number of tuples for a page*/
//			page->numTuples++;
//			return true;
//		}else{
//			return false;
//		}
//
//}
//
//
//bool
//smooth_resultcache_find_tuple(ResultCache *cache, HeapTuple tuple)
//{
//	BlockNumber blk;
//	ResultCacheEntry *pageCache;
//	bool found = false;
//	int i;
//
//
//	blk = ItemPointerGetBlockNumber(&(tuple->t_self));
//	pageCache = smooth_resultcache_find_resultentry(cache, blk);
//
//	/* if we have a bucket for this block */
//	if (pageCache){
//		for(i = 0; i < pageCache->numTuples; i++){
//			/* if we have that tuple in the cache */
//			if(pageCache->tupleInfo[i].heapTID.ip_posid == tuple->t_self.ip_posid){
//
//				tuple->t_data = ((HeapTupleHeader) (pageCache->tuples + pageCache->tupleInfo[i].tupleOffset));
//				found = true;
//				break;
//			}
//		}
//	}
//	return found;
//}
///* This method returns ResultCacheEntry if exists, if not NULL is returned */
//
//static  ResultCacheEntry *
//smooth_resultcache_find_resultentry(ResultCache *cache, BlockNumber pageno)
//{
//	ResultCacheEntry *page;
//
//	if (cache->nentries == 0)		/* in case pagetable doesn't exist */
//		return NULL;
//
//	if (cache->status == SS_EMPTY)
//	{
//		return NULL;
//	}
//
//	page = (ResultCacheEntry *) hash_search(cache->hashtable,
//										  (void *) &pageno,
//										  HASH_FIND, NULL);
//
//	return page;
//}
//
// /* This method returns ResultCacheEntry if exists, if not new one is created and returned */
//
///*
// * This may cause the table to exceed the desired memory size.
// */
//static ResultCacheEntry *
//smooth_resultcache_get_resultentry(ResultCache *cache, BlockNumber pageno)
//{
//	ResultCacheEntry *page;
//	bool		found;
//
//	if(cache->status == SS_EMPTY){
//		smooth_resultcache_create(cache);
//	}
//	if(cache->status == SS_HASH){
//		/* Look up or create an entry */
//		page = (ResultCacheEntry *) hash_search(cache->hashtable,
//												  (void *) &pageno,
//												  HASH_ENTER, &found);
//	}else{
//		/* either last or full */
//		/* WE CANNOT CREATE ADD MORE PAGES - so we can only add tuples to existing pages */
//		page = (ResultCacheEntry *) hash_search(cache->hashtable,
//												  (void *) &pageno,
//												  HASH_FIND, &found);
//	}
//	/*checking if hash table is full*/
//	if(page){
//		/* Initialize it if not present before */
//		if (!found)
//		{
//			MemSet(page, 0, sizeof(ResultCacheEntry));
//			page->blockID = pageno;
//			page->numTuples = 0;
//			page->nextTupleOffset = 0;
//			/* must count it too */
//			cache->nentries++;
//			cache->npages++;
//			if (cache->npages == cache->maxentries){
//				printf("\nNO MORE PAGES ARE SUPPOSED TO BE ADDED IN THE CACHE. FULL! \n ");
//				cache->status = SS_FULL;
//			}
//		}
//	}else{
//		printf("\nHash table is full!\n");
//		cache->status = SS_FULL;
//	}
//
//	return page;
//}

/*
 * smooth_free_result_cache - free ResultCache
 */
void smooth_resultcache_free(ResultCache *cache) {
	if (enable_benchmarking || enable_smoothnestedloop)
		if (cache->nentries)
			printf("\n Number of entries is %d, max is %d", cache->nentries, cache->maxentries);

	pfree(cache->projected_values);
	pfree(cache->projected_isnull);
	if (!enable_smoothshare) {
		if (cache->hashtable)
			hash_destroy(cache->hashtable);

		pfree(cache);
	}
}

/*
 * smooth_free_result_cache - free ResultCache
 */
void smooth_tuplecache_free(TupleIDCache *cache) {

	if (cache->hashtable)
		hash_destroy(cache->hashtable);

	pfree(cache);
}

//TID form_tuple_id(HeapTuple tpl, BlockNumber blknum)
//{
//	//TID tid =  (blknum << 16) | tpl->t_self.ip_posid;
//	TID temp = (TID) blknum;
//	TID tid = (TID) ((temp << 32) | ((uint32)tpl->t_self.ip_posid));
//
//	//printf("\n Blok is: %u, Offset: %u, TID: %lu \n", blknum, tpl->t_self.ip_posid, tid);
//	return tid;
//}

/****************************************************************************************************
 /**SOLUTION WHERE ONE KEY = TID, VALUE = TUPLE */
/*Smooth Operators
 * Result Cache methods*/
/* renata
 * This function only prepares for creating hash_table, while the hash table is actually created
 * in the function smooth_create_resultcache */

ResultCache *
smooth_resultcache_create_empty(long maxbytes, int numatt) {
	ResultCache *result;
	bool found;
	MemoryContext oldctx = CurrentMemoryContext;
	/* Create ResultCache*/
	if (enable_smoothshare) {
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		//Need initialize name properly

		result = ShmemInitStruct("Result Cache", sizeof(ResultCache), &found);
		result->type = T_ResultCache;

	} else
		result = makeNode(ResultCache);

	if (!found) {
		result->mcxt = CurrentMemoryContext;
		result->size = maxbytes;
		result->isCached = false;
	}
	result->status = SS_EMPTY;
	result->isCached = found;
	/*space for projection game */
	MemoryContextSwitchTo(oldctx);
	result->projected_values = (Datum *) palloc(numatt * sizeof(Datum));
	result->projected_isnull = (bool *) palloc(numatt * sizeof(bool));

	Assert(result!=NULL);
	return result;
}
TupleIDCache *
smooth_tuplecache_create_empty() {
	HASHCTL hash_ctl;
	TupleIDCache *tupleCache;
	tupleCache = makeNode(TupleIDCache);
	tupleCache->status = SS_EMPTY;

	/* Create the hashtable proper */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(TID);
	hash_ctl.entrysize = sizeof(TupleIDCacheEntry);
	hash_ctl.hash = tag_hash;
	hash_ctl.hcxt = CurrentMemoryContext;
	tupleCache->hashtable = hash_create("TIDCache", 128, /* start small and extend */
	&hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	tupleCache->nentries = 0;
	return tupleCache;
}
/*
 *
 * Actually create the hashtable.  Since this is a moderately expensive
 * proposition, we don't do it until we have to.
 */

static void smooth_resultcache_create(ResultCache *res_cache, uint32 tup_length) {
	HASHCTL hash_ctl;
	HASHCTL *hash_ctl_ptr;
	bool found;
	MemoryContext oldctx = CurrentMemoryContext;
	int hash_tag = HASH_ELEM | HASH_FUNCTION | HASH_SMOOTH;

	long nbuckets;

	Assert(res_cache != NULL);
	if (enable_smoothshare) {
		oldctx = MemoryContextSwitchTo(TopMemoryContext);

		hash_ctl_ptr = ShmemInitStruct("Result HASHCTL", sizeof(HASHCTL), &found);
		if (found)
			printf("\nHASHCTL struct for result cache was found\n");
	} else {
		hash_ctl_ptr = &hash_ctl;
		MemSet(hash_ctl_ptr, 0, sizeof(hash_ctl));
	}
	/*
	 * Estimate number of hashtable entries we can have within maxbytes. This
	 * estimates the hash overhead at MAXALIGN(sizeof(HASHELEMENT)) plus a
	 * pointer per hash entry, which is crude but good enough for our purpose.
	 * Also count an extra Pointer per entry for the arrays created during
	 * iteration readout.
	 */
	/* to do - I should calculate the size of ResultCacheEntry by hand */

//	nbuckets = res_cache->size /n
//		(MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(sizeof(ResultCacheEntry) + (tup_length))
//		 + sizeof(Pointer) + sizeof(Pointer));
//this one works
	nbuckets = res_cache->size
			/ (MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(sizeof(ResultCacheKey)+ (tup_length)) + sizeof(Pointer)
					+ sizeof(Pointer));

//	//this is just try for tpch
//	nbuckets = res_cache->size /
//				(MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(sizeof(ResultCacheKey))  + HEAPTUPLESIZE + MAXALIGN(tup_length)
//				 + sizeof(Pointer) + sizeof(Pointer));

	nbuckets = Min(nbuckets, INT_MAX - 1); /* safety limit */
	nbuckets = Max(nbuckets, 16); /* sanity limit */

	res_cache->maxentries = (int) nbuckets;
	res_cache->nentries = 0;
	res_cache->tuple_length = (tup_length);
	//if (enable_benchmarking || enable_smoothnestedloop)
	printf("\nmax number of entries in hash table is %ld\n", nbuckets);

	/* Create the hashtable proper */

//	Size entry = sizeof(ResultCacheEntry)+ (tup_length);
//this one works!!!
	Size entry = MAXALIGN(sizeof(ResultCacheKey)+ (tup_length));

	//this is just a try for tpch
	//Size entry= MAXALIGN(sizeof(ResultCacheKey))  + HEAPTUPLESIZE + MAXALIGN(tup_length);
	if (enable_benchmarking || enable_smoothnestedloop)
		printf("\nhash table entry size is %d\n", entry);
	//printf("\n Size of result cache entry is %d, tuple length %d \n", sizeof(ResultCacheEntry), tup_length);
	if (!found) {
		hash_ctl_ptr->keysize = sizeof(ResultCacheKey);
		hash_ctl_ptr->entrysize = entry;
		hash_ctl_ptr->hash = tag_hash;
		hash_ctl_ptr->hcxt = res_cache->mcxt;
	}

	Assert(hash_ctl_ptr!=NULL);
	if (!enable_smoothshare) {
		hash_tag |= HASH_CONTEXT;
		res_cache->hashtable = hash_create("ResultCache Hash", 128, /* start small and extend */
		hash_ctl_ptr, hash_tag);
	} else {
		if (!found)
			IsUnderPostmaster = true;
		res_cache->hashtable = ShmemInitHash("ResultCache Hash", res_cache->maxentries,
				res_cache->maxentries, hash_ctl_ptr, hash_tag);
		IsUnderPostmaster = false;
	}
	MemoryContextSwitchTo(oldctx);
	res_cache->nentries = hash_get_num_entries(res_cache->hashtable);
	res_cache->status = SS_HASH;
}

//renata: add tuple id in tuple cache
//this is used to remember tuples obtained in stage 1 of Smooth Scan
// these are tuples obtained by classical index  - and we should avoid producing them again in other stages

bool smooth_tuplecache_add_tuple(TupleIDCache * cache, const TID tupleID) {
	TupleIDCacheEntry * resultEntry = NULL;
	bool found = false, inserted = false;
	resultEntry = (TupleIDCacheEntry *) hash_search(cache->hashtable, (void *) &tupleID, HASH_ENTER, &found);
	if (resultEntry != NULL) {
		resultEntry->tid = tupleID;
		resultEntry->valid = (uint8) 1;
		inserted = true;
		cache->nentries++;
	}
	return inserted;
}

/* PROJECTION
 */

bool smooth_resultcache_add_tuple(SmoothScanOpaque ss, const BlockNumber blknum, const OffsetNumber off,
		const HeapTuple tpl, const TupleDesc tupleDesc, List *target_list, List *qual_list, Index index,
		bool *pageHasOneResultTuple) {
	ResultCacheEntry *resultEntry = NULL;
	bool inserted = false;
	/* safety check to ensure we don't overrun bit array bounds */
	if (off < 1 || off > MaxHeapTuplesPerPage)
		elog(ERROR, "tuple offset out of range: %u", off);

	//todo - see if i can change existing tuple here -not creating a new one
	HeapTuple projectedTuple = project_tuple(tpl, tupleDesc, target_list, qual_list, index,
			ss->result_cache->projected_values, ss->result_cache->projected_isnull);

	resultEntry = smooth_resultcache_get_resultentry(ss->result_cache, projectedTuple, blknum);

	if (resultEntry != NULL) {

		//heap_copytuple_into_hash(tpl, &resultEntry->tuple);
		//heap_copytuple_with_tuple(tpl, &resultEntry->tuple);
		//resultEntry->tuple = heap_copytuple(tpl);
		/* old version that worked before */
		//memcpy((char *) &resultEntry->tuple_data, (char *) tpl->t_data, tpl->t_len);
		//TODO
		memcpy((char *) &resultEntry->tuple_data, (char *) projectedTuple->t_data, projectedTuple->t_len);

		inserted = true;
		ss->prefetch_counter++;
		ss->smooth_counter++;
		//17.02.2014
		//increase the counter just for the first time we calculate this page
		if (!(*pageHasOneResultTuple)) {
			ss->global_qualifying_pages++;
			ss->local_qualifying_pages++;
			*pageHasOneResultTuple = true;
		}

	} else {
		inserted = false;
	}
	/*I am supposed to free projected tuple here */
	heap_freetuple(projectedTuple);

	return inserted;

}

////renata: NO PROJECTION
//
//bool
//smooth_resultcache_add_tuple(ResultCache *cache, const BlockNumber blknum, const OffsetNumber off, const HeapTuple tpl, const TupleDesc tupleDesc, List *target_list, List *qual_list, Index index)
//{
//		ResultCacheEntry *resultEntry;
//
//		/* safety check to ensure we don't overrun bit array bounds */
//		if (off < 1 || off > MaxHeapTuplesPerPage)
//			elog(ERROR, "tuple offset out of range: %u", off);
//
//
//		resultEntry = smooth_resultcache_get_resultentry(cache, tpl, blknum);
//
//		if(resultEntry){
//
//			//heap_copytuple_into_hash(tpl, &resultEntry->tuple);
//			//heap_copytuple_with_tuple(tpl, &resultEntry->tuple);
//			//resultEntry->tuple = heap_copytuple(tpl);
//
//
//			memcpy((char *) &resultEntry->tuple_data, (char *) tpl->t_data, tpl->t_len);
//
//			return true;
//		}
//		return false;
//
//}

HeapTuple project_tuple(const HeapTuple tuple, const TupleDesc tupleDesc, List *target_list, List *qual_list,
		Index index, Datum *values, bool * isnull) {
	int numberOfAttributes = tupleDesc->natts;
	Form_pg_attribute *att = tupleDesc->attrs;
	int attoff;
	HeapTuple newTuple = NULL;
	bool found = false;
	Bitmapset *attrs_used = NULL;

	ListCell *tl;

	/*
	 * Add all the attributes needed for joins or final output.  Note: we must
	 * look at reltargetlist, not the attr_needed data, because attr_needed
	 * isn't computed for inheritance child rels.
	 */
	//pull_varattnos((Node *) target_list, index, &attrs_used);
	/* Add all the attributes used by restriction clauses. */

	foreach(tl, qual_list) {
		ExprState *exprstate = (ExprState *) lfirst(tl);
		pull_varattnos((Node *) exprstate->expr, index, &attrs_used);
	}

	heap_deform_tuple(tuple, tupleDesc, values, isnull);

	for (attoff = 0; attoff < numberOfAttributes; attoff++) {
		Form_pg_attribute thisatt = att[attoff];

		is_target_attribute(thisatt, target_list, found);
		//2014 - sigmod 2015 - todo filter attributes should also be stored as attributes of interest
		//if(!found){
		//is_qual_attribute(thisatt, qual_list, found);
		//found = is_qual_attribute(thisatt, qual_list);
		//}

		//old
		//if (!found)
//		if (!is_target_attribute(thisatt, target_list))
		// 2014 - todo sigmod 2015 - new with bitmap
		if (!found) {
			if (qual_list == NULL
					|| (!bms_is_member((thisatt->attnum - FirstLowInvalidHeapAttributeNumber), attrs_used))) { //FOR THE ONES I DON'T NEED - JUST SET IS NULL TO YES - AND HOPEFULLY I WON'T PICK IT UP WHEN I FORM NEW TUPLE
				values[attoff] = (Datum) 0;
				isnull[attoff] = true;
			}
		}
	}

	/*
	 * create a new tuple from the values and isnull arrays
	 */
	newTuple = heap_form_tuple(tupleDesc, values, isnull);

	newTuple->t_self = tuple->t_self;
	newTuple->t_tableOid = tuple->t_tableOid;

	return newTuple;

}
//was tuple already processed in Stage 1 of Smooth Scan

bool smooth_tuplecache_find_tuple(TupleIDCache *cache, TID tid) {
	TupleIDCacheEntry * resultEntry = NULL;
	bool found = false;
	resultEntry = (TupleIDCacheEntry *) hash_search(cache->hashtable, (void *) &tid, HASH_FIND, NULL);
	if (resultEntry != NULL)
		return true;
	else
		return false;
}

bool smooth_resultcache_find_tuple(ResultCache *cache, HeapTuple tpl, BlockNumber blkn) {
	ResultCacheEntry *resultCache = NULL;
	bool found = false;

//	TID tid = form_tuple_id(tpl, blkn);
	TID tid;
	//calling macro
	form_tuple_id(tpl, blkn, &tid);

	resultCache = smooth_resultcache_find_resultentry(cache, tid);

	/* if we have a bucket for this block */
	if (resultCache != NULL) {
		//this works for regular
		tpl->t_data = ((HeapTupleHeader) (&resultCache->tuple_data));

		found = true;
	}
	return found;
}

/* This method returns ResultCacheEntry if exists, if not NULL is returned */
static ResultCacheEntry *
smooth_resultcache_find_resultentry(ResultCache *cache, ResultCacheKey tid) {
	ResultCacheEntry *resultCache = NULL;

	if (cache->nentries == 0) /* in case pagetable doesn't exist */
	{
		printf("\nCache has not entries\n");
		return NULL;
	}

	if (cache->status == SS_EMPTY) {
		if (cache->isCached) {

			smooth_resultcache_create(cache, cache->tuple_length);
		} else {

			printf("Cache is empty\n");
			return NULL;
		}
	}

	resultCache = (ResultCacheEntry *) hash_search(cache->hashtable, (void *) &tid, HASH_FIND, NULL);

	return resultCache;
}

/* This method returns ResultCacheEntry if exists, if not new one is created and returned */

/*
 * This may cause the table to exceed the desired memory size.
 */
static ResultCacheEntry *
smooth_resultcache_get_resultentry(ResultCache *cache, HeapTuple tpl, BlockNumber blknum) {
	ResultCacheEntry *resultCache;
	bool found;

//	TID tid = form_tuple_id(tpl, blknum);
	TID tid;
	//calling macro
	form_tuple_id(tpl, blknum, &tid);

	if (cache->status == SS_EMPTY) {
		smooth_resultcache_create(cache, tpl->t_len);
	}
	if (cache->status == SS_HASH) {
		/* Look up or create an entry */
		resultCache = (ResultCacheEntry *) hash_search(cache->hashtable, (void *) &tid, HASH_ENTER, &found);
	} else {
		/* either last or full */
		/* WE CANNOT CREATE ADD MORE PAGES  */
		resultCache = (ResultCacheEntry *) hash_search(cache->hashtable, (void *) &tid, HASH_FIND, &found);
	}
	/*checking if hash table is full*/
	if (resultCache != NULL) {
		/* Initialize it if not present before */
		if (!found) {
			//this works
			Size entry = MAXALIGN(sizeof(ResultCacheKey)+ (tpl->t_len));

			//this is just for tpch testing
			//Size entry = MAXALIGN(sizeof(ResultCacheKey))+ HEAPTUPLESIZE + cache->tuple_length;

			MemSet(resultCache, 0, entry);
			//MemSet(resultCache, 0, (sizeof(TID) + tpl->t_len));
			resultCache->tid = tid;

			/* must count it too */
			cache->nentries++;

			if (cache->nentries == cache->maxentries) {
				printf("\nNO MORE PAGES ARE SUPPOSED TO BE ADDED IN THE CACHE. FULL! \n ");
				cache->status = SS_FULL;
			}
		}
	} else {
		printf("\nHash table is full!\n");
		cache->status = SS_FULL;
	}

	return resultCache;
}
/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the IndexScan node's currentRelation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
IndexSmoothNext(IndexSmoothScanState *node) {
	EState *estate;
	ExprContext *econtext;
	ScanDirection direction;
	IndexScanDesc scandesc;
	HeapTuple tuple;
	TupleTableSlot *slot;

	/*
	 * extract necessary information from index scan node
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	/* flip direction if this is an overall backward scan */
	if (ScanDirectionIsBackward(((IndexScan *) node->ss.ps.plan)->indexorderdir)) {
		if (ScanDirectionIsForward(direction))
			direction = BackwardScanDirection;
		else if (ScanDirectionIsBackward(direction))
			direction = ForwardScanDirection;
	}
	scandesc = node->iss_ScanDesc;
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * ok, now that we have what we need, fetch the next tuple.
	 */
	//while ((tuple = indexsmooth_getnext(scandesc, direction, node->iss_NumSmoothScanKeys, node->iss_SmoothScanKeys, node->ss.ps.targetlist)) != NULL)
	//while ((tuple = indexsmooth_getnext(scandesc, direction, ((BTScanOpaque)scandesc->opaque)->numberOfKeys, ((BTScanOpaque)scandesc->opaque)->keyData, node->ss.ps.targetlist)) != NULL)
	while ((tuple = indexsmooth_getnext(scandesc, direction, node->ss.ps.plan->plan_rows, node->iss_NumScanKeys,
			node->iss_ScanKeys, node->ss.ps.targetlist, node->ss.ps.qual,
			((IndexSmoothScan *) (node->ss.ps.plan))->scan.scanrelid, node->iss_NumSmoothScanKeys,
			node->iss_SmoothScanKeys, node->allqual, node->ss.ps.ps_ExprContext, node->ss.ss_ScanTupleSlot)) != NULL) {
		/*
		 * Store the scanned tuple in the scan tuple slot of the scan state.
		 * Note: we pass 'false' because tuples returned by amgetnext are
		 * pointers onto disk pages and must not be pfree()'d.
		 */
		ExecStoreTuple(tuple, /* tuple to store */
		slot, /* slot to store in */
		scandesc->xs_cbuf, /* buffer containing tuple */
		false); /* don't pfree */

		/*
		 * If the index was lossy, we have to recheck the index quals using
		 * the fetched tuple.
		 */
		if (scandesc->xs_recheck) {
			econtext->ecxt_scantuple = slot;
			ResetExprContext(econtext);
			if (!ExecQual(node->indexqualorig, econtext, false)) {
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				continue;
			}
		}

		return slot;
	}

	/*
	 * if we get here it means the index scan failed so we are at the end of
	 * the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 * IndexSmoothRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool IndexSmoothRecheck(IndexSmoothScanState *node, TupleTableSlot *slot) {
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the indexqual condition? */
	econtext->ecxt_scantuple = slot;

	ResetExprContext(econtext);

	return ExecQual(node->indexqualorig, econtext, false);
}

/* ----------------------------------------------------------------
 *		ExecIndexScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecIndexSmoothScan(IndexSmoothScanState *node) {
	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 */
	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		ExecReScan((PlanState *) node);

	return ExecScan(&node->ss, (ExecScanAccessMtd) IndexSmoothNext, (ExecScanRecheckMtd) IndexSmoothRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void ExecReScanIndexSmoothScan(IndexSmoothScanState *node) {
	SmoothScanOpaque ss;
	IndexScanDesc indexScanDesc;
	indexScanDesc = node->iss_ScanDesc;

	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.  But first,
	 * reset the context so we don't leak memory as each outer tuple is
	 * scanned.  Note this assumes that we will recalculate *all* runtime keys
	 * on each call.
	 */
	if (node->iss_NumRuntimeKeys != 0) {
		ExprContext *econtext = node->iss_RuntimeContext;

		ResetExprContext(econtext);
		ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
	}
	node->iss_RuntimeKeysReady = true;

	//14.02.2014 - clear data structures

	/*delete smooth info*/
	if (indexScanDesc != NULL) {
		// we are executing SMOOTHNESTEDLOOP we should have just one HASH table for everything
		if (!enable_smoothnestedloop) {

			ss = (SmoothScanOpaque) node->iss_ScanDesc->smoothInfo;
			ss->prefetch_counter = 0;
			ss->smooth_counter = 0;
			ss->num_result_cache_hits = 0;
			ss->num_result_cache_misses = 0;
			ss->num_result_tuples = 0;

			//17.02.2014 -for enable_skewcheck
			ss->global_qualifying_pages = 0;
			ss->local_qualifying_pages = 0;
			ss->local_num_pages = 0;

			ss->start_prefetch = false;
			ss->start_smooth = false;

			if (ss->bs_vispages != NULL && !enable_smoothshare)
				bms_free(ss->bs_vispages);

			ss->currPos.nextTupleOffset = 0;
			ss->markPos.nextTupleOffset = 0;
			//	ss->bs_tovispages = NULL;
			ss->bs_vispages = NULL;

			ss->more_data_for_smooth = false;

			ss->nextPageId = InvalidBlockNumber;

			ss->currPos.firstHeapItem = 0;
			ss->currPos.lastHeapItem = 0;
			ss->currPos.itemHeapIndex = 0;
			ss->currPos.nextTupleOffset = 0;

			ss->prefetch_pages = 0;
			ss->prefetch_target = 0;
			ss->prefetch_cumul = 0;

			if (ss->tupleID_cache)
				smooth_tuplecache_free(ss->tupleID_cache);
			ss->tupleID_cache = NULL;

			if (ss->result_cache != NULL) {
				smooth_resultcache_free(ss->result_cache);
			}

			//RECREATE
			if (ss->orderby) {

				if(enable_smoothshare)
				ss->result_cache = smooth_resultcache_create_empty(smooth_work_mem * 512L,
						RelationGetDescr(indexScanDesc->heapRelation)->natts);
				else
				ss->result_cache = smooth_resultcache_create_empty(work_mem * 1024L,
											RelationGetDescr(indexScanDesc->heapRelation)->natts);

			} else {
				ss->result_cache = NULL;
			}

			if (num_tuples_switch >= 0) {
				ss->tupleID_cache = smooth_tuplecache_create_empty();

			} else {
				ss->tupleID_cache = NULL;
			}
		} // end if enable_smoothnestedloop
	}

	//do the same for SMOOTH SCAN KEYS
	//2014 renata todo: commented after push down predicates
	if (node->iss_NumRuntimeSmoothKeys != 0) {
		ExprContext *econtext = node->iss_RuntimeSmoothContext;

		ResetExprContext(econtext);
		ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeSmoothKeys, node->iss_NumRuntimeSmoothKeys);
	}
	node->iss_RuntimeSmoothKeysReady = true;

	//todo: check whether i need to do the same for smooth keys  if Runtime keys - update allqual keys!!!!
	/* reset index scan */

	index_rescan(node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys,
			node->iss_NumOrderByKeys);

	ExecScanReScan(&node->ss);

}

/* ----------------------------------------------------------------
 *		ExecEndIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndIndexSmoothScan(IndexSmoothScanState *node) {
	Relation indexRelationDesc;
	IndexScanDesc indexScanDesc;
	Relation relation;
	SmoothScanOpaque ss;
	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->iss_RelationDesc;
	indexScanDesc = node->iss_ScanDesc;
	relation = node->ss.ss_currentRelation;

	/*delete smooth info*/
	if (indexScanDesc != NULL) {
		ss = (SmoothScanOpaque) node->iss_ScanDesc->smoothInfo;

		printf("\nOverall table size in blocks %ld, prefetcher accumulated %ld, page cache size %ld \n",
				ss->rel_nblocks, ss->prefetch_cumul, bms_num_members(ss->bs_vispages));
		if (ss->bs_vispages != NULL)
			printf("\n Page ID cache size %ld in words", ss->bs_vispages->nwords);

		printf("\n Global number of qualifying pages %ld", ss->global_qualifying_pages);

		if (ss->tupleID_cache != NULL)
			printf("\n Table ID cache size %ld, number of entries %ld",
					hash_estimate_size(ss->tupleID_cache->nentries, sizeof(TupleIDCacheEntry)),
					ss->tupleID_cache->nentries);

		printf("\n Number of result cache (tableID) hits: %ld, misses:  %ld, out of total number of tuples: %ld \n",
				ss->num_result_cache_hits, ss->num_result_cache_misses, ss->num_result_tuples);
		printf("\n Smooth counter of added tuples: %d\n", ss->smooth_counter);
		/*delete smooth info*/
		/* we aren't holding any read locks, but gotta drop the pins */
		if (SmoothScanPosIsValid(ss->currPos)) {
			/* Before leaving current page, deal with any killed items */
			if (ss->numKilled > 0)
				_bt_killitems(indexScanDesc, false);
			ReleaseBuffer(ss->currPos.buf);
			ss->currPos.buf = InvalidBuffer;
		}
		if (SmoothScanPosIsValid(ss->markPos)) {
			ReleaseBuffer(ss->markPos.buf);
			ss->markPos.buf = InvalidBuffer;
		}
		ss->markItemIndex = -1;

		/* Release storage */
		if (ss->keyData != NULL)
			pfree(ss->keyData);
		/* so->arrayKeyData and so->arrayKeys are in arrayContext */
		if (ss->arrayContext != NULL)
			MemoryContextDelete(ss->arrayContext);
		if (ss->killedItems != NULL)
			pfree(ss->killedItems);
		if (ss->currTuples != NULL)
			pfree(ss->currTuples);
		//	if(ss->bs_tovispages != NULL)
		//		bms_free(ss->bs_tovispages);
		if (ss->bs_vispages != NULL)

			if (enable_smoothshare) {
				bool found;
				Size bs_size = sizeof(ss->bs_vispages) + (sizeof(bitmapword) * (ss->bs_vispages->nwords - 1));
				Bitmapset *bs_shared = (Bitmapset*) ShmemInitStruct("Bitmap vispages", bs_size, &found);
				bs_shared->nwords = ss->bs_vispages->nwords;
				memcpy(bs_shared->words, ss->bs_vispages->words, sizeof(bitmapword) * ss->bs_vispages->nwords);
				printf("Size of words local : %d.\n", sizeof(bitmapword) * ss->bs_vispages->nwords);
				printf("number of memebers in local :  %d.\n", bms_num_members(ss->bs_vispages));
				printf("Size of words shared : %d.\n", sizeof(bitmapword) * bs_shared->nwords);
				printf("number of memebers in shared :  %d.\n", bms_num_members(bs_shared));

				ss->result_cache->bs_size = bs_size;
			} else
				bms_free(ss->bs_vispages);

		if (ss->tupleID_cache)
			smooth_tuplecache_free(ss->tupleID_cache);
		ss->tupleID_cache = NULL;
		printf("\nHash table with %d tuples\n", hash_get_num_entries(ss->result_cache->hashtable));

		if (ss->result_cache != NULL) {
			smooth_resultcache_free(ss->result_cache);
		}

		/* so->markTuples should not be pfree'd, see btrescan */
		pfree(ss);

	}

	/*
	 * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
	 */
#ifdef NOT_USED
	ExecFreeExprContext(&node->ss.ps);
	if (node->iss_RuntimeContext)
	FreeExprContext(node->iss_RuntimeContext, true);
	if (node->iss_RuntimeSmoothContext)
	FreeExprContext(node->iss_RuntimeSmoothContext, true);
#endif

	/*
	 * clear out tuple table slots
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (indexScanDesc)
		index_endscan(indexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecIndexSmoothMarkPos(IndexSmoothScanState *node) {
	index_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecIndexSmoothRestrPos(IndexSmoothScanState *node) {
	index_restrpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
IndexSmoothScanState *
ExecInitIndexSmoothScan(IndexSmoothScan *node, EState *estate, int eflags) {
	IndexSmoothScanState *indexstate;
	Relation currentRelation;
	bool relistarget;
	SmoothScanOpaque ss;
	ResultCache *resultCache;

	/*
	 * create state structure
	 */
	indexstate = makeNode(IndexSmoothScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &indexstate->ss.ps);

	indexstate->ss.ps.ps_TupFromTlist = false;

	//14.02.2014 change this by only looking at order by statement
	//old - for microbechmarks
	if (node->orderby) {
		//for tpch I need this
		//if(node->indexorderby){
		printf("\nOrder should be respected\n");
	} else {
		printf("\nOrder does NOT have to be respected\n");
	}

	/*
	 * initialize child expressions
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).  Likewise for
	 * indexorderby, if any.  But the indexqualorig expression is always
	 * initialized even though it will only be used in some uncommon cases ---
	 * would be nice to improve that.  (Problem is that any SubPlans present
	 * in the expression must be found now...)
	 */
	indexstate->ss.ps.targetlist = (List *) ExecInitExpr((Expr *) node->scan.plan.targetlist, (PlanState *) indexstate);
	//todo initialize them only if !enable_filterpushdown
	if (!enable_filterpushdown) {
		indexstate->ss.ps.qual = (List *) ExecInitExpr((Expr *) node->scan.plan.qual, (PlanState *) indexstate);
	} else {
		indexstate->ss.ps.qual = NULL;
	}

	indexstate->indexqualorig = (List *) ExecInitExpr((Expr *) node->indexqualorig, (PlanState *) indexstate);

	indexstate->allqual = (List *) ExecInitExpr((Expr *) node->allqual, (PlanState *) indexstate); //smooth scan needs to keep all predicates

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &indexstate->ss.ps);
	ExecInitScanTupleSlot(estate, &indexstate->ss);

	/*
	 * open the base relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);

	indexstate->ss.ss_currentRelation = currentRelation;
	indexstate->ss.ss_currentScanDesc = NULL; /* no heap scan here */

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecAssignScanType(&indexstate->ss, RelationGetDescr(currentRelation));

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&indexstate->ss.ps);
	ExecAssignScanProjectionInfo(&indexstate->ss);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
	 * references to nonexistent indexes.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return indexstate;

	/*
	 * Open the index relation.
	 *
	 * If the parent table is one of the target relations of the query, then
	 * InitPlan already opened and write-locked the index, so we can avoid
	 * taking another lock here.  Otherwise we need a normal reader's lock.
	 */
	relistarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
	indexstate->iss_RelationDesc = index_open(node->indexid, relistarget ? NoLock : AccessShareLock);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->iss_RuntimeKeysReady = false;
	indexstate->iss_RuntimeKeys = NULL;
	indexstate->iss_NumRuntimeKeys = 0;

	/*
	 * Initialize index-specific SMOOTH SCAN statte
	 */
	indexstate->iss_RuntimeSmoothKeysReady = false;
	indexstate->iss_RuntimeSmoothKeys = NULL;
	indexstate->iss_NumRuntimeSmoothKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate, indexstate->iss_RelationDesc, node->indexqual, false,
			&indexstate->iss_ScanKeys, &indexstate->iss_NumScanKeys, &indexstate->iss_RuntimeKeys,
			&indexstate->iss_NumRuntimeKeys, NULL, /* no ArrayKeys */
			NULL);

	/*
	 * build the index scan keys from the index qualification
	 */
	//this one worked for vldb and sigmod 2013
	ExecIndexBuildSmoothScanKeys((PlanState *) indexstate, indexstate->iss_RelationDesc,
			//this works
			node->indexqual, false, &indexstate->iss_SmoothScanKeys, &indexstate->iss_NumSmoothScanKeys,
			&indexstate->iss_RuntimeSmoothKeys, &indexstate->iss_NumRuntimeSmoothKeys, NULL, /* no ArrayKeys */
			NULL);
	//for sigmod 2014 new code, renata todo
	//THIS IS NOT ANYMORE - PREDICATE PUSHDOWN IS DONE DIFFERENTLY
//	ExecIndexBuildSmoothScanKeys((PlanState *) indexstate,
//						   indexstate->iss_RelationDesc,
//						   //this works
//						   //node->indexqual,
//						   //05.28.2014 - adding all predicates instead of smooth keys
//						   node->allqual,
//						   false,
//						   &indexstate->iss_SmoothScanKeys,
//						   &indexstate->iss_NumSmoothScanKeys,
//						   &indexstate->iss_RuntimeSmoothKeys,
//						   &indexstate->iss_NumRuntimeSmoothKeys,
//						   NULL,	/* no ArrayKeys */
//						   NULL);
///******************************************************/
//	//OLD ONE before the internship
//	ExecIndexBuildSmoothScanKeys((PlanState *) indexstate,
//						   indexstate->iss_RelationDesc,
//						   node->indexqualorig,
//						   false,
//						   &indexstate->iss_SmoothScanKeys,
//						   &indexstate->iss_NumSmoothScanKeys,
//						   &indexstate->iss_RuntimeKeys,
//						   &indexstate->iss_NumRuntimeKeys,
//						   NULL,	/* no ArrayKeys */
//						   NULL);
///******************************************************/

	/*
	 * any ORDER BY exprs have to be turned into scankeys in the same way
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate, indexstate->iss_RelationDesc, node->indexorderby, true,
			&indexstate->iss_OrderByKeys, &indexstate->iss_NumOrderByKeys, &indexstate->iss_RuntimeKeys,
			&indexstate->iss_NumRuntimeKeys, NULL, /* no ArrayKeys */
			NULL);

	ExecIndexBuildScanKeys((PlanState *) indexstate, indexstate->iss_RelationDesc, node->indexorderby, true,
			&indexstate->iss_OrderBySmoothKeys, &indexstate->iss_NumOrderBySmoothKeys,
			&indexstate->iss_RuntimeSmoothKeys, &indexstate->iss_NumRuntimeSmoothKeys, NULL, /* no ArrayKeys */
			NULL);

	/*
	 * If we have runtime keys, we need an ExprContext to evaluate them. The
	 * node's standard context won't do because we want to reset that context
	 * for every tuple.  So, build another context just like the other one...
	 * -tgl 7/11/00
	 */
	if (indexstate->iss_NumRuntimeKeys != 0) {
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->iss_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	} else {
		indexstate->iss_RuntimeContext = NULL;
	}

	//todo check this
	if (indexstate->iss_NumRuntimeSmoothKeys != 0) {
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->iss_RuntimeSmoothContext = indexstate->ss.ps.ps_ExprContext;

	} else {
		indexstate->iss_RuntimeSmoothContext = NULL;
	}

	/*
	 * Initialize scan descriptor.
	 */
	indexstate->iss_ScanDesc = index_beginscan(currentRelation, indexstate->iss_RelationDesc, estate->es_snapshot,
			indexstate->iss_NumScanKeys, indexstate->iss_NumOrderByKeys);
	/**********************************************************/
	//smooth scan part
	ss = (SmoothScanOpaque) palloc(sizeof(SmoothScanOpaqueData));
	ss->currPos.buf = ss->markPos.buf = InvalidBuffer;
	if (indexstate->iss_ScanDesc->numberOfKeys > 0)
		ss->keyData = (ScanKey) palloc(indexstate->iss_ScanDesc->numberOfKeys * sizeof(ScanKeyData));
	else
		ss->keyData = NULL;

	ss->arrayKeyData = NULL; /* assume no array keys for now */
	ss->numArrayKeys = 0;
	ss->arrayKeys = NULL;
	ss->arrayContext = NULL;

	ss->killedItems = NULL; /* until needed */
	ss->numKilled = 0;
	/*
	 * We don't know yet whether the scan will be index-only, so we do not
	 * allocate the tuple workspace arrays until btrescan.	However, we set up
	 * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
	 */
	ss->currTuples = (char *) palloc(BLCKSZ * 2);
	ss->markTuples = ss->currTuples + BLCKSZ;

	ss->currPos.nextTupleOffset = 0;
	ss->markPos.nextTupleOffset = 0;
	//	ss->bs_tovispages = NULL;
	ss->bs_vispages = NULL;

	ss->more_data_for_smooth = false;

	ss->nextPageId = InvalidBlockNumber;

	ss->currPos.firstHeapItem = 0;
	ss->currPos.lastHeapItem = 0;
	ss->currPos.itemHeapIndex = 0;
	ss->currPos.nextTupleOffset = 0;

	ss->prefetch_pages = 0;
	ss->prefetch_target = 0;
	ss->prefetch_cumul = 0;

	ss->rel_nblocks = RelationGetNumberOfBlocks(currentRelation);

	/* respect order constraint - yes or no*/
	//14.02.2014
	//old - for microbechmarks
	if (node->orderby)
		//for tpch I need this
		//if(node->indexorderby)
		ss->orderby = true;
	else
		ss->orderby = false;

	ss->prefetch_counter = 0;
	ss->smooth_counter = 0;
	ss->num_result_cache_hits = 0;
	ss->num_result_cache_misses = 0;

	ss->num_result_tuples = 0;

	ss->start_prefetch = false;
	ss->start_smooth = false;

	//17.02.2014 -for enable_skewcheck
	ss->global_qualifying_pages = 0;
	ss->local_qualifying_pages = 0;
	ss->local_num_pages = 0;
	//ss->num_tuples_per_page = BLCKSZ / indexstate->ss.ps.plan->plan_width;  // this is simplification

	if (ss->orderby) {
		if (enable_smoothshare)
		ss->result_cache = smooth_resultcache_create_empty(smooth_work_mem * 512L, RelationGetDescr(currentRelation)->natts);
		else
		ss->result_cache = smooth_resultcache_create_empty(work_mem * 1024L, RelationGetDescr(currentRelation)->natts);

		// we need  to check if there's exist one in shared memory otherwise we start by building
		// a bitmap in local memory
		if (enable_smoothshare && ss->result_cache->isCached) {
			bool found;
			ss->bs_vispages = (Bitmapset*) ShmemInitStruct("Bitmap vispages", ss->result_cache->bs_size, &found);
			Assert(found==true);
			printf("Size of words shared : %d.\n", sizeof(bitmapword) * ss->bs_vispages->nwords);
			printf("number of memebers in shared :  %d.\n", bms_num_members(ss->bs_vispages));
		}
	} else {
		ss->result_cache = NULL;
	}

	if (num_tuples_switch >= 0) {
		ss->tupleID_cache = smooth_tuplecache_create_empty();

	} else {
		ss->tupleID_cache = NULL;
	}

	/* this should go in initialize smooth info */
	indexstate->iss_ScanDesc->smoothInfo = ss;
	/**************************************************************/

	/*/AFTER THIS STEP indexstate->iss_ScanDesc->KEYDATA AND indexstate->iss_ScanDesc->ORDERBY IS SET
	 * If no run-time keys to calculate, go ahead and pass the scankeys to the
	 * index AM.
	 * if (scankey && scan->numberOfKeys > 0)
	 memmove(scan->keyData,
	 scankey,
	 scan->numberOfKeys * sizeof(ScanKeyData));
	 */
	if (indexstate->iss_NumRuntimeKeys == 0)
		index_rescan(indexstate->iss_ScanDesc, indexstate->iss_ScanKeys, indexstate->iss_NumScanKeys,
				indexstate->iss_OrderByKeys, indexstate->iss_NumOrderByKeys);

	/* renata */
	/* in initialize Smooth Scan Info */

	/*
	 * all done.
	 */
	return indexstate;
}

//renata
//THIS METHOD IS NEEDED BECAUSE WE NEED TRANSLATION BETWEEN INDEX ATTRIBUTE NUMBER AND HEAP ATTRIBUTE NUMBER
void ExecIndexBuildSmoothScanKeys(PlanState *planstate, Relation index, List *quals, bool isorderby, ScanKey *scanKeys,
		int *numScanKeys, IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys, IndexArrayKeyInfo **arrayKeys,
		int *numArrayKeys) {
	ListCell *qual_cell;
	ScanKey scan_keys;
	IndexRuntimeKeyInfo *runtime_keys;
	IndexArrayKeyInfo *array_keys;
	int n_scan_keys;
	int n_runtime_keys;
	int max_runtime_keys;
	int n_array_keys;
	int j;

	/* Allocate array for ScanKey structs: one per qual */
	n_scan_keys = list_length(quals);
	scan_keys = (ScanKey) palloc(n_scan_keys * sizeof(ScanKeyData));

	/*
	 * runtime_keys array is dynamically resized as needed.  We handle it this
	 * way so that the same runtime keys array can be shared between
	 * indexquals and indexorderbys, which will be processed in separate calls
	 * of this function.  Caller must be sure to pass in NULL/0 for first
	 * call.
	 */
	runtime_keys = *runtimeKeys;
	n_runtime_keys = max_runtime_keys = *numRuntimeKeys;

	/* Allocate array_keys as large as it could possibly need to be */
	array_keys = (IndexArrayKeyInfo *) palloc0(n_scan_keys * sizeof(IndexArrayKeyInfo));
	n_array_keys = 0;

	/*
	 * for each opclause in the given qual, convert the opclause into a single
	 * scan key
	 */
	j = 0;
	foreach(qual_cell, quals) {
		Expr *clause = (Expr *) lfirst(qual_cell);
		ScanKey this_scan_key = &scan_keys[j++];
		Oid opno; /* operator's OID */
		RegProcedure opfuncid; /* operator proc id used in scan */
		Oid opfamily; /* opfamily of index column */
		int op_strategy; /* operator's strategy number */
		Oid op_lefttype; /* operator's declared input types */
		Oid op_righttype;
		Expr *leftop; /* expr on lhs of operator */
		Expr *rightop; /* expr on rhs ... */
		AttrNumber varattno; /* att number used in INDEX */
		AttrNumber varorigattno; /* att number used in HEAP */
		bool belongsToJoin = false;

		if (IsA(clause, OpExpr)) {
			/* indexkey op const or indexkey op expression */
			int flags = 0;
			Datum scanvalue;

			opno = ((OpExpr *) clause)->opno;
			opfuncid = ((OpExpr *) clause)->opfuncid;

			/*
			 * leftop should be the index key Var, possibly relabeled
			 */
			leftop = (Expr *) get_leftop(clause);

			if (leftop && IsA(leftop, RelabelType))
				leftop = ((RelabelType *) leftop)->arg;

			Assert(leftop != NULL);
//2014
//				if (!(IsA(leftop, Var) &&
//					  ((Var *) leftop)->varno == INDEX_VAR))
//					elog(ERROR, "indexqual doesn't have key on left side");

			//ATTRIBUTE NUMBER IN INDEX (STARTING FROM 1)
			varattno = ((Var *) leftop)->varattno;
			//ATTRIBUTE NUMBER IN HEAP STARTING FROM 1
			varorigattno = ((Var *) leftop)->varoattno;

			//THE IDEA HERE IS TO CHECK ALL THE LOGIC WITH INDEX ORDER AND THEN IN THE END JUST GIVE HEAP ORDER NUMBER
			//SINCE WE WILL BE USING THIS ONE FOR CHECKS
//2014
//				if (varattno < 1 || varattno > index->rd_index->indnatts)
//					elog(ERROR, "bogus index qualification");
//
//				/*
//				 * We have to look up the operator's strategy number.  This
//				 * provides a cross-check that the operator does match the index.
//				 */
//				opfamily = index->rd_opfamily[varattno - 1];
//
//				get_op_opfamily_properties(opno, opfamily, isorderby,
//										   &op_strategy,
//										   &op_lefttype,
//										   &op_righttype);
//
//				if (isorderby)
//					flags |= SK_ORDER_BY;

			/*
			 * rightop is the constant or variable comparison value
			 */
			rightop = (Expr *) get_rightop(clause);

			if (rightop && IsA(rightop, RelabelType))
				rightop = ((RelabelType *) rightop)->arg;

			Assert(rightop != NULL);

			if (IsA(rightop, Const)) {
				/* OK, simple constant comparison value */
				scanvalue = ((Const *) rightop)->constvalue;
				if (((Const *) rightop)->constisnull)
					flags |= SK_ISNULL;
			} else {
				/* Need to treat this one as a runtime key */
				if (n_runtime_keys >= max_runtime_keys) {
					if (max_runtime_keys == 0) {
						max_runtime_keys = 8;
						runtime_keys = (IndexRuntimeKeyInfo *) palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
					} else {
						max_runtime_keys *= 2;
						runtime_keys = (IndexRuntimeKeyInfo *) repalloc(runtime_keys,
								max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
					}
				}
				runtime_keys[n_runtime_keys].scan_key = this_scan_key;
				runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, planstate);
				runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
				n_runtime_keys++;
				scanvalue = (Datum) 0;
				belongsToJoin = true;
			}

			/*
			 * initialize the scan key's fields appropriately
			 */
			ScanKeyEntryInitialize(this_scan_key, flags,
			//varattno,	/* attribute number to scan */
					varorigattno, /* WE WILL BE USING FOR HEAP CHECKS*/
					op_strategy, /* op's strategy */
					op_righttype, /* strategy subtype */
					((OpExpr *) clause)->inputcollid, /* collation */
					opfuncid, /* reg proc to use */
					scanvalue, belongsToJoin); /* constant */

		} else if (IsA(clause, RowCompareExpr)) {
			/* (indexkey, indexkey, ...) op (expression, expression, ...) */
			RowCompareExpr *rc = (RowCompareExpr *) clause;
			ListCell *largs_cell = list_head(rc->largs);
			ListCell *rargs_cell = list_head(rc->rargs);
			ListCell *opnos_cell = list_head(rc->opnos);
			ListCell *collids_cell = list_head(rc->inputcollids);
			ScanKey first_sub_key;
			int n_sub_key;

			Assert(!isorderby);

			first_sub_key = (ScanKey) palloc(list_length(rc->opnos) * sizeof(ScanKeyData));
			n_sub_key = 0;

			/* Scan RowCompare columns and generate subsidiary ScanKey items */
			while (opnos_cell != NULL) {
				ScanKey this_sub_key = &first_sub_key[n_sub_key];
				int flags = SK_ROW_MEMBER;
				Datum scanvalue;
				Oid inputcollation;

				/*
				 * leftop should be the index key Var, possibly relabeled
				 */
				leftop = (Expr *) lfirst(largs_cell);
				largs_cell = lnext(largs_cell);

				if (leftop && IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;

				Assert(leftop != NULL);
//
//					if (!(IsA(leftop, Var) &&
//						  ((Var *) leftop)->varno == INDEX_VAR))
//						elog(ERROR, "indexqual doesn't have key on left side");

				//ATTRIBUTE NUMBER IN INDEX (STARTING FROM 1)
				varattno = ((Var *) leftop)->varattno;

				//ATTRIBUTE NUMBER IN HEAP STARTING FROM 1
				varorigattno = ((Var *) leftop)->varoattno;

				/*
				 * We have to look up the operator's associated btree support
				 * function
				 */
				opno = lfirst_oid(opnos_cell);
				opnos_cell = lnext(opnos_cell);
//
//					if (index->rd_rel->relam != BTREE_AM_OID ||
//						varattno < 1 || varattno > index->rd_index->indnatts)
//						elog(ERROR, "bogus RowCompare index qualification");
//					opfamily = index->rd_opfamily[varattno - 1];
//
//					get_op_opfamily_properties(opno, opfamily, isorderby,
//											   &op_strategy,
//											   &op_lefttype,
//											   &op_righttype);
//
//					if (op_strategy != rc->rctype)
//						elog(ERROR, "RowCompare index qualification contains wrong operator");
//
//					opfuncid = get_opfamily_proc(opfamily,
//												 op_lefttype,
//												 op_righttype,
//												 BTORDER_PROC);

				inputcollation = lfirst_oid(collids_cell);
				collids_cell = lnext(collids_cell);

				/*
				 * rightop is the constant or variable comparison value
				 */
				rightop = (Expr *) lfirst(rargs_cell);
				rargs_cell = lnext(rargs_cell);

				if (rightop && IsA(rightop, RelabelType))
					rightop = ((RelabelType *) rightop)->arg;

				Assert(rightop != NULL);

				if (IsA(rightop, Const)) {
					/* OK, simple constant comparison value */
					scanvalue = ((Const *) rightop)->constvalue;
					if (((Const *) rightop)->constisnull)
						flags |= SK_ISNULL;
				} else {
					/* Need to treat this one as a runtime key */
					if (n_runtime_keys >= max_runtime_keys) {
						if (max_runtime_keys == 0) {
							max_runtime_keys = 8;
							runtime_keys =
									(IndexRuntimeKeyInfo *) palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
						} else {
							max_runtime_keys *= 2;
							runtime_keys = (IndexRuntimeKeyInfo *) repalloc(runtime_keys,
									max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
						}
					}
					runtime_keys[n_runtime_keys].scan_key = this_sub_key;
					runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, planstate);
					runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
					n_runtime_keys++;
					scanvalue = (Datum) 0;
					belongsToJoin = true;
				}

				/*
				 * initialize the subsidiary scan key's fields appropriately
				 */
				ScanKeyEntryInitialize(this_sub_key, flags,
				//varattno,		/* attribute number */
						varorigattno, //USE ATTRIBUTE NUMBER IN HEAP
						op_strategy, /* op's strategy */
						op_righttype, /* strategy subtype */
						inputcollation, /* collation */
						opfuncid, /* reg proc to use */
						scanvalue, belongsToJoin); /* constant */
				n_sub_key++;
			}

			/* Mark the last subsidiary scankey correctly */
			first_sub_key[n_sub_key - 1].sk_flags |= SK_ROW_END;

			/*
			 * We don't use ScanKeyEntryInitialize for the header because it
			 * isn't going to contain a valid sk_func pointer.
			 */
			MemSet(this_scan_key, 0, sizeof(ScanKeyData));
			this_scan_key->sk_flags = SK_ROW_HEADER;
			this_scan_key->sk_attno = first_sub_key->sk_attno;
			this_scan_key->sk_strategy = rc->rctype;
			/* sk_subtype, sk_collation, sk_func not used in a header */
			this_scan_key->sk_argument = PointerGetDatum(first_sub_key);
		} else if (IsA(clause, ScalarArrayOpExpr)) {
			/* indexkey op ANY (array-expression) */
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
			int flags = 0;
			Datum scanvalue;

			Assert(!isorderby);

			Assert(saop->useOr);
			opno = saop->opno;
			opfuncid = saop->opfuncid;

			/*
			 * leftop should be the index key Var, possibly relabeled
			 */
			leftop = (Expr *) linitial(saop->args);

			if (leftop && IsA(leftop, RelabelType))
				leftop = ((RelabelType *) leftop)->arg;

			Assert(leftop != NULL);
//
//				if (!(IsA(leftop, Var) &&
//					  ((Var *) leftop)->varno == INDEX_VAR))
//					elog(ERROR, "indexqual doesn't have key on left side");

			//ATTRIBUTE NUMBER IN INDEX (STARTING FROM 1)
			varattno = ((Var *) leftop)->varattno;

			//ATTRIBUTE NUMBER IN HEAP STARTING FROM 1
			varorigattno = ((Var *) leftop)->varoattno;

			if (varattno < 1 || varattno > index->rd_index->indnatts)
				elog(ERROR, "bogus index qualification");

//				/*
//				 * We have to look up the operator's strategy number.  This
//				 * provides a cross-check that the operator does match the index.
//				 */
//				opfamily = index->rd_opfamily[varattno - 1];
//
//				get_op_opfamily_properties(opno, opfamily, isorderby,
//										   &op_strategy,
//										   &op_lefttype,
//										   &op_righttype);
//
			/*
			 * rightop is the constant or variable array value
			 */
			rightop = (Expr *) lsecond(saop->args);

			if (rightop && IsA(rightop, RelabelType))
				rightop = ((RelabelType *) rightop)->arg;

			Assert(rightop != NULL);

			if (index->rd_am->amsearcharray) {
				/* Index AM will handle this like a simple operator */
				flags |= SK_SEARCHARRAY;
				if (IsA(rightop, Const)) {
					/* OK, simple constant comparison value */
					scanvalue = ((Const *) rightop)->constvalue;
					if (((Const *) rightop)->constisnull)
						flags |= SK_ISNULL;
				} else {
					/* Need to treat this one as a runtime key */
					if (n_runtime_keys >= max_runtime_keys) {
						if (max_runtime_keys == 0) {
							max_runtime_keys = 8;
							runtime_keys =
									(IndexRuntimeKeyInfo *) palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
						} else {
							max_runtime_keys *= 2;
							runtime_keys = (IndexRuntimeKeyInfo *) repalloc(runtime_keys,
									max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
						}
					}
					runtime_keys[n_runtime_keys].scan_key = this_scan_key;
					runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, planstate);

					/*
					 * Careful here: the runtime expression is not of
					 * op_righttype, but rather is an array of same; so
					 * TypeIsToastable() isn't helpful.  However, we can
					 * assume that all array types are toastable.
					 */
					runtime_keys[n_runtime_keys].key_toastable = true;
					n_runtime_keys++;
					scanvalue = (Datum) 0;
					belongsToJoin = true;
				}
			} else {
				/* Executor has to expand the array value */
				array_keys[n_array_keys].scan_key = this_scan_key;
				array_keys[n_array_keys].array_expr = ExecInitExpr(rightop, planstate);
				/* the remaining fields were zeroed by palloc0 */
				n_array_keys++;
				scanvalue = (Datum) 0;
			}

			/*
			 * initialize the scan key's fields appropriately
			 */
			ScanKeyEntryInitialize(this_scan_key, flags,
			//varattno,	/* attribute number to scan */
					varorigattno, //USE ORDER IN SCAN
					op_strategy, /* op's strategy */
					op_righttype, /* strategy subtype */
					saop->inputcollid, /* collation */
					opfuncid, /* reg proc to use */
					scanvalue, belongsToJoin); /* constant */
		} else if (IsA(clause, NullTest)) {
			/* indexkey IS NULL or indexkey IS NOT NULL */
			NullTest *ntest = (NullTest *) clause;
			int flags;

			Assert(!isorderby);

			/*
			 * argument should be the index key Var, possibly relabeled
			 */
			leftop = ntest->arg;

			if (leftop && IsA(leftop, RelabelType))
				leftop = ((RelabelType *) leftop)->arg;

			Assert(leftop != NULL);
//
//				if (!(IsA(leftop, Var) &&
//					  ((Var *) leftop)->varno == INDEX_VAR))
//					elog(ERROR, "NullTest indexqual has wrong key");

			//ATTRIBUTE NUMBER IN INDEX (STARTING FROM 1)
			varattno = ((Var *) leftop)->varattno;

			//ATTRIBUTE NUMBER IN HEAP STARTING FROM 1
			varorigattno = ((Var *) leftop)->varoattno;

			/*
			 * initialize the scan key's fields appropriately
			 */
			switch (ntest->nulltesttype) {
				case IS_NULL:
					flags = SK_ISNULL | SK_SEARCHNULL;
					break;
				case IS_NOT_NULL:
					flags = SK_ISNULL | SK_SEARCHNOTNULL;
					break;
				default:
					elog(ERROR, "unrecognized nulltesttype: %d", (int) ntest->nulltesttype);
					flags = 0; /* keep compiler quiet */
					break;
			}

			ScanKeyEntryInitialize(this_scan_key, flags,
			//varattno,	/* attribute number to scan */
					varorigattno, //USE ATTRIBUTE NUMBER FROM HEAP
					InvalidStrategy, /* no strategy */
					InvalidOid, /* no strategy subtype */
					InvalidOid, /* no collation */
					InvalidOid, /* no reg proc for this */
					(Datum) 0, belongsToJoin); /* constant */
		} else
			elog(ERROR, "unsupported indexqual type: %d", (int) nodeTag(clause));
	}

	Assert(n_runtime_keys <= max_runtime_keys);

	/* Get rid of any unused arrays */
	if (n_array_keys == 0) {
		pfree(array_keys);
		array_keys = NULL;
	}

	/*
	 * Return info to our caller.
	 */
	*scanKeys = scan_keys;
	*numScanKeys = n_scan_keys;
	*runtimeKeys = runtime_keys;
	*numRuntimeKeys = n_runtime_keys;
	if (arrayKeys) {
		*arrayKeys = array_keys;
		*numArrayKeys = n_array_keys;
	} else if (n_array_keys != 0)
		elog(ERROR, "ScalarArrayOpExpr index qual found where not allowed");
}

/***************************************************************************************************/
//previous version that partially worked
//in statement didn't work
//void
//ExecIndexBuildSmoothScanKeys(PlanState *planstate, Relation index,
//					   List *quals, bool isorderby,
//					   ScanKey *scanKeys, int *numScanKeys,
//					   IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
//					   IndexArrayKeyInfo **arrayKeys, int *numArrayKeys)
//{
//	ListCell   *qual_cell;
//	ScanKey		scan_keys;
//	IndexRuntimeKeyInfo *runtime_keys;
//	IndexArrayKeyInfo *array_keys;
//	int			n_scan_keys;
//	int			n_runtime_keys;
//	int			max_runtime_keys;
//	int			n_array_keys;
//	int			j;
//
//	/* Allocate array for ScanKey structs: one per qual */
//	n_scan_keys = list_length(quals);
//	scan_keys = (ScanKey) palloc(n_scan_keys * sizeof(ScanKeyData));
//
//	/*
//	 * runtime_keys array is dynamically resized as needed.  We handle it this
//	 * way so that the same runtime keys array can be shared between
//	 * indexquals and indexorderbys, which will be processed in separate calls
//	 * of this function.  Caller must be sure to pass in NULL/0 for first
//	 * call.
//	 */
//	runtime_keys = *runtimeKeys;
//	n_runtime_keys = max_runtime_keys = *numRuntimeKeys;
//
//	/* Allocate array_keys as large as it could possibly need to be */
//	array_keys = (IndexArrayKeyInfo *)
//		palloc0(n_scan_keys * sizeof(IndexArrayKeyInfo));
//	n_array_keys = 0;
//
//	/*
//	 * for each opclause in the given qual, convert the opclause into a single
//	 * scan key
//	 */
//	j = 0;
//	foreach(qual_cell, quals)
//	{
//		Expr	   *clause = (Expr *) lfirst(qual_cell);
//		ScanKey		this_scan_key = &scan_keys[j++];
//		Oid			opno;		/* operator's OID */
//		RegProcedure opfuncid;	/* operator proc id used in scan */
//		Oid			opfamily;	/* opfamily of index column */
//		int			op_strategy;	/* operator's strategy number */
//		Oid			op_lefttype;	/* operator's declared input types */
//		Oid			op_righttype;
//		Expr	   *leftop;		/* expr on lhs of operator */
//		Expr	   *rightop;	/* expr on rhs ... */
//		AttrNumber	varattno;	/* att number used in INDEX */
//		AttrNumber	varorigattno;	/* att number used in HEAP */
//
//		if (IsA(clause, OpExpr))
//		{
//			/* indexkey op const or indexkey op expression */
//			int			flags = 0;
//			Datum		scanvalue;
//
//			opno = ((OpExpr *) clause)->opno;
//			opfuncid = ((OpExpr *) clause)->opfuncid;
//
//			/*
//			 * leftop should be the index key Var, possibly relabeled
//			 */
//			leftop = (Expr *) get_leftop(clause);
//
//			if (leftop && IsA(leftop, RelabelType))
//				leftop = ((RelabelType *) leftop)->arg;
//
//			Assert(leftop != NULL);
//
//			if (!(IsA(leftop, Var)))
//				elog(ERROR, "indexqual doesn't have key on left side");
//
//			varattno = ((Var *) leftop)->varattno;
//			if (varattno < 1 )
//				elog(ERROR, "bogus index qualification");
//
////			/*
////			 * We have to look up the operator's strategy number.  This
////			 * provides a cross-check that the operator does match the index.
////			 */
////			opfamily = index->rd_opfamily[varattno - 1];
////
////			get_op_opfamily_properties(opno, opfamily, isorderby,
////									   &op_strategy,
////									   &op_lefttype,
////									   &op_righttype);
//
//			if (isorderby)
//				flags |= SK_ORDER_BY;
//
//			/*
//			 * rightop is the constant or variable comparison value
//			 */
//			rightop = (Expr *) get_rightop(clause);
//
//			if (rightop && IsA(rightop, RelabelType))
//				rightop = ((RelabelType *) rightop)->arg;
//
//			Assert(rightop != NULL);
//
//			if (IsA(rightop, Const))
//			{
//				/* OK, simple constant comparison value */
//				scanvalue = ((Const *) rightop)->constvalue;
//				if (((Const *) rightop)->constisnull)
//					flags |= SK_ISNULL;
//			}
//			else
//			{
//				/* Need to treat this one as a runtime key */
//				if (n_runtime_keys >= max_runtime_keys)
//				{
//					if (max_runtime_keys == 0)
//					{
//						max_runtime_keys = 8;
//						runtime_keys = (IndexRuntimeKeyInfo *)
//							palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//					}
//					else
//					{
//						max_runtime_keys *= 2;
//						runtime_keys = (IndexRuntimeKeyInfo *)
//							repalloc(runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//					}
//				}
//				runtime_keys[n_runtime_keys].scan_key = this_scan_key;
//				runtime_keys[n_runtime_keys].key_expr =
//					ExecInitExpr(rightop, planstate);
//				runtime_keys[n_runtime_keys].key_toastable =
//					TypeIsToastable(op_righttype);
//				n_runtime_keys++;
//				scanvalue = (Datum) 0;
//			}
//
//			/*
//			 * initialize the scan key's fields appropriately
//			 */
//			ScanKeyEntryInitialize(this_scan_key,
//								   flags,
//								   varattno,	/* attribute number to scan */
//								   op_strategy, /* op's strategy */
//								   op_righttype,		/* strategy subtype */
//								   ((OpExpr *) clause)->inputcollid,	/* collation */
//								   opfuncid,	/* reg proc to use */
//								   scanvalue);	/* constant */
//		}
//		else if (IsA(clause, RowCompareExpr))
//		{
//			/* (indexkey, indexkey, ...) op (expression, expression, ...) */
//			RowCompareExpr *rc = (RowCompareExpr *) clause;
//			ListCell   *largs_cell = list_head(rc->largs);
//			ListCell   *rargs_cell = list_head(rc->rargs);
//			ListCell   *opnos_cell = list_head(rc->opnos);
//			ListCell   *collids_cell = list_head(rc->inputcollids);
//			ScanKey		first_sub_key;
//			int			n_sub_key;
//
//			Assert(!isorderby);
//
//			first_sub_key = (ScanKey)
//				palloc(list_length(rc->opnos) * sizeof(ScanKeyData));
//			n_sub_key = 0;
//
//			/* Scan RowCompare columns and generate subsidiary ScanKey items */
//			while (opnos_cell != NULL)
//			{
//				ScanKey		this_sub_key = &first_sub_key[n_sub_key];
//				int			flags = SK_ROW_MEMBER;
//				Datum		scanvalue;
//				Oid			inputcollation;
//
//				/*
//				 * leftop should be the index key Var, possibly relabeled
//				 */
//				leftop = (Expr *) lfirst(largs_cell);
//				largs_cell = lnext(largs_cell);
//
//				if (leftop && IsA(leftop, RelabelType))
//					leftop = ((RelabelType *) leftop)->arg;
//
//				Assert(leftop != NULL);
//
//				if (!(IsA(leftop, Var)))
//					elog(ERROR, "indexqual doesn't have key on left side");
//
//				varattno = ((Var *) leftop)->varattno;
//
//				/*
//				 * We have to look up the operator's associated btree support
//				 * function
//				 */
//				opno = lfirst_oid(opnos_cell);
//				opnos_cell = lnext(opnos_cell);
//
//				if (index->rd_rel->relam != BTREE_AM_OID ||
//					varattno < 1 )
//					elog(ERROR, "bogus RowCompare index qualification");
//
////				opfamily = index->rd_opfamily[varattno - 1];
////
////				get_op_opfamily_properties(opno, opfamily, isorderby,
////										   &op_strategy,
////										   &op_lefttype,
////										   &op_righttype);
////
////				if (op_strategy != rc->rctype)
////					elog(ERROR, "RowCompare index qualification contains wrong operator");
////
////				opfuncid = get_opfamily_proc(opfamily,
////											 op_lefttype,
////											 op_righttype,
////											 BTORDER_PROC);
//
//				inputcollation = lfirst_oid(collids_cell);
//				collids_cell = lnext(collids_cell);
//
//				/*
//				 * rightop is the constant or variable comparison value
//				 */
//				rightop = (Expr *) lfirst(rargs_cell);
//				rargs_cell = lnext(rargs_cell);
//
//				if (rightop && IsA(rightop, RelabelType))
//					rightop = ((RelabelType *) rightop)->arg;
//
//				Assert(rightop != NULL);
//
//				if (IsA(rightop, Const))
//				{
//					/* OK, simple constant comparison value */
//					scanvalue = ((Const *) rightop)->constvalue;
//					if (((Const *) rightop)->constisnull)
//						flags |= SK_ISNULL;
//				}
//				else
//				{
//					/* Need to treat this one as a runtime key */
//					if (n_runtime_keys >= max_runtime_keys)
//					{
//						if (max_runtime_keys == 0)
//						{
//							max_runtime_keys = 8;
//							runtime_keys = (IndexRuntimeKeyInfo *)
//								palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//						}
//						else
//						{
//							max_runtime_keys *= 2;
//							runtime_keys = (IndexRuntimeKeyInfo *)
//								repalloc(runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//						}
//					}
//					runtime_keys[n_runtime_keys].scan_key = this_sub_key;
//					runtime_keys[n_runtime_keys].key_expr =
//						ExecInitExpr(rightop, planstate);
//					runtime_keys[n_runtime_keys].key_toastable =
//						TypeIsToastable(op_righttype);
//					n_runtime_keys++;
//					scanvalue = (Datum) 0;
//				}
//
//				/*
//				 * initialize the subsidiary scan key's fields appropriately
//				 */
//				ScanKeyEntryInitialize(this_sub_key,
//									   flags,
//									   varattno,		/* attribute number */
//									   op_strategy,		/* op's strategy */
//									   op_righttype,	/* strategy subtype */
//									   inputcollation,	/* collation */
//									   opfuncid,		/* reg proc to use */
//									   scanvalue);		/* constant */
//				n_sub_key++;
//			}
//
//			/* Mark the last subsidiary scankey correctly */
//			first_sub_key[n_sub_key - 1].sk_flags |= SK_ROW_END;
//
//			/*
//			 * We don't use ScanKeyEntryInitialize for the header because it
//			 * isn't going to contain a valid sk_func pointer.
//			 */
//			MemSet(this_scan_key, 0, sizeof(ScanKeyData));
//			this_scan_key->sk_flags = SK_ROW_HEADER;
//			this_scan_key->sk_attno = first_sub_key->sk_attno;
//			this_scan_key->sk_strategy = rc->rctype;
//			/* sk_subtype, sk_collation, sk_func not used in a header */
//			this_scan_key->sk_argument = PointerGetDatum(first_sub_key);
//		}
//		else if (IsA(clause, ScalarArrayOpExpr))
//		{
//			/* indexkey op ANY (array-expression) */
//			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
//			int			flags = 0;
//			Datum		scanvalue;
//
//			Assert(!isorderby);
//
//			Assert(saop->useOr);
//			opno = saop->opno;
//			opfuncid = saop->opfuncid;
//
//			/*
//			 * leftop should be the index key Var, possibly relabeled
//			 */
//			leftop = (Expr *) linitial(saop->args);
//
//			if (leftop && IsA(leftop, RelabelType))
//				leftop = ((RelabelType *) leftop)->arg;
//
//			Assert(leftop != NULL);
//
//			if (!(IsA(leftop, Var) ))
//				elog(ERROR, "indexqual doesn't have key on left side");
//
//			varattno = ((Var *) leftop)->varattno;
//			if (varattno < 1 )
//				elog(ERROR, "bogus index qualification");
//
////			/*
////			 * We have to look up the operator's strategy number.  This
////			 * provides a cross-check that the operator does match the index.
////			 */
////			opfamily = index->rd_opfamily[varattno - 1];
////
////			get_op_opfamily_properties(opno, opfamily, isorderby,
////									   &op_strategy,
////									   &op_lefttype,
////									   &op_righttype);
//
//			/*
//			 * rightop is the constant or variable array value
//			 */
//			rightop = (Expr *) lsecond(saop->args);
//
//			if (rightop && IsA(rightop, RelabelType))
//				rightop = ((RelabelType *) rightop)->arg;
//
//			Assert(rightop != NULL);
//
//			if (index->rd_am->amsearcharray)
//			{
//				/* Index AM will handle this like a simple operator */
//				flags |= SK_SEARCHARRAY;
//				if (IsA(rightop, Const))
//				{
//					/* OK, simple constant comparison value */
//					scanvalue = ((Const *) rightop)->constvalue;
//					if (((Const *) rightop)->constisnull)
//						flags |= SK_ISNULL;
//				}
//				else
//				{
//					/* Need to treat this one as a runtime key */
//					if (n_runtime_keys >= max_runtime_keys)
//					{
//						if (max_runtime_keys == 0)
//						{
//							max_runtime_keys = 8;
//							runtime_keys = (IndexRuntimeKeyInfo *)
//								palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//						}
//						else
//						{
//							max_runtime_keys *= 2;
//							runtime_keys = (IndexRuntimeKeyInfo *)
//								repalloc(runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
//						}
//					}
//					runtime_keys[n_runtime_keys].scan_key = this_scan_key;
//					runtime_keys[n_runtime_keys].key_expr =
//						ExecInitExpr(rightop, planstate);
//
//					/*
//					 * Careful here: the runtime expression is not of
//					 * op_righttype, but rather is an array of same; so
//					 * TypeIsToastable() isn't helpful.  However, we can
//					 * assume that all array types are toastable.
//					 */
//					runtime_keys[n_runtime_keys].key_toastable = true;
//					n_runtime_keys++;
//					scanvalue = (Datum) 0;
//				}
//			}
//			else
//			{
//				/* Executor has to expand the array value */
//				array_keys[n_array_keys].scan_key = this_scan_key;
//				array_keys[n_array_keys].array_expr =
//					ExecInitExpr(rightop, planstate);
//				/* the remaining fields were zeroed by palloc0 */
//				n_array_keys++;
//				scanvalue = (Datum) 0;
//			}
//
//			/*
//			 * initialize the scan key's fields appropriately
//			 */
//			ScanKeyEntryInitialize(this_scan_key,
//								   flags,
//								   varattno,	/* attribute number to scan */
//								   op_strategy, /* op's strategy */
//								   op_righttype,		/* strategy subtype */
//								   saop->inputcollid,	/* collation */
//								   opfuncid,	/* reg proc to use */
//								   scanvalue);	/* constant */
//		}
//		else if (IsA(clause, NullTest))
//		{
//			/* indexkey IS NULL or indexkey IS NOT NULL */
//			NullTest   *ntest = (NullTest *) clause;
//			int			flags;
//
//			Assert(!isorderby);
//
//			/*
//			 * argument should be the index key Var, possibly relabeled
//			 */
//			leftop = ntest->arg;
//
//			if (leftop && IsA(leftop, RelabelType))
//				leftop = ((RelabelType *) leftop)->arg;
//
//			Assert(leftop != NULL);
//
//			if (!(IsA(leftop, Var) &&
//				  ((Var *) leftop)->varno == INDEX_VAR))
//				elog(ERROR, "NullTest indexqual has wrong key");
//
//			varattno = ((Var *) leftop)->varattno;
//
//			/*
//			 * initialize the scan key's fields appropriately
//			 */
//			switch (ntest->nulltesttype)
//			{
//				case IS_NULL:
//					flags = SK_ISNULL | SK_SEARCHNULL;
//					break;
//				case IS_NOT_NULL:
//					flags = SK_ISNULL | SK_SEARCHNOTNULL;
//					break;
//				default:
//					elog(ERROR, "unrecognized nulltesttype: %d",
//						 (int) ntest->nulltesttype);
//					flags = 0;	/* keep compiler quiet */
//					break;
//			}
//
//			ScanKeyEntryInitialize(this_scan_key,
//								   flags,
//								   varattno,	/* attribute number to scan */
//								   InvalidStrategy,		/* no strategy */
//								   InvalidOid,	/* no strategy subtype */
//								   InvalidOid,	/* no collation */
//								   InvalidOid,	/* no reg proc for this */
//								   (Datum) 0);	/* constant */
//		}
//		else
//			elog(ERROR, "unsupported indexqual type: %d",
//				 (int) nodeTag(clause));
//	}
//
//	Assert(n_runtime_keys <= max_runtime_keys);
//
//	/* Get rid of any unused arrays */
//	if (n_array_keys == 0)
//	{
//		pfree(array_keys);
//		array_keys = NULL;
//	}
//
//	/*
//	 * Return info to our caller.
//	 */
//	*scanKeys = scan_keys;
//	*numScanKeys = n_scan_keys;
//	*runtimeKeys = runtime_keys;
//	*numRuntimeKeys = n_runtime_keys;
//	if (arrayKeys)
//	{
//		*arrayKeys = array_keys;
//		*numArrayKeys = n_array_keys;
//	}
//	else if (n_array_keys != 0)
//		elog(ERROR, "ScalarArrayOpExpr index qual found where not allowed");
//}
/***************************************************************************************************/

