/*-------------------------------------------------------------------------
 *
 * nodeHash.c
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHash.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */

#include "postgres.h"

#include <math.h>
#include <limits.h>

#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeMHashjoin.h"
#include "executor/nodeMultiJoin.h"
#include "miscadmin.h"
#include "utils/dynahash.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


typedef struct  uint32 HTIndexKey;


typedef struct HTIndexEntry
{
	uint32 		key;			/* Tuple id number (hashtable key) */
	uint32		ht_ptr;
} HTIndexEntry;

#define HTIENTRYSIZE	MAXALIGN(sizeof(HTIndexEntry))
bool
ExecMHash_get_op_hash_functions(Oid hashop,
					  RegProcedure *hashfn, bool isLeft);
static void
ExecChooseMultiHashTableSize(double ntuples, int tupwidth, int *numbuckets);
static void ExecHashIncreaseNumBatches(HashJoinTable hashtable);
static void ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node,
					  int mcvsToUse);
static void ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber);
static void ExecHashRemoveNextSkewBucket(HashJoinTable hashtable);
static void ExecMHashDumpBatch(MJoinTable hashtable);
static TupleTableSlot *  ExecMJoinGetNextTuple(HashState *hashtable);
static MinimalTuple copyMinmalTuple(MultiHashState *mhstate , MinimalTuple mtuple);
static void ExecMultiHashAllocateHashtable(SimpleHashTable hashtable);

/* ----------------------------------------------------------------
 *	Alex: Return and insert one tuple from  the outer node scan
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecHash(HashState *node) {
	PlanState *outerNode;
	List *hashkeys;
	MJoinTable hashtable;
	TupleTableSlot *slot = NULL;
	ExprContext *econtext;
	uint32 hashvalue;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);
	hashtable = (MJoinTable)node->hashtable;

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	if (hashtable->status == MHJ_EXAHUSTED || hashtable->status == MHJ_BUFFERED) {

		slot =  ExecMJoinGetNextTuple(node);
		if (TupIsNull(slot))
			return NULL;

		if (node->isOuter)
			econtext->ecxt_outertuple = slot;
		else
			econtext->ecxt_innertuple = slot;
		return slot;

	}
		else
		slot = ExecProcNode(outerNode);

		if (TupIsNull(slot))
			return NULL;

		/* We have to compute the hash value */
		if(node->isOuter)
		econtext->ecxt_outertuple = slot;
		else
		econtext->ecxt_innertuple = slot;

		if (ExecMHashGetHashValue(hashtable, econtext, hashkeys,hashtable->keepNulls, &hashvalue)) {

				/* Not subject to skew optimization, so insert normally */
			ExecMHashTableInsert(hashtable->parent,hashtable,slot, hashvalue, false);
			//	ExecHashTableInsert(hashtable, slot, hashvalue);

			hashtable->totalTuples += 1;

		}


	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, hashtable->totalTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return slot;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecHash(HashState *node)
{
	PlanState  *outerNode;
	List	   *hashkeys;
	HashJoinTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStartNode(node->ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);
	hashtable = node->hashtable;

	/*
	 * set expression context
	 */
	hashkeys = node->hashkeys;
	econtext = node->ps.ps_ExprContext;

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	for (;;)
	{
		slot = ExecProcNode(outerNode);
		if (TupIsNull(slot))
			break;
		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext, hashkeys,
								 false, hashtable->keepNulls,
								 &hashvalue))
		{
			int			bucketNumber;

			bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
			if (bucketNumber != INVALID_SKEW_BUCKET_NO)
			{
				/* It's a skew tuple, so put it into that hash table */
				ExecHashSkewTableInsert(hashtable, slot, hashvalue,
										bucketNumber);
			}
			else
			{
				/* Not subject to skew optimization, so insert normally */
				ExecHashTableInsert(hashtable, slot, hashvalue);
			}
			hashtable->totalTuples += 1;
		}
	}

	/* must provide our own instrumentation support */
	if (node->ps.instrument)
		InstrStopNode(node->ps.instrument, hashtable->totalTuples);

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}

TupleTableSlot *
ExecMultiHash(MultiHashState *node) {
	PlanState  *outerNode;
	List	   *hashkeys;
	SimpleHashTable hashtable;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;
	ListCell *lc;
	MinimalTuple mtuple = NULL;
	MinimalTuple  hashTuple = NULL;
	bool inserted = false;

	/* must provide our own instrumentation support */
	if (node->hstate.ps.instrument)
		InstrStartNode(node->hstate.ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);

	econtext = node->hstate.ps.ps_ExprContext;
	Assert(node->currChunk != NULL);
	for (;;)
	{
		if (node->currTuple) {
			mtuple = node->currTuple;
			node->currTuple = (MinimalTuple )ChunkGetNextTuple(node->currChunk,node->currTuple);
			slot = econtext->ecxt_innertuple;
			ExecClearTuple(slot);
			slot = ExecStoreMinimalTuple(mtuple, slot, false);
			econtext->ecxt_innertuple = slot;
			return slot;
		} else
			break;

			slot = ExecProcNode(outerNode);
			if (TupIsNull(slot))
				break;
			/* We have to compute the hash value */
			econtext->ecxt_innertuple = slot;

			mtuple = ExecFetchSlotMinimalTuple(slot);



			hashTuple = JC_StoreMinmalTuple(node->currChunk,mtuple);



			foreach(lc,node->all_hashkeys) {

				HashInfo * hinfo = (HashInfo *) lfirst(lc);

				hashtable = node->hashable_array[hinfo->id];
				//printf("idx : %d  ",hinfo->id );

				/*
				 * set expression context
				 */
				hashkeys = hinfo->hashkeys;
				//pprint(hashkeys);

				if (ExecMultiHashGetHashValue(hashtable,
						econtext,
						hashkeys,
						false,
						hashtable->keepNulls,
						&hashvalue)) {
					inserted = true;
					/* Not subject to skew optimization, so insert normally */
					ExecMultiHashTableInsert(hashtable, hashTuple, hashvalue);
					hashtable->totalTuples += 1;
				}

				/* must provide our own instrumentation support */
					if (node->hstate.ps.instrument)
						InstrStopNode(node->hstate.ps.instrument, hashtable->totalTuples);
			}
			if(inserted)
					return slot;
	}


	return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node *
MultiExecMultiHash(MultiHashState *node)
{
	PlanState  *outerNode;
	List	   *hashkeys;
	SimpleHashTable hashtable;
	ScanState *scan;
	TupleTableSlot *slot;
	ExprContext *econtext;
	uint32		hashvalue;
	ListCell *lc;
		/* must provide our own instrumentation support */


	Assert(node->currChunk != NULL);



	if (node->hstate.ps.instrument)
		InstrStartNode(node->hstate.ps.instrument);

	/*
	 * get state info from node
	 */
	outerNode = outerPlanState(node);

	econtext = node->hstate.ps.ps_ExprContext;
	scan = (ScanState *)outerNode;

	/*
	 * get all inner tuples and insert into the hash table (or temp files)
	 */
	for (;;)
	{
		MinimalTuple mtuple = NULL;
		MinimalTuple  hashTuple = NULL;



		slot = ExecProcNode(outerNode);



		if (TupIsNull(slot)) {

			// it's our last chunk
			if(node->currChunk->state == CH_WAITTING && scan->es_scanBytes > 0){
				node->needUpdate = true;

				break;
			}

			// we are reading an dropped chunk so rescan to refill and update the supblans
			if(node->currChunk->state == CH_DROPPED){

			node->needUpdate = true;
			printf("CALLING RESCAN in %d scanbytes %d\n", outerNode->type, scan->es_scanBytes);
			fflush(stdout);

			ExecReScan(outerNode);

			continue;
			}


			return NULL;

		}

		node->started = true;

		/* We have to compute the hash value */
		econtext->ecxt_innertuple = slot;

		mtuple = ExecFetchSlotMinimalTuple(slot);
		scan->es_scanBytes++;

		hashTuple = JC_StoreMinmalTuple(node->currChunk,mtuple);

		foreach(lc,node->all_hashkeys) {

			HashInfo * hinfo = (HashInfo *) lfirst(lc);

			if (node->hashable_array[hinfo->id] == NULL) {
				ExecMultiHashTableCreate(node,
						hinfo->hoperators,
						false,
						&node->hashable_array[hinfo->id],
						scan->len,
						node->currChunk->pages);

			}

			hashtable = node->hashable_array[hinfo->id];
			/*
			 * set expression context
			 */
			hashkeys = hinfo->hashkeys;

			if (ExecMultiHashGetHashValue(hashtable,
					econtext,
					hashkeys,
					false,
					hashtable->keepNulls,
					&hashvalue)) {

				/* Not subject to skew optimization, so insert normally */
				ExecMultiHashTableInsert(hashtable, hashTuple, hashvalue);

				hashtable->totalTuples += 1;
			}
			else{

				if(hashTuple!=NULL){

						pfree(hashTuple);
				}
			}
			/* must provide our own instrumentation support */
				if (node->hstate.ps.instrument)
					InstrStopNode(node->hstate.ps.instrument, hashtable->totalTuples);


		}

		//todo renata raja - do computattion for num tuples!!!
		//if (scan->es_scanBytes == multi_join_chunk_tup || scan->es_scanBytes  == node->currChunk->tuples) {
		if ((multi_join_tuple_count && scan->es_scanBytes == multi_join_chunk_tup )|| scan->es_scanBytes  == node->currChunk->tuples) {
			node->chunkIds = bms_add_member(node->chunkIds, (int)ChunkGetID(node->currChunk));
			break;
		}


	}

	if(hashtable && node->currChunk->state == CH_WAITTING)
		node->currChunk->tuples = hashtable->totalTuples;

	node->currChunk->state = CH_READ;
	node->chunkIds = bms_add_member(node->chunkIds,(int) ChunkGetID(node->currChunk));


	node->lchunks = lappend(node->lchunks , node->currChunk);
	scan->es_scanBytes = 0;


	foreach(lc,node->all_hashkeys) {

		HashInfo * hinfo = (HashInfo *) lfirst(lc);

		hashtable = node->hashable_array[hinfo->id];

		printf("\nTotal tuples for hashtable %d  : %0.lf \n",
				hinfo->id,
				hashtable->totalTuples);
		fflush(stdout);

	}

	/*
	 * We do not return the hash table directly because it's not a subtype of
	 * Node, and so would violate the MultiExecProcNode API.  Instead, our
	 * parent Hashjoin node is expected to know how to fish it out of our node
	 * state.  Ugly but not really worth cleaning up, since Hashjoin knows
	 * quite a bit more about Hash besides that.
	 */
	return NULL;
}


/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for MultiHash node
 * ----------------------------------------------------------------
 */



HashState *
ExecInitMultiHash(MultiHash *node, EState *estate, int eflags)
{
	HashState  *hashstate;
	ListCell *lc;
	MultiHashState * mhashstate;
	ScanState *scanNode;

	List * hashkeys_list= NIL;

	if(node->ps != NULL)
		return (HashState * )node->ps;


	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = (HashState  *)makeNode(MultiHashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize our result slot
	 */
	ExecInitResultTupleSlot(estate, &hashstate->ps);

	/*
	 * initialize child expressions
	 */
	hashstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->hash.plan.targetlist,
					 (PlanState *) hashstate);
	hashstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->hash.plan.qual,
					 (PlanState *) hashstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);
	scanNode = (ScanState *)outerPlanState(hashstate);



	scanNode->es_scanBytes  = 0;
//	pprint(node->hash.plan.targetlist);
	/*
	 * initialize tuple type. no need to initialize projection info because
	 * this node doesn't do projections
	 */
	ExecAssignResultTypeFromTL(&hashstate->ps);
	hashstate->ps.ps_ProjInfo = NULL;

	printf("\n Hash node with %d attributes \n", hashstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts);

	mhashstate = (MultiHashState *)hashstate;
	mhashstate->all_hashkeys = NIL;

	mhashstate->tupCxt = AllocSetContextCreate(CurrentMemoryContext,
													   "HashTable tuple Context",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);
	mhashstate->nun_hashtables = 0;
	mhashstate->num_chunks = node->num_chunks;
	mhashstate->hashable_array = NULL;
	mhashstate->chunk_hashables = NULL;

	mhashstate->lchunks = NIL;
	mhashstate->currChunk = NULL;
	mhashstate->chunkIds = NULL;
	mhashstate->needUpdate = false;
	mhashstate->allChunks = node->chunks;
	mhashstate->relid = ((Scan *)scanNode->ps.plan)->scanrelid;

	printf("\nINIT MULTI HASH REL %d  \n", mhashstate->relid);
		/*
		 * Create temporary memory contexts in which to keep the hashtable working
		 * storage.  See notes in executor/hashjoin.h.
		 */

	node->ps = (PlanState *)mhashstate;
	return hashstate;
}



/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */


HashState *
ExecInitHash(Hash *node, EState *estate, int eflags)
{
	HashState  *hashstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hashstate = makeNode(HashState);
	hashstate->ps.plan = (Plan *) node;
	hashstate->ps.state = estate;
	hashstate->hashtable = NULL;
	hashstate->hashkeys = NIL;	/* will be set by parent HashJoin */

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hashstate->ps);

	/*
	 * initialize our result slot
	 */
	ExecInitResultTupleSlot(estate, &hashstate->ps);

	/*
	 * initialize child expressions
	 */
	hashstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) hashstate);
	hashstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) hashstate);

	/*
	 * initialize child nodes
	 */
	outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type. no need to initialize projection info because
	 * this node doesn't do projections
	 */
	ExecAssignResultTypeFromTL(&hashstate->ps);
	hashstate->ps.ps_ProjInfo = NULL;

	return hashstate;
}






/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void
ExecEndHash(HashState *node)
{
	PlanState  *outerPlan;

	/*
	 * free exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * shut down the subplan
	 */
	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}


void build_outer_hashkeys(List *hashkeys){

	ListCell *lc;

	foreach(lc, hashkeys){

		ExprState * expr = (ExprState *)lfirst(lc);
		Var		   *variable = (Var *) expr->expr;
		variable->varno = OUTER_VAR;

	}

}
/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
void ExecMultiHashCreateHashTables(MultiHashState * mhstate){
	if (mhstate->nun_hashtables == 0) {
		ListCell *lc;
		List * hkeysList = mhstate->all_hashkeys;
		int num_htables= list_length(hkeysList);
		int htidx = 0;
		int i;
		Index relid;
		MemoryContext oldctx;
		mhstate->nun_hashtables = num_htables;

		oldctx = MemoryContextSwitchTo(mhstate->tupCxt);

		mhstate->chunk_hashables = (SimpleHashTable **) palloc0(mhstate->num_chunks *  sizeof(SimpleHashTable) );

		MemoryContextSwitchTo(oldctx);
		Assert(mhstate->chunk_hashables  != NULL);
		relid = mhstate->relid;
		pprint(hkeysList);
		for(i = 0 ; i<mhstate->num_chunks ; i++ ){
		//mhstate->hashable_array =
		mhstate->chunk_hashables[i] =(SimpleHashTable *) palloc0(num_htables * sizeof(SimpleHashTable) );
		htidx = 0;

			foreach(lc, hkeysList) {
				HashInfo * hinfo = (HashInfo *) lfirst(lc);

//				 ExecMultiHashTableCreate(mhstate,
//						hinfo->hoperators,
//						false, &mhstate->chunk_hashables[i][htidx]);
				mhstate->chunk_hashables[i][htidx] =NULL;

				hinfo->id = htidx;
				htidx++;
			}


		}
		mhstate->hashable_array = NULL;

		printf("Created %d hash tables for relation %d \n",
				num_htables,relid);


		fflush(stdout);
	}

}
SimpleHashTable ExecChooseHashTable(MultiHashState * mhstate, List *hoperators, List *hashkeys, HashInfo **hinfo){

	HashInfo * dummy_hinfo = makeNode(HashInfo);
	HashInfo *result = NULL;
	Index idx;
	dummy_hinfo->hashkeys = hashkeys;
	dummy_hinfo->hoperators = hoperators;



	result = list_member_return(mhstate->all_hashkeys, dummy_hinfo);
	*hinfo = result;
	idx = ((HashInfo *) result)->id;
	Assert(result != NULL);
	pfree(dummy_hinfo);

	Assert(mhstate->currChunk != NULL);

	mhstate->hashable_array = mhstate->chunk_hashables[ChunkGetID(mhstate->currChunk)];

	mhstate->hashable_array[idx]->totalTuples = mhstate->currChunk->tuples;
	return mhstate->hashable_array[idx];



}
HashJoinTable
ExecHashTableCreate(Hash *node, List *hashOperators, bool keepNulls)
{
	HashJoinTable hashtable;
	Plan	   *outerNode;
	int			nbuckets;
	int			nbatch;
	int			num_skew_mcvs;
	int			log2_nbuckets;
	int			nkeys;
	int			i;
	ListCell   *ho;
	MemoryContext oldcxt;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	outerNode = outerPlan(node);

	ExecChooseHashTableSize(outerNode->plan_rows, outerNode->plan_width,
							OidIsValid(node->skewTable),
							&nbuckets, &nbatch, &num_skew_mcvs);

#ifdef HJDEBUG
	printf("nbatch = %d, nbuckets = %d\n", nbatch, nbuckets);
#endif

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	hashtable = (HashJoinTable) palloc(sizeof(HashJoinTableData));
	hashtable->nbuckets = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->buckets = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->skewEnabled = false;
	hashtable->skewBucket = NULL;
	hashtable->skewBucketLen = 0;
	hashtable->nSkewBuckets = 0;
	hashtable->skewBucketNums = NULL;
	hashtable->nbatch = nbatch;
	hashtable->curbatch = 0;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;
	hashtable->growEnabled = true;
	hashtable->totalTuples = 0;
	hashtable->innerBatchFile = NULL;
	hashtable->outerBatchFile = NULL;
	hashtable->spaceUsed = 0;
	hashtable->spacePeak = 0;
	hashtable->spaceAllowed = work_mem * 1024L;
	hashtable->spaceUsedSkew = 0;
	hashtable->spaceAllowedSkew =
	hashtable->spaceAllowed * SKEW_WORK_MEM_PERCENT / 100;

	/*
	 * Get info about the hash functions to be used for each hash key. Also
	 * remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->outer_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->inner_hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));
	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	i = 0;
	foreach(ho, hashOperators)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			left_hashfn;
		Oid			right_hashfn;

		if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
		fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
		hashtable->hashStrict[i] = op_strict(hashop);
		i++;
	}

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	/* Allocate data that will live for the life of the hashjoin */

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (nbatch > 1)
	{
		/*
		 * allocate and initialize the file arrays in hashCxt
		 */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* The files will not be opened until needed... */
		/* ... but make sure we have temp tablespaces established for them */
		PrepareTempTablespaces();
	}

	/*
	 * Prepare context for the first-scan space allocations; allocate the
	 * hashbucket array therein, and set each bucket "empty".
	 */
	MemoryContextSwitchTo(hashtable->batchCxt);

	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	/*
	 * Set up for skew optimization, if possible and there's a need for more
	 * than one batch.	(In a one-batch join, there's no point in it.)
	 */
//	if (nbatch > 1)
//		ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);

	MemoryContextSwitchTo(oldcxt);
	printf("\n Hash table created for %d entries \n", hashtable->nbuckets);
	printf("\n TOTAL BACTHES  %d  \n", hashtable->nbatch);
	return hashtable;
}

/* ----------------------------------------------------------------
 *		ExecMultiHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */

void ExecMultiHashTableCreate(MultiHashState *node, List *hashOperators, bool keepNulls,
		SimpleHashTable * hashtableptr,  int tupwidth, int pages)
{
	SimpleHashTable hashtable;
	Plan	   *outerNode;
	int			nbuckets;

	int			log2_nbuckets;
	int			nkeys;
	MemoryContext	oldcxt;
	ListCell   *ho;

	int i;


	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */
	outerNode =  node->hstate.ps.lefttree->plan;

	ExecChooseMultiHashTableSize(pages, tupwidth,&nbuckets);


///#ifdef HJDEBUG
	printf("tupwidth : %d, nbuckets = %d\n",  tupwidth, nbuckets);

//#endif

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	oldcxt =  MemoryContextSwitchTo(node->tupCxt);

	hashtable = (SimpleHashTable) palloc0(sizeof(SimpleHashTableData));

	MemoryContextSwitchTo(oldcxt);

	hashtable->nbuckets = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->buckets = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->growEnabled = true;
	hashtable->totalTuples = 0;
	hashtable->spaceUsed = 0;
	hashtable->spaceAllowed = multi_join_chunk_size * 1024L;


	/*
		 * Get info about the hash functions to be used for each hash key. Also
		 * remember whether the join operators are strict.
		 */
		nkeys = list_length(hashOperators);
		hashtable->hashfunctions =
			(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));

		hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
		i = 0;
		foreach(ho, hashOperators)
		{
			Oid			hashop = lfirst_oid(ho);
			Oid			hashfn;


			if (!ExecMHash_get_op_hash_functions(hashop, &hashfn, false))
				elog(ERROR, "could not find hash function for hash operator %u",
					 hashop);
			fmgr_info(hashfn, &hashtable->hashfunctions[i]);

			hashtable->hashStrict[i] = op_strict(hashop);
			i++;
		}


	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
												   "HashTableContext",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);



	/* Allocate data that will live for the life of the hashjoin */

	ExecMultiHashAllocateHashtable(hashtable);

	*hashtableptr = hashtable;

}

/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
MJoinTable
ExecMHashTableCreate(Hash *node, List *hashOperators, bool keepNulls, bool isLeft, int nbuckets, int nbatch)
{
	MJoinTable hashtable;

	int			log2_nbuckets;
	int			nkeys;
	int			i;
	ListCell   *ho;
	MemoryContext oldcxt;

	/*
	 * Get information about the size of the relation to be hashed (it's the
	 * "outer" subtree of this node, but the inner relation of the hashjoin).
	 * Compute the appropriate size of the hash table.
	 */



#ifdef HJDEBUG
	printf("nbatch = %d, nbuckets = %d\n", nbatch, nbuckets);
#endif

	/* nbuckets must be a power of 2 */
	log2_nbuckets = my_log2(nbuckets);
	Assert(nbuckets == (1 << log2_nbuckets));

	/*
	 * Initialize the hash table control block.
	 *
	 * The hashtable control block is just palloc'd from the executor's
	 * per-query memory context.
	 */
	hashtable = (MJoinTable) palloc(sizeof(MJoinTableData));
	hashtable->nbuckets = nbuckets;
	hashtable->log2_nbuckets = log2_nbuckets;
	hashtable->buckets = NULL;
	hashtable->keepNulls = keepNulls;
	hashtable->nbatch = nbatch;
	hashtable->curbatch = 0;
	hashtable->nbatch_original = nbatch;
	hashtable->nbatch_outstart = nbatch;
	hashtable->growEnabled = true;
	hashtable->totalTuples = 0;
	hashtable->batches = NULL;

	hashtable->spaceUsed = 0;
	hashtable->spacePeak = 0;
	hashtable->spaceAllowed = work_mem * 1024L;

	hashtable->status = MHJ_EMPTY;
	hashtable->nInserted = 0;
	/*
	 * Get info about the hash functions to be used for each hash key. Also
	 * remember whether the join operators are strict.
	 */
	nkeys = list_length(hashOperators);
	hashtable->hashfunctions =
		(FmgrInfo *) palloc(nkeys * sizeof(FmgrInfo));

	hashtable->hashStrict = (bool *) palloc(nkeys * sizeof(bool));
	i = 0;
	foreach(ho, hashOperators)
	{
		Oid			hashop = lfirst_oid(ho);
		Oid			hashfn;


		if (!ExecMHash_get_op_hash_functions(hashop, &hashfn, isLeft))
			elog(ERROR, "could not find hash function for hash operator %u",
				 hashop);
		fmgr_info(hashfn, &hashtable->hashfunctions[i]);

		hashtable->hashStrict[i] = op_strict(hashop);
		i++;
	}

	/*
	 * Create temporary memory contexts in which to keep the hashtable working
	 * storage.  See notes in executor/hashjoin.h.
	 */
	hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
											   "HashTableContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
												"HashBatchContext",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

	/* Allocate data that will live for the life of the hashjoin */

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);


		/*
		 * allocate and initialize the file arrays in hashCxt
		 */

		hashtable->batches =palloc0(nbatch * sizeof(MJoinBatchDesc));

		for (i = 0; i< nbatch ; i++){

			hashtable->batches[i] = palloc(sizeof(MJoinBatchData));
			ExecMHashJoinResetBatch(hashtable->batches[i]);
			hashtable->batches[i]->spaceAllowed = hashtable->spaceAllowed;

		}
//		hashtable->batchFile = (BufFile **)
//			palloc0(nbatch * sizeof(BufFile *));
			/* The files will not be opened until needed... */
		/* ... but make sure we have temp tablespaces established for them */
		PrepareTempTablespaces();


	/*
	 * Prepare context for the first-scan space allocations; allocate the
	 * hashbucket array therein, and set each bucket "empty".
	 */
	MemoryContextSwitchTo(hashtable->batchCxt);

	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));


	MemoryContextSwitchTo(oldcxt);

	return hashtable;
}


bool
ExecMHash_get_op_hash_functions(Oid hashop,
					  RegProcedure *hashfn, bool isLeft){

	if(isLeft)
		return get_op_hash_functions(hashop, hashfn,NULL);
	else
		return get_op_hash_functions(hashop, NULL,hashfn);




}
/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */

/* Target bucket loading (tuples per bucket) */
#define NTUP_PER_BUCKET			10

static void
ExecChooseMultiHashTableSize(double ntuples, int tupwidth, int *numbuckets)
{

	int nbuckets;

//	if (multi_join_tuple_count)
//		nbuckets = multi_join_chunk_tup;
//	else
	nbuckets =  ceil ((ntuples * BLCKSZ) / tupwidth);

	nbuckets = Min(nbuckets, ntuples);
	nbuckets = ceil(nbuckets / NTUP_PER_BUCKET);
	nbuckets = 1 << my_log2(nbuckets);

	*numbuckets = nbuckets;


}

void
ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs)
{
		int			tupsize;
		double		inner_rel_bytes;
		long		bucket_bytes;
		long		hash_table_bytes;
		long		skew_table_bytes;
		long		max_pointers;
		int			nbatch = 1;
		int			nbuckets;
		double		dbuckets;

		/* Force a plausible relation size if no info */
		if (ntuples <= 0.0)
			ntuples = 1000.0;

		/*
		 * Estimate tupsize based on footprint of tuple in hashtable... note this
		 * does not allow for any palloc overhead.  The manipulations of spaceUsed
		 * don't count palloc overhead either.
		 */
		tupsize = HJTUPLE_OVERHEAD +
			MAXALIGN(sizeof(MinimalTuple)) +
			MAXALIGN(tupwidth);
		inner_rel_bytes = ntuples * tupsize;

		/*
		 * Target in-memory hashtable size is work_mem kilobytes.
		 */
		hash_table_bytes = work_mem * 1024L;

		/*
		 * If skew optimization is possible, estimate the number of skew buckets
		 * that will fit in the memory allowed, and decrement the assumed space
		 * available for the main hash table accordingly.
		 *
		 * We make the optimistic assumption that each skew bucket will contain
		 * one inner-relation tuple.  If that turns out to be low, we will recover
		 * at runtime by reducing the number of skew buckets.
		 *
		 * hashtable->skewBucket will have up to 8 times as many HashSkewBucket
		 * pointers as the number of MCVs we allow, since ExecHashBuildSkewHash
		 * will round up to the next power of 2 and then multiply by 4 to reduce
		 * collisions.
		 */
		if (useskew)
		{
			skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

			/*----------
			 * Divisor is:
			 * size of a hash tuple +
			 * worst-case size of skewBucket[] per MCV +
			 * size of skewBucketNums[] entry +
			 * size of skew bucket struct itself
			 *----------
			 */
			*num_skew_mcvs = skew_table_bytes / (tupsize +
												 (8 * sizeof(HashSkewBucket *)) +
												 sizeof(int) +
												 SKEW_BUCKET_OVERHEAD);
			if (*num_skew_mcvs > 0)
				hash_table_bytes -= skew_table_bytes;
		}
		else
			*num_skew_mcvs = 0;

		/*
		 * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
		 * memory is filled, assuming a single batch.  The Min() step limits the
		 * results so that the pointer arrays we'll try to allocate do not exceed
		 * work_mem.
		 */
		max_pointers = (work_mem * 1024L) / sizeof(void *);
		/* also ensure we avoid integer overflow in nbatch and nbuckets */
		max_pointers = Min(max_pointers, INT_MAX / 2);
		dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
		dbuckets = Min(dbuckets, max_pointers);
		nbuckets = Max((int) dbuckets, 1024);
		nbuckets = 1 << my_log2(nbuckets);
		bucket_bytes = sizeof(HashJoinTuple) * nbuckets;

		/*
		 * If there's not enough space to store the projected number of tuples
		 * and the required bucket headers, we will need multiple batches.
		 */
		if (inner_rel_bytes + bucket_bytes > hash_table_bytes)
		{
			/* We'll need multiple batches */
			long		lbuckets;
			double		dbatch;
			int			minbatch;
			long		bucket_size;

			/*
			 * Estimate the number of buckets we'll want to have when work_mem
			 * is entirely full.  Each bucket will contain a bucket pointer plus
			 * NTUP_PER_BUCKET tuples, whose projected size already includes
			 * overhead for the hash code, pointer to the next tuple, etc.
			 */
			bucket_size = (tupsize * NTUP_PER_BUCKET + sizeof(HashJoinTuple));
			lbuckets = 1 << my_log2(hash_table_bytes / bucket_size);
			lbuckets = Min(lbuckets, max_pointers);
			nbuckets = (int) lbuckets;
			bucket_bytes = nbuckets * sizeof(HashJoinTuple);

			/*
			 * Buckets are simple pointers to hashjoin tuples, while tupsize
			 * includes the pointer, hash code, and MinimalTupleData.  So buckets
			 * should never really exceed 25% of work_mem (even for
			 * NTUP_PER_BUCKET=1); except maybe * for work_mem values that are
			 * not 2^N bytes, where we might get more * because of doubling.
			 * So let's look for 50% here.
			 */
			Assert(bucket_bytes <= hash_table_bytes / 2);

			/* Calculate required number of batches. */
			dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
			dbatch = Min(dbatch, max_pointers);
			minbatch = (int) dbatch;
			nbatch = 2;
			while (nbatch < minbatch)
				nbatch <<= 1;
		}

		*numbuckets = nbuckets;
		*numbatches = nbatch;
}


/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecMHashTableDestroy(MJoinTable hashtable)
{
	int			i;

	/*
	 * Make sure all the temp files are closed.  We skip batch 0, since it
	 * can't have any temp files (and the arrays might not even exist if
	 * nbatch is only 1).
	 */
	for (i = 0; i < hashtable->nbatch; i++)
	{   MJoinBatchDesc batch =hashtable->batches[i];
		if (batch->batchFile){

			BufFileClose(batch->batchFile);

		}

	}

	/* Release working memory (batchCxt is a child, so it goes away too) */


	/* And drop the control block */

	pfree(hashtable);


}

/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void
ExecHashTableDestroy(HashJoinTable hashtable)
{
	int			i;

	/*
	 * Make sure all the temp files are closed.  We skip batch 0, since it
	 * can't have any temp files (and the arrays might not even exist if
	 * nbatch is only 1).
	 */
	for (i = 1; i < hashtable->nbatch; i++)
	{
		if (hashtable->innerBatchFile[i])
			BufFileClose(hashtable->innerBatchFile[i]);
		if (hashtable->outerBatchFile[i])
			BufFileClose(hashtable->outerBatchFile[i]);
	}

	/* Release working memory (batchCxt is a child, so it goes away too) */
	MemoryContextDelete(hashtable->hashCxt);

	/* And drop the control block */
	pfree(hashtable);
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
static void
ExecHashIncreaseNumBatches(HashJoinTable hashtable)
{
	int			oldnbatch = hashtable->nbatch;
	int			curbatch = hashtable->curbatch;
	int			nbatch;
	int			i;
	MemoryContext oldcxt;
	long		ninmemory;
	long		nfreed;

	printf("Incresing batches\n");

	/* do nothing if we've decided to shut off growth */
	if (!hashtable->growEnabled)
		return;

	/* safety check to avoid overflow */
	if (oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2)))
		return;

	nbatch = oldnbatch * 2;
	Assert(nbatch > 1);

#ifdef HJDEBUG
	printf("Increasing nbatch to %d because space = %lu\n",
		   nbatch, (unsigned long) hashtable->spaceUsed);
#endif

	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

	if (hashtable->innerBatchFile == NULL)
	{
		/* we had no file arrays before */
		hashtable->innerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			palloc0(nbatch * sizeof(BufFile *));
		/* time to establish the temp tablespaces, too */
		PrepareTempTablespaces();
	}
	else
	{
		/* enlarge arrays and zero out added entries */
		hashtable->innerBatchFile = (BufFile **)
			repalloc(hashtable->innerBatchFile, nbatch * sizeof(BufFile *));
		hashtable->outerBatchFile = (BufFile **)
			repalloc(hashtable->outerBatchFile, nbatch * sizeof(BufFile *));
		MemSet(hashtable->innerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
		MemSet(hashtable->outerBatchFile + oldnbatch, 0,
			   (nbatch - oldnbatch) * sizeof(BufFile *));
	}

	MemoryContextSwitchTo(oldcxt);

	hashtable->nbatch = nbatch;

	/*
	 * Scan through the existing hash table entries and dump out any that are
	 * no longer of the current batch.
	 */
	ninmemory = nfreed = 0;

	for (i = 0; i < hashtable->nbuckets; i++)
	{
		HashJoinTuple prevtuple;
		HashJoinTuple tuple;

		prevtuple = NULL;
		tuple = hashtable->buckets[i];

		while (tuple != NULL)
		{
			/* save link in case we delete */
			HashJoinTuple nexttuple = tuple->next;
			int			bucketno;
			int			batchno;

			ninmemory++;
			ExecHashGetBucketAndBatch(hashtable, tuple->hashvalue,
									  &bucketno, &batchno);
			Assert(bucketno == i);
			if (batchno == curbatch)
			{
				/* keep tuple */
				prevtuple = tuple;
			}
			else
			{
				/* dump it out */
				Assert(batchno > curbatch);
				ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(tuple),
									  tuple->hashvalue,
									  &hashtable->innerBatchFile[batchno]);
				/* and remove from hash table */
				if (prevtuple)
					prevtuple->next = nexttuple;
				else
					hashtable->buckets[i] = nexttuple;
				/* prevtuple doesn't change */
				hashtable->spaceUsed -=
					HJTUPLE_OVERHEAD + HJTUPLE_MINTUPLE(tuple)->t_len;
				pfree(tuple);
				nfreed++;
			}

			tuple = nexttuple;
		}
	}

#ifdef HJDEBUG
	printf("Freed %ld of %ld tuples, space now %lu\n",
		   nfreed, ninmemory, (unsigned long) hashtable->spaceUsed);
#endif

	/*
	 * If we dumped out either all or none of the tuples in the table, disable
	 * further expansion of nbatch.  This situation implies that we have
	 * enough tuples of identical hashvalues to overflow spaceAllowed.
	 * Increasing nbatch will not fix it since there's no way to subdivide the
	 * group any more finely. We have to just gut it out and hope the server
	 * has enough RAM.
	 */
	if (nfreed == 0 || nfreed == ninmemory)
	{
		hashtable->growEnabled = false;
#ifdef HJDEBUG
		printf("Disabling further increase of nbatch\n");
#endif
	}
}

static void ExecMHashDumpBatch(MJoinTable hashtable) {

	int curbatchno = hashtable->curbatch;
	MJoinBatchDesc curbatch = hashtable->batches[curbatchno];
	HashJoinTuple tuple;
	long ninmemory;
	long nfreed;

	int i;
	/*
	 * Scan through the existing hash table entries and dump out any that are
	 * no longer of the current batch.
	 */

	curbatch->nentries = 0;
	curbatch->spaceUsed = 0;
	hashtable->spaceUsed = 0;


	ninmemory = nfreed = 0;

	for (i = 0; i < hashtable->nbuckets; i++) {
		HashJoinTuple prevtuple;


		prevtuple = NULL;
		tuple = hashtable->buckets[i];

		while (tuple != NULL) {
			/* save link in case we delete */
			HashJoinTuple nexttuple = tuple->next;
			int bucketno;
			int batchno;
			int tuplesize = HJTUPLE_OVERHEAD + HJTUPLE_MINTUPLE(tuple)->t_len;

			ninmemory++;
			ExecHashGetBucketAndBatch((HashJoinTable) hashtable, tuple->hashvalue, &bucketno, &batchno);

			if (batchno == curbatchno) {
				/* keep tuple */
				curbatch->nentries++;
				prevtuple = tuple;
				hashtable->spaceUsed += tuplesize;
				curbatch->spaceUsed +=tuplesize;
			} else {
				MJoinBatchDesc batch = hashtable->batches[batchno];
				/* dump it out */
				Assert(batchno > curbatchno);

				ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(tuple), tuple->hashvalue, &batch->savedFile);
				/* and remove from hash table */
				if (prevtuple)
					prevtuple->next = nexttuple;
				else
					hashtable->buckets[i] = nexttuple;
				/* prevtuple doesn't change */

				pfree(tuple);
				batch->nentries++;
				curbatch->spaceUsed +=tuplesize;
				hashtable->nInserted--;
				nfreed++;
			}

			tuple = nexttuple;
		}
	}

	printf("Num of saved tuples : %ld\n", nfreed);


#ifdef HJDEBUG
	printf("Freed %ld of %ld tuples, space now %lu\n",
			nfreed, ninmemory, (unsigned long) hashtable->spaceUsed);
#endif

	/*
	 * If we dumped out either all or none of the tuples in the table, disable
	 * further expansion of nbatch.  This situation implies that we have
	 * enough tuples of identical hashvalues to overflow spaceAllowed.
	 * Increasing nbatch will not fix it since there's no way to subdivide the
	 * group any more finely. We have to just gut it out and hope the server
	 * has enough RAM.
//	 */
//	if (nfreed == 0 || nfreed == ninmemory) {
//		hashtable->growEnabled = false;
//#ifdef HJDEBUG
//		printf("Disabling further increase of nbatch\n");
//#endif
//	}
}
/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
void
ExecMHashIncreaseNumBatches(SymHashJoinState * mhjstate)
{
	MJoinTable innerhashtable = mhjstate->mhj_InnerHashTable;
	MJoinTable outerhashtable = mhjstate->mhj_OuterHashTable;
	int			oldnbatch = innerhashtable->nbatch;
	int			nbatch;
	int bno = 0;

	MemoryContext oldcxt;




	/* safety check to avoid overflow */
	if (oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void *) * 2)))
		return;

	nbatch = oldnbatch * 2;
	Assert(nbatch > 1);

#ifdef HJDEBUG
	printf("Increasing nbatch to %d because space = %lu\n",
		   nbatch, (unsigned long) hashtable->spaceUsed);
#endif



	oldcxt = MemoryContextSwitchTo(innerhashtable->hashCxt);

	innerhashtable->batches = (MJoinBatchDesc *) repalloc(innerhashtable->batches, nbatch * sizeof(MJoinBatchDesc));

	MemoryContextSwitchTo(outerhashtable->hashCxt);

	outerhashtable->batches = (MJoinBatchDesc *) repalloc(outerhashtable->batches, nbatch * sizeof(MJoinBatchDesc));

	MemoryContextSwitchTo(oldcxt);

	MemSet(innerhashtable->batches + oldnbatch, 0, (nbatch - oldnbatch) * sizeof(MJoinBatchDesc));
	MemSet(outerhashtable->batches + oldnbatch, 0, (nbatch - oldnbatch) * sizeof(MJoinBatchDesc));

	MemoryContextSwitchTo(innerhashtable->hashCxt);

	for (bno = oldnbatch; bno < nbatch; bno++) {

		innerhashtable->batches[bno] = (MJoinBatchDesc) palloc(sizeof(MJoinBatchData));
		ExecMHashJoinResetBatch(innerhashtable->batches[bno]);
		innerhashtable->batches[bno]->spaceAllowed = innerhashtable->spaceAllowed;

	}
	MemoryContextSwitchTo(outerhashtable->hashCxt);

	for (bno = oldnbatch; bno < nbatch; bno++) {

		outerhashtable->batches[bno] = (MJoinBatchDesc) palloc(sizeof(MJoinBatchData));
		ExecMHashJoinResetBatch(outerhashtable->batches[bno]);
		outerhashtable->batches[bno]->spaceAllowed = outerhashtable->spaceAllowed;
	}


	MemoryContextSwitchTo(oldcxt);

	innerhashtable->nbatch = nbatch;
	outerhashtable->nbatch = nbatch;
	printf("\nDumping inner!\n");
	ExecMHashDumpBatch(innerhashtable);
	printf("\nDumping outer!\n");
	ExecMHashDumpBatch(outerhashtable);

}
/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void ExecHashTableInsert(HashJoinTable hashtable, TupleTableSlot *slot, uint32 hashvalue) {
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	int bucketno;
	int batchno;

	ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */
	if (batchno == hashtable->curbatch) {
		/*
		 * put the tuple in hash table
		 */
		HashJoinTuple hashTuple;
		int hashTupleSize;

		/* Create the HashJoinTuple */
		hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
		hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt, hashTupleSize);
		hashTuple->hashvalue = hashvalue;
		memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);

		/*
		 * We always reset the tuple-matched flag on insertion.  This is okay
		 * even when reloading a tuple from a batch file, since the tuple
		 * could not possibly have been matched to an outer tuple before it
		 * went into the batch file.
		 */
		HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

		/* Push it onto the front of the bucket's list */
		hashTuple->next = hashtable->buckets[bucketno];
		hashtable->buckets[bucketno] = hashTuple;

		/* Account for space used, and back off if we've used too much */
		hashtable->spaceUsed += hashTupleSize;
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;
		if (hashtable->spaceUsed > hashtable->spaceAllowed)
			ExecHashIncreaseNumBatches(hashtable);
	}
	else
	{
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > hashtable->curbatch);
		ExecHashJoinSaveTuple(tuple,
							  hashvalue,
							  &hashtable->innerBatchFile[batchno]);
	}
}
/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void ExecMultiHashTableInsert(SimpleHashTable hashtable, MinimalTuple tuple, uint32 hashvalue) {
	int bucketno;
	JoinTuple jtuple;


	ExecMultiHashGetBucket(hashtable, hashvalue, &bucketno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */

		/*
		 * put the tuple in hash table
		 */

		/* Create the HashJoinTuple */
		if( hashtable->freeList == NULL)
			elog(ERROR, "out of memory: No more buckets in the hashtable");

		jtuple = hashtable->freeList;
		jtuple->hashvalue = hashvalue;
		jtuple->mtuple=tuple;
		hashtable->freeList =  hashtable->freeList->next;


		/*
		 * We always reset the tuple-matched flag on insertion.  This is okay
		 * even when reloading a tuple from a batch file, since the tuple
		 * could not possibly have been matched to an outer tuple before it
		 * went into the batch file.
		 */
		HeapTupleHeaderClearMatch(jtuple->mtuple);

		/* Push it onto the front of the bucket's list */
		jtuple->next = hashtable->buckets[bucketno];
		hashtable->buckets[bucketno] = jtuple;

		/* Account for space used, and back off if we've used too much */
		hashtable->spaceUsed += JTUPLESIZE;




}
void
ExecMHashTableInsert(SymHashJoinState *mhjstate,MJoinTable hashtabledest,
					TupleTableSlot *slot,
					uint32 hashvalue, bool saved)
{

	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	int			bucketno;
	int			batchno;
	int			hashTupleSize;

	ExecHashGetBucketAndBatch((HashJoinTable)hashtabledest, hashvalue,
							  &bucketno, &batchno);

	/*
	 * decide whether to put the tuple in the hash table or a temp file
	 */



	// precheck  hashtable overflow before adding a new tuple
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	if (batchno == hashtabledest->curbatch) {


		hashtabledest->spaceUsed += hashTupleSize;
		if (hashtabledest->spaceUsed > hashtabledest->spaceAllowed) {
			hashtabledest->spaceUsed -= hashTupleSize;
			ExecMHashIncreaseNumBatches(mhjstate);

		}
	}

	ExecHashGetBucketAndBatch((HashJoinTable)hashtabledest, hashvalue,
								  &bucketno, &batchno);


	if (batchno == hashtabledest->curbatch)
	{
		/*
		 * put the tuple in hash table
		 */
		HashJoinTuple hashTuple;


		/* Create the HashJoinTuple */
		hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
		hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtabledest->batchCxt,
													   hashTupleSize);
		hashTuple->hashvalue = hashvalue;
		memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);

		/*
		 * We always reset the tuple-matched flag on insertion.  This is okay
		 * even when reloading a tuple from a batch file, since the tuple
		 * could not possibly have been matched to an outer tuple before it
		 * went into the batch file.
		 */
		HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

		/* Push it onto the front of the bucket's list */

		hashTuple->next = hashtabledest->buckets[bucketno];
		hashtabledest->buckets[bucketno] = hashTuple;
		hashtabledest->nInserted++;
		hashtabledest->batches[batchno]->nentries++;

	}
	else
	{
		/*
		 * put the tuple into a temp file for later batches
		 */
		Assert(batchno > hashtabledest->curbatch);


		if(!saved)
		ExecHashJoinSaveTuple(tuple,
							  hashvalue,
							  &(hashtabledest->batches[batchno]->batchFile));
		else
		ExecHashJoinSaveTuple(tuple,
							hashvalue,
							 &(hashtabledest->batches[batchno]->savedFile));


	}
	hashtabledest->batches[batchno]->spaceUsed+=hashTupleSize;

}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions should have
 * varno either OUTER_VAR or INNER_VAR.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 */
bool
ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	if (outer_tuple)
		hashfunctions = hashtable->outer_hashfunctions;
	else
		hashfunctions = hashtable->inner_hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.	However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions should have
 * varno either OUTER_VAR or INNER_VAR.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 */
bool
ExecMultiHashGetHashValue(SimpleHashTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);


		hashfunctions = hashtable->hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.	However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}
bool
ExecMHashGetHashValue(MJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool keep_nulls,
					 uint32 *hashvalue)
{
	uint32		hashkey = 0;
	FmgrInfo   *hashfunctions;
	ListCell   *hk;
	int			i = 0;
	MemoryContext oldContext;

	/*
	 * We reset the eval context each time to reclaim any memory leaked in the
	 * hashkey expressions.
	 */
	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);



		hashfunctions = hashtable->hashfunctions;

	foreach(hk, hashkeys)
	{
		ExprState  *keyexpr = (ExprState *) lfirst(hk);
		Datum		keyval;
		bool		isNull;

		/* rotate hashkey left 1 bit at each step */
		hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

		/*
		 * Get the join attribute value of the tuple
		 */
		keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

		/*
		 * If the attribute is NULL, and the join operator is strict, then
		 * this tuple cannot pass the join qual so we can reject it
		 * immediately (unless we're scanning the outside of an outer join, in
		 * which case we must not reject it).  Otherwise we act like the
		 * hashcode of NULL is zero (this will support operators that act like
		 * IS NOT DISTINCT, though not any more-random behavior).  We treat
		 * the hash support function as strict even if the operator is not.
		 *
		 * Note: currently, all hashjoinable operators must be strict since
		 * the hash index AM assumes that.	However, it takes so little extra
		 * code here to allow non-strict that we may as well do it.
		 */
		if (isNull)
		{
			if (hashtable->hashStrict[i] && !keep_nulls)
			{
				MemoryContextSwitchTo(oldContext);
				return false;	/* cannot match */
			}
			/* else, leave hashkey unmodified, equivalent to hashcode 0 */
		}
		else
		{
			/* Compute the hash function */
			uint32		hkey;

			hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
			hashkey ^= hkey;
		}

		i++;
	}

	MemoryContextSwitchTo(oldContext);

	*hashvalue = hashkey;
	return true;
}
/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = (hashvalue DIV nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets doesn't change over the course of the join.
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.
 */
void
ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;
	uint32		nbatch = (uint32) hashtable->nbatch;

	if (nbatch > 1)
	{
		/* we can do MOD by masking, DIV by shifting */
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
	}
	else
	{
		*bucketno = hashvalue & (nbuckets - 1);
		*batchno = 0;
	}


}

void
ExecMultiHashGetBucket(SimpleHashTable hashtable,
						  uint32 hashvalue,
						  int *bucketno)
{
	uint32		nbuckets = (uint32) hashtable->nbuckets;


		*bucketno = hashvalue & (nbuckets - 1);



}
/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashBucket(HashJoinState *hjstate,
				   ExprContext *econtext)
{
	List	   *hjclauses = hjstate->hashclauses;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;
	uint32		hashvalue = hjstate->hj_CurHashValue;

	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	if (hashTuple != NULL)
		hashTuple = hashTuple->next;
	else if (hjstate->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
		hashTuple = hashtable->skewBucket[hjstate->hj_CurSkewBucketNo]->tuples;
	else
		hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											 hjstate->hj_HashTupleSlot,
											 false);	/* do not pfree */
			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext, false))
			{
				hjstate->hj_CurTuple = hashTuple;
				return true;
			}
		}

		hashTuple = hashTuple->next;
	}

	/*
	 * no match
	 */
	return false;
}

/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanMultiHashBucket(CHashJoinState *chjstate,
				   ExprContext *econtext)
{
	List	   *hjclauses = chjstate->hashclauses;
	SimpleHashTable hashtable = chjstate->chj_HashTable;
	JoinTuple hashTuple =  chjstate->chj_CurTuple;
	uint32		hashvalue = chjstate->chj_CurHashValue;

	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	//printf("hastable pointer num : %X \n",hashtable);
//	printf("Bucket num : %d \n",chjstate->chj_CurBucketNo);

	if (hashTuple != NULL)
		hashTuple = hashTuple->next;
	else{
//		printf("NEW BUCKET\n");
		hashTuple = hashtable->buckets[chjstate->chj_CurBucketNo];
	}

//	printf("hashTuple pointer : %X \n",hashTuple);
//	printf("\n");
//	fflush(stdout);

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue ) {

			TupleTableSlot *inntuple;

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			inntuple = ExecStoreMinimalTuple(hashTuple->mtuple,
					chjstate->chj_HashTupleSlot,
					false); /* do not pfree */

			econtext->ecxt_innertuple = inntuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext, false)) {
				chjstate->chj_CurTuple = hashTuple;
				return true;
			}
		}

		hashTuple = hashTuple->next;
	}


	/*
	 * no match
	 */
	return false;
}


/*
 * ExecPrepHashTableForUnmatched
 *		set up for a series of ExecScanHashTableForUnmatched calls
 */
void
ExecPrepHashTableForUnmatched(HashJoinState *hjstate)
{
	/*
	 * ---------- During this scan we use the HashJoinState fields as follows:
	 *
	 * hj_CurBucketNo: next regular bucket to scan hj_CurSkewBucketNo: next
	 * skew bucket (an index into skewBucketNums) hj_CurTuple: last tuple
	 * returned, or NULL to start next bucket ----------
	 */
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = 0;
	hjstate->hj_CurTuple = NULL;
}

/*
 * ExecScanHashTableForUnmatched
 *		scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool
ExecScanHashTableForUnmatched(HashJoinState *hjstate, ExprContext *econtext)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	HashJoinTuple hashTuple = hjstate->hj_CurTuple;

	for (;;)
	{
		/*
		 * hj_CurTuple is the address of the tuple last returned from the
		 * current bucket, or NULL if it's time to start scanning a new
		 * bucket.
		 */
		if (hashTuple != NULL)
			hashTuple = hashTuple->next;
		else if (hjstate->hj_CurBucketNo < hashtable->nbuckets)
		{
			hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];
			hjstate->hj_CurBucketNo++;
		}
		else if (hjstate->hj_CurSkewBucketNo < hashtable->nSkewBuckets)
		{
			int			j = hashtable->skewBucketNums[hjstate->hj_CurSkewBucketNo];

			hashTuple = hashtable->skewBucket[j]->tuples;
			hjstate->hj_CurSkewBucketNo++;
		}
		else
			break;				/* finished all buckets */

		while (hashTuple != NULL)
		{
			if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple)))
			{
				TupleTableSlot *inntuple;

				/* insert hashtable's tuple into exec slot */
				inntuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
												 hjstate->hj_HashTupleSlot,
												 false);		/* do not pfree */
				econtext->ecxt_innertuple = inntuple;

				/*
				 * Reset temp memory each time; although this function doesn't
				 * do any qual eval, the caller will, so let's keep it
				 * parallel to ExecScanHashBucket.
				 */
				ResetExprContext(econtext);

				hjstate->hj_CurTuple = hashTuple;
				return true;
			}

			hashTuple = hashTuple->next;
		}
	}

	/*
	 * no more unmatched tuples
	 */
	return false;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecHashTableReset(HashJoinTable hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = hashtable->nbuckets;

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	hashtable->spaceUsed = 0;

	MemoryContextSwitchTo(oldcxt);
}
/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void
ExecMHashTableReset(MJoinTable hashtable)
{
	MemoryContext oldcxt;
	int			nbuckets = hashtable->nbuckets;

	/*
	 * Release all the hash buckets and tuples acquired in the prior pass, and
	 * reinitialize the context for a new pass.
	 */
	MemoryContextReset(hashtable->batchCxt);
	oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

	/* Reallocate and reinitialize the hash bucket headers. */
	hashtable->buckets = (HashJoinTuple *)
		palloc0(nbuckets * sizeof(HashJoinTuple));

	hashtable->spaceUsed = 0;
	hashtable->nInserted = 0;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * ExecHashTableResetMatchFlags
 *		Clear all the HeapTupleHeaderHasMatch flags in the table
 */
void
ExecHashTableResetMatchFlags(HashJoinTable hashtable)
{
	HashJoinTuple tuple;
	int			i;

	/* Reset all flags in the main table ... */
	for (i = 0; i < hashtable->nbuckets; i++)
	{
		for (tuple = hashtable->buckets[i]; tuple != NULL; tuple = tuple->next)
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}

	/* ... and the same for the skew buckets, if any */
	for (i = 0; i < hashtable->nSkewBuckets; i++)
	{
		int			j = hashtable->skewBucketNums[i];
		HashSkewBucket *skewBucket = hashtable->skewBucket[j];

		for (tuple = skewBucket->tuples; tuple != NULL; tuple = tuple->next)
			HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
	}
}


void
ExecReScanHash(HashState *node)
{
	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);
}


/*
 * ExecHashBuildSkewHash
 *
 *		Set up for skew optimization if we can identify the most common values
 *		(MCVs) of the outer relation's join key.  We make a skew hash bucket
 *		for the hash value of each MCV, up to the number of slots allowed
 *		based on available memory.
 */
static void
ExecHashBuildSkewHash(HashJoinTable hashtable, Hash *node, int mcvsToUse)
{
	HeapTupleData *statsTuple;
	Datum	   *values;
	int			nvalues;
	float4	   *numbers;
	int			nnumbers;

	/* Do nothing if planner didn't identify the outer relation's join key */
	if (!OidIsValid(node->skewTable))
		return;
	/* Also, do nothing if we don't have room for at least one skew bucket */
	if (mcvsToUse <= 0)
		return;

	/*
	 * Try to find the MCV statistics for the outer relation's join key.
	 */
	statsTuple = SearchSysCache3(STATRELATTINH,
								 ObjectIdGetDatum(node->skewTable),
								 Int16GetDatum(node->skewColumn),
								 BoolGetDatum(node->skewInherit));
	if (!HeapTupleIsValid(statsTuple))
		return;

	if (get_attstatsslot(statsTuple, node->skewColType, node->skewColTypmod,
						 STATISTIC_KIND_MCV, InvalidOid,
						 NULL,
						 &values, &nvalues,
						 &numbers, &nnumbers))
	{
		double		frac;
		int			nbuckets;
		FmgrInfo   *hashfunctions;
		int			i;

		if (mcvsToUse > nvalues)
			mcvsToUse = nvalues;

		/*
		 * Calculate the expected fraction of outer relation that will
		 * participate in the skew optimization.  If this isn't at least
		 * SKEW_MIN_OUTER_FRACTION, don't use skew optimization.
		 */
		frac = 0;
		for (i = 0; i < mcvsToUse; i++)
			frac += numbers[i];
		if (frac < SKEW_MIN_OUTER_FRACTION)
		{
			free_attstatsslot(node->skewColType,
							  values, nvalues, numbers, nnumbers);
			ReleaseSysCache(statsTuple);
			return;
		}

		/*
		 * Okay, set up the skew hashtable.
		 *
		 * skewBucket[] is an open addressing hashtable with a power of 2 size
		 * that is greater than the number of MCV values.  (This ensures there
		 * will be at least one null entry, so searches will always
		 * terminate.)
		 *
		 * Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
		 * MaxAllocSize/sizeof(void *)/8, but that is not currently possible
		 * since we limit pg_statistic entries to much less than that.
		 */
		nbuckets = 2;
		while (nbuckets <= mcvsToUse)
			nbuckets <<= 1;
		/* use two more bits just to help avoid collisions */
		nbuckets <<= 2;

		hashtable->skewEnabled = true;
		hashtable->skewBucketLen = nbuckets;

		/*
		 * We allocate the bucket memory in the hashtable's batch context. It
		 * is only needed during the first batch, and this ensures it will be
		 * automatically removed once the first batch is done.
		 */
		hashtable->skewBucket = (HashSkewBucket **)
			MemoryContextAllocZero(hashtable->batchCxt,
								   nbuckets * sizeof(HashSkewBucket *));
		hashtable->skewBucketNums = (int *)
			MemoryContextAllocZero(hashtable->batchCxt,
								   mcvsToUse * sizeof(int));

		hashtable->spaceUsed += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		hashtable->spaceUsedSkew += nbuckets * sizeof(HashSkewBucket *)
			+ mcvsToUse * sizeof(int);
		if (hashtable->spaceUsed > hashtable->spacePeak)
			hashtable->spacePeak = hashtable->spaceUsed;

		/*
		 * Create a skew bucket for each MCV hash value.
		 *
		 * Note: it is very important that we create the buckets in order of
		 * decreasing MCV frequency.  If we have to remove some buckets, they
		 * must be removed in reverse order of creation (see notes in
		 * ExecHashRemoveNextSkewBucket) and we want the least common MCVs to
		 * be removed first.
		 */
		hashfunctions = hashtable->outer_hashfunctions;

		for (i = 0; i < mcvsToUse; i++)
		{
			uint32		hashvalue;
			int			bucket;

			hashvalue = DatumGetUInt32(FunctionCall1(&hashfunctions[0],
													 values[i]));

			/*
			 * While we have not hit a hole in the hashtable and have not hit
			 * the desired bucket, we have collided with some previous hash
			 * value, so try the next bucket location.	NB: this code must
			 * match ExecHashGetSkewBucket.
			 */
			bucket = hashvalue & (nbuckets - 1);
			while (hashtable->skewBucket[bucket] != NULL &&
				   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
				bucket = (bucket + 1) & (nbuckets - 1);

			/*
			 * If we found an existing bucket with the same hashvalue, leave
			 * it alone.  It's okay for two MCVs to share a hashvalue.
			 */
			if (hashtable->skewBucket[bucket] != NULL)
				continue;

			/* Okay, create a new skew bucket for this hashvalue. */
			hashtable->skewBucket[bucket] = (HashSkewBucket *)
				MemoryContextAlloc(hashtable->batchCxt,
								   sizeof(HashSkewBucket));
			hashtable->skewBucket[bucket]->hashvalue = hashvalue;
			hashtable->skewBucket[bucket]->tuples = NULL;
			hashtable->skewBucketNums[hashtable->nSkewBuckets] = bucket;
			hashtable->nSkewBuckets++;
			hashtable->spaceUsed += SKEW_BUCKET_OVERHEAD;
			hashtable->spaceUsedSkew += SKEW_BUCKET_OVERHEAD;
			if (hashtable->spaceUsed > hashtable->spacePeak)
				hashtable->spacePeak = hashtable->spaceUsed;
		}

		free_attstatsslot(node->skewColType,
						  values, nvalues, numbers, nnumbers);
	}

	ReleaseSysCache(statsTuple);
}

/*
 * ExecHashGetSkewBucket
 *
 *		Returns the index of the skew bucket for this hashvalue,
 *		or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *		associated with any active skew bucket.
 */
int
ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)
{
	int			bucket;

	/*
	 * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
	 * particular, this happens after the initial batch is done).
	 */
	if (!hashtable->skewEnabled)
		return INVALID_SKEW_BUCKET_NO;

	/*
	 * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
	 */
	bucket = hashvalue & (hashtable->skewBucketLen - 1);

	/*
	 * While we have not hit a hole in the hashtable and have not hit the
	 * desired bucket, we have collided with some other hash value, so try the
	 * next bucket location.
	 */
	while (hashtable->skewBucket[bucket] != NULL &&
		   hashtable->skewBucket[bucket]->hashvalue != hashvalue)
		bucket = (bucket + 1) & (hashtable->skewBucketLen - 1);

	/*
	 * Found the desired bucket?
	 */


	if (hashtable->skewBucket[bucket] != NULL)
		return bucket;

	bucket =INVALID_SKEW_BUCKET_NO;
	/*
	 * There must not be any hashtable entry for this hash value.
	 */

	return bucket;
}

/*
 * ExecHashSkewTableInsert
 *
 *		Insert a tuple into the skew hashtable.
 *
 * This should generally match up with the current-batch case in
 * ExecHashTableInsert.
 */
static void
ExecHashSkewTableInsert(HashJoinTable hashtable,
						TupleTableSlot *slot,
						uint32 hashvalue,
						int bucketNumber)
{
	MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
	HashJoinTuple hashTuple;
	int			hashTupleSize;

	/* Create the HashJoinTuple */
	hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
	hashTuple = (HashJoinTuple) MemoryContextAlloc(hashtable->batchCxt,
												   hashTupleSize);
	hashTuple->hashvalue = hashvalue;
	memcpy(HJTUPLE_MINTUPLE(hashTuple), tuple, tuple->t_len);
	HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

	/* Push it onto the front of the skew bucket's list */
	hashTuple->next = hashtable->skewBucket[bucketNumber]->tuples;
	hashtable->skewBucket[bucketNumber]->tuples = hashTuple;

	/* Account for space used, and back off if we've used too much */
	hashtable->spaceUsed += hashTupleSize;
	hashtable->spaceUsedSkew += hashTupleSize;
	if (hashtable->spaceUsed > hashtable->spacePeak)
		hashtable->spacePeak = hashtable->spaceUsed;
	while (hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew)
		ExecHashRemoveNextSkewBucket(hashtable);

	/* Check we are not over the total spaceAllowed, either */
	if (hashtable->spaceUsed > hashtable->spaceAllowed)
		ExecHashIncreaseNumBatches(hashtable);
}

/*
 *		ExecHashRemoveNextSkewBucket
 *
 *		Remove the least valuable skew bucket by pushing its tuples into
 *		the main hash table.
 */
static void
ExecHashRemoveNextSkewBucket(HashJoinTable hashtable)
{
	int			bucketToRemove;
	HashSkewBucket *bucket;
	uint32		hashvalue;
	int			bucketno;
	int			batchno;
	HashJoinTuple hashTuple;

	/* Locate the bucket to remove */
	bucketToRemove = hashtable->skewBucketNums[hashtable->nSkewBuckets - 1];
	bucket = hashtable->skewBucket[bucketToRemove];

	/*
	 * Calculate which bucket and batch the tuples belong to in the main
	 * hashtable.  They all have the same hash value, so it's the same for all
	 * of them.  Also note that it's not possible for nbatch to increase while
	 * we are processing the tuples.
	 */
	hashvalue = bucket->hashvalue;
	ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

	/* Process all tuples in the bucket */
	hashTuple = bucket->tuples;
	while (hashTuple != NULL)
	{
		HashJoinTuple nextHashTuple = hashTuple->next;
		MinimalTuple tuple;
		Size		tupleSize;

		/*
		 * This code must agree with ExecHashTableInsert.  We do not use
		 * ExecHashTableInsert directly as ExecHashTableInsert expects a
		 * TupleTableSlot while we already have HashJoinTuples.
		 */
		tuple = HJTUPLE_MINTUPLE(hashTuple);
		tupleSize = HJTUPLE_OVERHEAD + tuple->t_len;

		/* Decide whether to put the tuple in the hash table or a temp file */
		if (batchno == hashtable->curbatch)
		{
			/* Move the tuple to the main hash table */
			hashTuple->next = hashtable->buckets[bucketno];
			hashtable->buckets[bucketno] = hashTuple;
			/* We have reduced skew space, but overall space doesn't change */
			hashtable->spaceUsedSkew -= tupleSize;
		}
		else
		{
			/* Put the tuple into a temp file for later batches */
			Assert(batchno > hashtable->curbatch);
			ExecHashJoinSaveTuple(tuple, hashvalue,
								  &hashtable->innerBatchFile[batchno]);
			pfree(hashTuple);
			hashtable->spaceUsed -= tupleSize;
			hashtable->spaceUsedSkew -= tupleSize;
		}

		hashTuple = nextHashTuple;
	}

	/*
	 * Free the bucket struct itself and reset the hashtable entry to NULL.
	 *
	 * NOTE: this is not nearly as simple as it looks on the surface, because
	 * of the possibility of collisions in the hashtable.  Suppose that hash
	 * values A and B collide at a particular hashtable entry, and that A was
	 * entered first so B gets shifted to a different table entry.	If we were
	 * to remove A first then ExecHashGetSkewBucket would mistakenly start
	 * reporting that B is not in the hashtable, because it would hit the NULL
	 * before finding B.  However, we always remove entries in the reverse
	 * order of creation, so this failure cannot happen.
	 */
	hashtable->skewBucket[bucketToRemove] = NULL;
	hashtable->nSkewBuckets--;
	pfree(bucket);
	hashtable->spaceUsed -= SKEW_BUCKET_OVERHEAD;
	hashtable->spaceUsedSkew -= SKEW_BUCKET_OVERHEAD;

	/*
	 * If we have removed all skew buckets then give up on skew optimization.
	 * Release the arrays since they aren't useful any more.
	 */
	if (hashtable->nSkewBuckets == 0)
	{
		hashtable->skewEnabled = false;
		pfree(hashtable->skewBucket);
		pfree(hashtable->skewBucketNums);
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->spaceUsed -= hashtable->spaceUsedSkew;
		hashtable->spaceUsedSkew = 0;
	}
}


/* Alex:  From a batch loaded from disk return the next tuple. the tuple are
 * iterated by bucket natural order (array index);
 */
static TupleTableSlot * ExecMJoinGetNextTuple(HashState * node) {

	MJoinTable hashtable = (MJoinTable)node->hashtable;
	MJoinBatchDesc batch = hashtable->batches[hashtable->curbatch];
	uint32 hashvalue;
	BufFile *bufFile = batch->batchFile;
	TupleTableSlot *slot = node->ps.ps_ResultTupleSlot;




	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	ExecClearTuple(slot);
	slot = ExecMJoinGetSavedTuple(bufFile,&hashvalue,slot);
	if (!TupIsNull(slot))
		ExecMHashTableInsert(hashtable->parent, hashtable, slot, hashvalue, false);

	return slot;

}

HashInfo *GetUniqueHashInfo (MultiHashState *mhstate , List * clauses, List *hoperators, bool *found){
	HashInfo *tmp = NULL;
	HashInfo * hinfo = makeNode(HashInfo);
	hinfo->hashkeys = clauses;
	hinfo->hoperators = hoperators;
	*found = false;
	tmp = list_member_return(mhstate->all_hashkeys, hinfo);
	if (tmp != NULL) {
		*found = true;
		pfree(hinfo);
		return tmp;
	}

	return hinfo;




}

HashInfo * add_hashinfo(MultiHashState *mhstate , List * clauses, List *hoperators, Bitmapset *relids){

	HashInfo *tmp  = NULL;
	bool found;

	tmp = GetUniqueHashInfo(mhstate, clauses, hoperators, &found);
	if (!found) {
		mhstate->all_hashkeys = lappend(mhstate->all_hashkeys, tmp);
	} else {
	}


	tmp->relids =  bms_add_members(tmp->relids,relids);


	return tmp;



}

void ExecResetMultiHashtable(MultiHashState *node, SimpleHashTable  * hashtables){

	ListCell *lc;
	List *hashkeys;
	SimpleHashTable hashtable;
	MemoryContext oldcxt;



	// Reset the all the buckets:



	foreach(lc,node->all_hashkeys) {


		HashInfo * hinfo = (HashInfo *) lfirst(lc);

		hashtable = hashtables[hinfo->id];

		MemoryContextReset(hashtable->hashCxt);



		// Update hashtable info
		hashtable->totalTuples = 0;
		hashtable->spaceAllowed += hashtable->spaceUsed;


		/* Allocate data that will live for the life of the multihashjoin */
		ExecMultiHashAllocateHashtable(hashtable);



	}


}

static void ExecMultiHashAllocateHashtable(SimpleHashTable hashtable) {

	int i;
	Size elementSize;
	JoinTuple firstElement;
	JoinTuple tmpElement;
	JoinTuple prevElement;
	MemoryContext oldcxt;

	int num_elements = hashtable->nbuckets *NTUP_PER_BUCKET;


	oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);
	//printf(" created hashtable with %d buckets", hashtable->nbuckets);
	hashtable->buckets = (JoinTuple *) palloc0(hashtable->nbuckets * sizeof(JoinTuple));

	elementSize = MAXALIGN(sizeof(JoinTupleData));

	firstElement = (JoinTuple) palloc0( num_elements * elementSize);

	if (!firstElement)
		elog(ERROR, "out of memory. could no create hashtable jointuples");

	hashtable->firstElement = firstElement;

	prevElement = NULL;
	tmpElement = firstElement;

	for (i = 0; i < num_elements; i++) {

		tmpElement->next = prevElement;
		prevElement = tmpElement;
		tmpElement = (JoinTuple) (((char *) tmpElement) + elementSize);
	}

	hashtable->freeList = prevElement;

	/*
	 * Set up for skew optimization, if possible and there's a need for more
	 * than one batch.	(In a one-batch join, there's no point in it.)
	 */

	MemoryContextSwitchTo(oldcxt);

}
