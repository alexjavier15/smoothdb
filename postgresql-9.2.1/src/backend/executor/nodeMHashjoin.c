/*
 * nodeMHashJoin.c
 *
 *  Created on: 5 mars 2015
 *      Author: alex

 *	  Routines to handle Mhash join nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMHashjoin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeMHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"



/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_NEED_NEW_INNER		3
#define HJ_SCAN_BUCKET			4
#define HJ_FILL_OUTER_TUPLE		5
#define HJ_FILL_INNER_TUPLES	6
#define HJ_NEED_NEW_BATCH		7
#define HJ_END					8


#define OuterIsExhausted(node) ( node->mhj_OuterHashTable->status == MHJ_EXAHUSTED)

#define InnerIsExhausted(node) ( node->mhj_InnerHashTable->status == MHJ_EXAHUSTED)

static TupleTableSlot *ExecMJoinGetTuple(PlanState *hashNode,
						MJoinTable hashtable,
						MJoinState *hjstate,
						uint32 *hashvalue,bool outer_tuple,
						bool *isNotEmpty);
static TupleTableSlot *ExecMJoinGetSavedTuple(MJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot);

static bool
ExecMJoinNewBatch(MJoinState *hjstate);

static bool
ExecMJoinScanHashBucket(MJoinState *hjstate,
				   ExprContext *econtext);

static int nextState(MJoinState *node);
/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		This function implements the Hybrid Hashjoin algorithm.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
static int nextState(MJoinState *node){

	switch (node->mhj_LastState) {

		case HJ_NEED_NEW_OUTER:
			if (!InnerIsExhausted(node)) {
				//printf("Asking INNER \n");
				return HJ_NEED_NEW_INNER;

			}
			if (!OuterIsExhausted(node)) {
				//printf("Asking OUTER \n");

				return HJ_NEED_NEW_OUTER;

			}


			break;
		case HJ_NEED_NEW_INNER:

			if (!OuterIsExhausted(node)) {
				//printf("Asking OUTER \n");

				return HJ_NEED_NEW_OUTER;

			}
			if (!InnerIsExhausted(node)) {
				//printf("Asking INNER \n");


				return HJ_NEED_NEW_INNER;

			}

			break;

		default:

			break;

	}


	return HJ_NEED_NEW_BATCH;


}
TupleTableSlot *				/* return: a tuple or NULL */
ExecMJoin(MJoinState *node)
{
	HashState  *innerHashNode;
	HashState  *outerHashNode;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;
	ExprDoneCond isDone;
	MJoinTable outerHashtable;
	MJoinTable innerHashtable;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	innerHashNode = (HashState *) innerPlanState(node);
	outerHashNode = (HashState *) outerPlanState(node);


	innerHashtable = node->mhj_InnerHashTable;
	outerHashtable = node->mhj_OuterHashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Check to see if we're still projecting out tuples from a previous join
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->js.ps.ps_TupFromTlist)
	{
		TupleTableSlot *result;

		result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return result;
		/* Done with that source tuple... */
		node->js.ps.ps_TupFromTlist = false;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		switch (node->mhj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(innerHashtable == NULL && outerHashtable == NULL);

				/*
				 * If the outer relation is completely empty, and it's not
				 * right/full join, we can quit without building the hash
				 * table.  However, for an inner join it is only a win to
				 * check this when the outer relation's startup cost is less
				 * than the projected cost of building the hash table.
				 * Otherwise it's best to build the hash table first and see
				 * if the inner relation is empty.	(When it's a left join, we
				 * should always make this check, since we aren't going to be
				 * able to skip the join on the strength of an empty inner
				 * relation anyway.)
				 *
				 * If we are rescanning the join, we make use of information
				 * gained on the previous scan: don't bother to try the
				 * prefetch if the previous scan found the outer relation
				 * nonempty. This is not 100% reliable since with new
				 * parameters the outer relation might yield different
				 * results, but it's a good heuristic.
				 *
				 * The only way to make the check is to try to fetch a tuple
				 * from the outer plan node.  If we succeed, we have to stash
				 * it away for later consumption by ExecHashJoinOuterGetTuple.
				 */

//				if (HJ_FILL_INNER(node))
//				{
//					/* no chance to not build the hash table */
//					node->mhj_FirstOuterTupleSlot = NULL;
//				}
//				else if (HJ_FILL_OUTER(node) ||
//						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
//						  !node->mhj_OuterNotEmpty))
//				{
//					node->mhj_FirstOuterTupleSlot = ExecProcNode(outerNode);
//					if (TupIsNull(node->mhj_FirstOuterTupleSlot))
//					{
//						node->mhj_OuterNotEmpty = false;
//						return NULL;
//					}
//					else
//						node->mhj_OuterNotEmpty = true;
//				}
//				else
//					node->mhj_FirstOuterTupleSlot = NULL;

				/*
				 * create the inner hash table
				 */

				innerHashtable = ExecMHashTableCreate((Hash *) innerHashNode->ps.plan,
												node->mhj_HashOperators,
												false, false);
				node->mhj_InnerHashTable = innerHashtable;
				innerHashtable->parent = node;
				/*
				 * create the  outer hash table
				 */
				outerHashtable = ExecMHashTableCreate((Hash *) outerHashNode->ps.plan, node->mhj_HashOperators,
						false, true);

				node->mhj_OuterHashTable = outerHashtable;

				outerHashtable->parent = node;

				innerHashNode->hashtable = (HashJoinTable)innerHashtable;

				outerHashNode->hashtable =  (HashJoinTable)outerHashtable;

				/*
				 * execute the Inner Hash node, to get the first inner tuple
				 */

				//(void) ExecProcNode((PlanState *) innerHashNode);

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */
				//if (innerHashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
				//	return NULL;


				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				//innerHashtable->nbatch_outstart = innerHashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */

				node->mhj_OuterNotEmpty = true;
				node->mhj_InnerNotEmpty = true;
				node->mhj_JoinState = HJ_NEED_NEW_OUTER;

				break;
			case HJ_NEED_NEW_INNER:
				node->mhj_LastState = HJ_NEED_NEW_INNER;

				innerTupleSlot = ExecMJoinGetTuple((PlanState *)innerHashNode,innerHashtable,
						node,&hashvalue,false,&node->mhj_InnerNotEmpty);
				/*
				 * We don't have an inner tuple, try to get the next one
				 */

				if (TupIsNull(innerTupleSlot)) {
					innerHashtable->status = MHJ_EXAHUSTED;
					node->mhj_JoinState = nextState(node);

					continue;
				}
				innerHashtable->status = MHJ_HASH;

				econtext->ecxt_innertuple = innerTupleSlot;
				node->mhj_MatchedInner = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->mhj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch((HashJoinTable)outerHashtable, hashvalue, &node->mhj_CurBucketNo, &batchno);

				node->mhj_CurTuple = NULL;


				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				node->mhj_JoinState = nextState(node);
				if (batchno != innerHashtable->curbatch)
					continue;


				/* OK, let's scan the bucket for matches */

				//printf("Go to scan OUTER \n");

				node->mhj_ScanHashTable = outerHashtable;
				node->mhj_ScanEcxt_slot = &econtext->ecxt_outertuple;
				node->mhj_JoinState = HJ_SCAN_BUCKET;


				break;
			case HJ_NEED_NEW_OUTER:
				node->mhj_LastState = HJ_NEED_NEW_OUTER;
				/*
				 * We don't have an outer tuple, try to get the next one
				 */

				outerTupleSlot = ExecMJoinGetTuple((PlanState *)outerHashNode,outerHashtable,
						node,&hashvalue,true,&node->mhj_OuterNotEmpty);

				if (TupIsNull(outerTupleSlot))
				{
					outerHashtable->status = MHJ_EXAHUSTED;
					/* end of batch, or maybe whole join */
					node->mhj_JoinState = nextState(node);
					continue;
				}
				outerHashtable->status = MHJ_HASH;

				econtext->ecxt_outertuple = outerTupleSlot;
				node->mhj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->mhj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch((HashJoinTable)innerHashtable, hashvalue, &node->mhj_CurBucketNo, &batchno);


				node->mhj_CurTuple = NULL;


				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				node->mhj_JoinState = nextState(node);
				if (batchno != outerHashtable->curbatch)
					continue;

				/* OK, let's scan the bucket for matches */

				//printf("Go to scan INNER \n");

				node->mhj_JoinState = HJ_SCAN_BUCKET;
				node->mhj_ScanEcxt_slot = &econtext->ecxt_innertuple;
				node->mhj_ScanHashTable = innerHashtable;


				/* FALL THRU */
				break;


			case HJ_SCAN_BUCKET:

				/*
				 * We check for interrupts here because this corresponds to
				 * where we'd fetch a row from a child plan node in other join
				 * types.
				 */
				CHECK_FOR_INTERRUPTS();

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				node->mhj_JoinState = nextState(node);
				if (!ExecMJoinScanHashBucket(node, econtext))
				{
					/* end of batch, or maybe whole join */


					continue;
				}
				node->mhj_JoinState =HJ_SCAN_BUCKET;

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NIL || ExecQual(joinqual, econtext, false))
				{
					node->mhj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->mhj_CurTuple));

//					/* In an antijoin, we never return a matched tuple */
//					if (node->js.jointype == JOIN_ANTI)
//					{
//						node->mhj_JoinState = HJ_NEED_NEW_OUTER;
//						continue;
//					}
//
//					/*
//					 * In a semijoin, we'll consider returning the first
//					 * match, but after that we're done with this outer tuple.
//					 */
//					if (node->js.jointype == JOIN_SEMI)
//						node->mhj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NIL ||
						ExecQual(otherqual, econtext, false))
					{
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult)
						{
							node->js.ps.ps_TupFromTlist =
								(isDone == ExprMultipleResult);


							return result;
						}
					}
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				if (!ExecMJoinNewBatch(node) )
					return NULL;	/* end of join */
				node->mhj_JoinState = HJ_NEED_NEW_INNER;


				break;
			case HJ_END:
				return NULL;
			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->mhj_JoinState);
				break;
		}
	}
	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
MJoinState *
ExecInitMJoin(HashJoin *node, EState *estate, int eflags)
{
	MJoinState *hjstate;
	Hash	   *outerHashNode;
	Hash	   *innerHashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(MJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) hjstate);
	hjstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) hjstate);
	hjstate->js.jointype = node->join.jointype;
	hjstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) hjstate);
	hjstate->hashclauses = (List *)
		ExecInitExpr((Expr *) node->hashclauses,
					 (PlanState *) hjstate);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerHashNode = (Hash *) outerPlan(node);
	innerHashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode((Plan *) outerHashNode, estate, eflags);
	innerPlanState(hjstate) = ExecInitNode((Plan *) innerHashNode, estate, eflags);



	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &hjstate->js.ps);


	//Mjoin: inner tuple slot

	/* set up null tuples for outer joins, if needed */
//	switch (node->join.jointype)
//	{
//		case JOIN_INNER:
//		case JOIN_SEMI:
//			break;
//		case JOIN_LEFT:
//		case JOIN_ANTI:
//			hjstate->mhj_NullInnerTupleSlot =
//				ExecInitNullTupleSlot(estate,
//								 ExecGetResultType(innerPlanState(hjstate)));
//			break;
//		case JOIN_RIGHT:
//			hjstate->mhj_NullOuterTupleSlot =
//				ExecInitNullTupleSlot(estate,
//								 ExecGetResultType(outerPlanState(hjstate)));
//			break;
//		case JOIN_FULL:
//			hjstate->mhj_NullOuterTupleSlot =
//				ExecInitNullTupleSlot(estate,
//								 ExecGetResultType(outerPlanState(hjstate)));
//			hjstate->mhj_NullInnerTupleSlot =
//				ExecInitNullTupleSlot(estate,
//								 ExecGetResultType(innerPlanState(hjstate)));
//			break;
//		default:
//			elog(ERROR, "unrecognized join type: %d",
//				 (int) node->join.jointype);
//	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.	-cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) outerPlanState(hjstate);
		hjstate->mhj_OuterTupleSlot = hashstate->ps.ps_ResultTupleSlot;

		hashstate = (HashState *) innerPlanState(hjstate);
		hjstate->mhj_InnerTupleSlot = hashstate->ps.ps_ResultTupleSlot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&hjstate->js.ps);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	ExecSetSlotDescriptor(hjstate->mhj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(hjstate)));

	ExecSetSlotDescriptor(hjstate->mhj_InnerTupleSlot,
							  ExecGetResultType(outerPlanState(hjstate)));

	/*
	 * initialize hash-specific info
	 */
	hjstate->mhj_InnerHashTable = NULL;
	hjstate->mhj_OuterHashTable = NULL;

	hjstate->mhj_CurHashValue = 0;
	hjstate->mhj_CurBucketNo = 0;
	hjstate->mhj_NextBucketNo =0;
	hjstate->mhj_CurTuple = NULL;


	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, hjstate->hashclauses)
	{
		FuncExprState *fstate = (FuncExprState *) lfirst(l);
		OpExpr	   *hclause;

		Assert(IsA(fstate, FuncExprState));
		hclause = (OpExpr *) fstate->xprstate.expr;
		Assert(IsA(hclause, OpExpr));
		lclauses = lappend(lclauses, linitial(fstate->args));
		rclauses = lappend(rclauses, lsecond(fstate->args));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	hjstate->mhj_OuterHashKeys = lclauses;
	hjstate->mhj_InnerHashKeys = rclauses;
	hjstate->mhj_HashOperators = hoperators;
	/* child Hash nodes need to evaluate inner/outer hash keys, too */
	((HashState *) innerPlanState(hjstate))->hashkeys = rclauses;
	((HashState *) outerPlanState(hjstate))->hashkeys = lclauses;
	((HashState *) outerPlanState(hjstate))->isOuter = true;
	((HashState *) innerPlanState(hjstate))->isOuter = false;
	hjstate->js.ps.ps_TupFromTlist = false;
	hjstate->mhj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->mhj_MatchedOuter = false;
	hjstate->mhj_OuterNotEmpty = false;

	hjstate->mhj_InnerNotEmpty = false;
	hjstate->mhj_MatchedInner = false;

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndMJoin(MJoinState *node)
{
	/*
	 * Free hash table
	 */
	printf("ENDING MJOIN");
	fflush(stdout);
	if (node->mhj_InnerHashTable)
	{
		ExecMHashTableDestroy(node->mhj_InnerHashTable);
		node->mhj_InnerHashTable = NULL;
	}
	if (node->mhj_OuterHashTable)
	{
		ExecMHashTableDestroy(node->mhj_OuterHashTable);
		node->mhj_OuterHashTable = NULL;
	}
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->mhj_OuterTupleSlot);
	ExecClearTuple(node->mhj_InnerTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing the outer plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecMJoinGetTuple(PlanState *hashNode, MJoinTable hashtable, MJoinState *hjstate, uint32 *hashvalue,
		bool outer_tuple, bool *isNotEmpty) {

	TupleTableSlot *slot = NULL;
	List *hashkeys;
	//MJoinTable hashtable = hjstate->mhj_HashTable;

	/*
	 * Check to see if first outer tuple was already fetched by
	 * ExecHashJoin() and not used yet.
	 */
	*isNotEmpty = false;
	slot = ExecProcNode(hashNode);

	while (!TupIsNull(slot)) {
		/*
		 * We have to compute the tuple's hash value.
		 */
		ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
		if (outer_tuple) {
			hashkeys = ((HashState *) outerPlanState(hjstate))->hashkeys;
			econtext->ecxt_outertuple = slot;
		} else {
			hashkeys = ((HashState *) innerPlanState(hjstate))->hashkeys;

			econtext->ecxt_innertuple = slot;
		}

		if (ExecMHashGetHashValue(hashtable, econtext, hashkeys, /* outer tuple */
		false, hashvalue)) {
			/* remember outer relation is not empty for possible rescan */

			*isNotEmpty = true;
			return slot;

		}

		/*
		 * That tuple couldn't match because of a NULL, so discard it and
		 * continue with the next one.
		 */
		slot = ExecProcNode(hashNode);
	}

	/* End of this batch */
	return slot;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecMJoinNewBatch(MJoinState *hjstate)
{
	MJoinTable innerhashtable = hjstate->mhj_InnerHashTable;
	MJoinTable outerhashtable = hjstate->mhj_OuterHashTable;
	int			nbatch;
	int			curbatch;
	BufFile    *innerFile;
	BufFile    *outerFile;
	BufFile    *savedInnerFile;
	BufFile    *savedOuterFile;
	TupleTableSlot *slot;
	uint32		hashvalue;



	nbatch = innerhashtable->nbatch;
	curbatch = innerhashtable->curbatch;

	printf("\nNew batch\n");

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 */
		if (innerhashtable->batches[curbatch]->batchFile)
			BufFileClose(innerhashtable->batches[curbatch]->batchFile);
		innerhashtable->batches[curbatch]->batchFile = NULL;

		if (outerhashtable->batches[curbatch]->batchFile)
					BufFileClose(outerhashtable->batches[curbatch]->batchFile);
		outerhashtable->batches[curbatch]->batchFile = NULL;
	}


	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a left/full outer join, we have to process outer batches even if
	 * the inner batch is empty.  Similarly, in a right/full outer join, we
	 * have to process inner batches even if the outer batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 */
	curbatch++;
	while (curbatch < nbatch &&
		   (innerhashtable->batches[curbatch] == NULL ||
				   outerhashtable->batches[curbatch] == NULL))
	{

			/* must process due to rule 1 */
		if (innerhashtable->batches[curbatch]->batchFile &&
			nbatch != innerhashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (outerhashtable->batches[curbatch]->batchFile &&
			nbatch != innerhashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (innerhashtable->batches[curbatch]->batchFile)
			BufFileClose(innerhashtable->batches[curbatch]->batchFile);
		innerhashtable->batches[curbatch]->batchFile = NULL;
		if (outerhashtable->batches[curbatch]->batchFile)
			BufFileClose(outerhashtable->batches[curbatch]->batchFile);
		outerhashtable->batches[curbatch]->batchFile = NULL;
		curbatch++;
	}

	if (curbatch >= nbatch)
		return false;			/* no more batches */

	innerhashtable->curbatch = curbatch;
	outerhashtable->curbatch = curbatch;

	/*
	 * Reload the hash table with the new inner batch (which could be empty)
	 */
	ExecMHashTableReset(innerhashtable);
	ExecMHashTableReset(outerhashtable);

	innerFile = innerhashtable->batches[curbatch]->batchFile;
	outerFile = outerhashtable->batches[curbatch]->batchFile;

	savedInnerFile = innerhashtable->batches[curbatch]->savedFile;
	savedOuterFile = outerhashtable->batches[curbatch]->savedFile;


	if (innerFile != NULL)
	{
		if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
				   errmsg("could not rewind hash-join temporary file: %m")));

		while ((slot = ExecMJoinGetSavedTuple(hjstate,
												 innerFile,
												 &hashvalue,
												 hjstate->mhj_InnerTupleSlot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
			ExecMHashTableInsert(hjstate ,innerhashtable, slot, hashvalue, false);
		}



	}
	if (savedInnerFile != NULL)
		{
			if (BufFileSeek(innerFile, 0, 0L, SEEK_SET))
				ereport(ERROR,
						(errcode_for_file_access(),
					   errmsg("could not rewind hash-join temporary file: %m")));

			while ((slot = ExecMJoinGetSavedTuple(hjstate,
													 innerFile,
													 &hashvalue,
													 hjstate->mhj_InnerTupleSlot)))
			{
				/*
				 * NOTE: some tuples may be sent to future batches.  Also, it is
				 * possible for hashtable->nbatch to be increased here!
				 */
				ExecMHashTableInsert(hjstate ,innerhashtable, slot, hashvalue, true);
			}

			/*
			 * after we build the hash table, the inner batch file is no longer
			 * needed
			 */


		}

	if(savedInnerFile)
		BufFileClose(savedInnerFile);
	if(innerFile)
		BufFileClose(innerFile);
	innerhashtable->batches[curbatch]->batchFile=NULL;
	innerhashtable->batches[curbatch]->savedFile=NULL;
	/*
	 * Rewind outer batch file (if present), so that we can start reading it.
	 */

	if (outerFile != NULL)
		{
			if (BufFileSeek(outerFile, 0, 0L, SEEK_SET))
				ereport(ERROR,
						(errcode_for_file_access(),
					   errmsg("could not rewind hash-join temporary file: %m")));

			while ((slot = ExecMJoinGetSavedTuple(hjstate,
													outerFile,
													 &hashvalue,
													 hjstate->mhj_OuterTupleSlot)))
			{
				/*
				 * NOTE: some tuples may be sent to future batches.  Also, it is
				 * possible for hashtable->nbatch to be increased here!
				 */
				ExecMHashTableInsert(hjstate ,innerhashtable, slot, hashvalue, false);
			}



		}
		if (savedOuterFile != NULL)
			{
				if (BufFileSeek(savedOuterFile, 0, 0L, SEEK_SET))
					ereport(ERROR,
							(errcode_for_file_access(),
						   errmsg("could not rewind hash-join temporary file: %m")));

				while ((slot = ExecMJoinGetSavedTuple(hjstate,
														savedOuterFile,
														 &hashvalue,
														 hjstate->mhj_OuterTupleSlot)))
				{
					/*
					 * NOTE: some tuples may be sent to future batches.  Also, it is
					 * possible for hashtable->nbatch to be increased here!
					 */
					ExecMHashTableInsert(hjstate ,outerhashtable, slot, hashvalue, true);
				}

				/*
				 * after we build the hash table, the inner batch file is no longer
				 * needed
				 */

			}
		if(savedOuterFile)
			BufFileClose(savedOuterFile);
		if(outerFile)
			BufFileClose(outerFile);
		outerhashtable->batches[curbatch]->batchFile=NULL;
		outerhashtable->batches[curbatch]->savedFile=NULL;


		return true;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecMJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr)
{
	BufFile    *file = *fileptr;
	size_t		written;

	if (file == NULL)
	{
		/* First write to this batch file, so open it. */
		file = BufFileCreateTemp(false);
		*fileptr = file;
	}

	written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
	if (written != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));

	written = BufFileWrite(file, (void *) tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.	Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecMJoinGetSavedTuple(MJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}


void
ExecReScanMHashJoin(MJoinState *node)
{
//	/*
//	 * In a multi-batch join, we currently have to do rescans the hard way,
//	 * primarily because batch temp files may have already been released. But
//	 * if it's a single-batch join, and there is no parameter change for the
//	 * inner subnode, then we can just re-use the existing hash table without
//	 * rebuilding it.
//	 */
//	if (node->mhj_InnerHashTable != NULL)
//	{
//		if (node->mhj_HashTable->nbatch == 1 &&
//			node->js.ps.righttree->chgParam == NULL)
//		{
//			/*
//			 * Okay to reuse the hash table; needn't rescan inner, either.
//			 *
//			 * However, if it's a right/full join, we'd better reset the
//			 * inner-tuple match flags contained in the table.
//			 */
//			if (HJ_FILL_INNER(node))
//				ExecHashTableResetMatchFlags(node->mhj_HashTable);
//
//			/*
//			 * Also, we need to reset our state about the emptiness of the
//			 * outer relation, so that the new scan of the outer will update
//			 * it correctly if it turns out to be empty this time. (There's no
//			 * harm in clearing it now because ExecHashJoin won't need the
//			 * info.  In the other cases, where the hash table doesn't exist
//			 * or we are destroying it, we leave this state alone because
//			 * ExecHashJoin will need it the first time through.)
//			 */
//			node->mhj_OuterNotEmpty = false;
//
//			/* ExecHashJoin can skip the BUILD_HASHTABLE step */
//			node->mhj_JoinState = HJ_NEED_NEW_OUTER;
//		}
//		else
//		{
//			/* must destroy and rebuild hash table */
//			ExecHashTableDestroy(node->mhj_HashTable);
//			node->mhj_HashTable = NULL;
//			node->mhj_JoinState = HJ_BUILD_HASHTABLE;
//
//			/*
//			 * if chgParam of subnode is not null then plan will be re-scanned
//			 * by first ExecProcNode.
//			 */
//			if (node->js.ps.righttree->chgParam == NULL)
//				ExecReScan(node->js.ps.righttree);
//		}
//	}
//
//	/* Always reset intra-tuple state */
//	node->mhj_CurHashValue = 0;
//	node->mhj_CurBucketNo = 0;
//	node->mhj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
//	node->mhj_CurTuple = NULL;
//
//	node->js.ps.ps_TupFromTlist = false;
//	node->mhj_MatchedOuter = false;
//	node->mhj_FirstOuterTupleSlot = NULL;
//
//	/*
//	 * if chgParam of subnode is not null then plan will be re-scanned by
//	 * first ExecProcNode.
//	 */
//	if (node->js.ps.lefttree->chgParam == NULL)
//		ExecReScan(node->js.ps.lefttree);
}

static bool
ExecMJoinScanHashBucket(MJoinState *hjstate,
				   ExprContext *econtext)
{
	List	   *hjclauses = hjstate->hashclauses;
	MJoinTable hashtable = hjstate->mhj_ScanHashTable;

	HashJoinTuple hashTuple = hjstate->mhj_CurTuple;
	uint32		hashvalue = hjstate->mhj_CurHashValue;


	/*
	 * hj_CurTuple is the address of the tuple last returned from the current
	 * bucket, or NULL if it's time to start scanning a new bucket.
	 *
	 * If the tuple hashed to a skew bucket then scan the skew bucket
	 * otherwise scan the standard hashtable bucket.
	 */
	if (hashTuple != NULL)
		hashTuple = hashTuple->next;
	else
		hashTuple = hashtable->buckets[hjstate->mhj_CurBucketNo];

	while (hashTuple != NULL)
	{
		if (hashTuple->hashvalue == hashvalue)
		{
			TupleTableSlot *nexttuple = *(hjstate->mhj_ScanEcxt_slot);

			/* insert hashtable's tuple into exec slot so ExecQual sees it */
			nexttuple = ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											nexttuple,
											 false);	/* do not pfree */
			*(hjstate->mhj_ScanEcxt_slot) = nexttuple;

			/* reset temp memory each time to avoid leaks from qual expr */
			ResetExprContext(econtext);

			if (ExecQual(hjclauses, econtext, false))
			{
				hjstate->mhj_CurTuple = hashTuple;
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
