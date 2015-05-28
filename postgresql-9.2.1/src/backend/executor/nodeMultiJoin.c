/*
 * nodeMultiJoin.c
 *
 *  Created on: 6 avr. 2015
 *      Author: alex
 */


#include "postgres.h"

#include "executor/executor.h"

#include "executor/nodeHash.h"
#include "executor/nodeMultiJoin.h"
#include "optimizer/cost.h"
#include "miscadmin.h"




#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_NEED_NEW_INNER		3
#define HJ_SCAN_BUCKET			4
#define HJ_FILL_OUTER_TUPLE		5
#define HJ_FILL_INNER_TUPLES	6
#define HJ_NEED_NEW_BATCH		7
#define HJ_END					8


#define MHJ_BUILD_SUBPLANS		1
#define MHJ_NEED_NEW_SUBPLAN	2
#define MHJ_NEED_NEW_CHUNK		3
#define MHJ_EXEC_JOIN			4
#define MHJ_END					5


#define HJ_FILL_OUTER(hjstate)	((hjstate)->chj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->chj_NullOuterTupleSlot != NULL)
static TupleTableSlot *
ExecMultiHashJoinOuterGetTuple(PlanState *outerNode,
						  CHashJoinState *chjstate,
						  uint32 *hashvalue);

static void ExecInitJoinCache(MultiJoinState * mhjoinstate);
static void
show_instrumentation_count(  PlanState *planstate);

static void ExecSetSeqNumber(CHashJoinState * chjoinstate, EState *estate );
static void
show_instrumentation_count_b( Instrumentation *instrument);

static void ExecMultiJoinPrepareSubplans(MultiJoinState * mhjoinstate);
static void ExecMultiJoinEndSubPlan(MultiJoinState * mhjoinstate, ChunkedSubPlan * subplan);
static ChunkedSubPlan * ExecMultiJoinGetNewSubplan(MultiJoinState * mhjoinstate);
static void  ExecMultiJoinGetNewChunk(MultiJoinState * mhjoinstate);
static void ExecMultiJoiSetSubplan(MultiJoinState * mhjoinstate, ChunkedSubPlan *subplan);
static RelChunk * ExecMultiJoinChooseDroppedChunk(MultiJoinState  * mhjoinstate, RelChunk *newChunk);
static RelChunk * ExecMerge(RelChunk ** chunk_array, int size, int m);
static RelChunk * ExecSortChuks(RelChunk ** chunk_array, int size);


TupleTableSlot *				/* return: a tuple or NULL */
ExecCHashJoin(CHashJoinState *node)
{
	PlanState  *outerNode;
	MultiHashState  *hashNode;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;
	ExprDoneCond isDone;
	SimpleHashTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;


	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (MultiHashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->chj_HashTable;
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
//	printf("\n Set outer tupleslot with  %d attrs \n",  ExecGetResultType(outerPlanState(node))->natts);
//		printf("\n Set inner tupleslot with  %d attrs \n",  node->chj_HashTupleSlot->tts_tupleDescriptor->natts);
//		printf("\n hashclauses are:  \n");
//		pprint(((HashJoin*)node->js.ps.plan)->hashclauses);
//		fflush(stdout);
	for (;;)
	{
		switch (node->chj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);

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

				node->chj_FirstOuterTupleSlot = NULL;

				/*
				 * create the hash table
				 */

				hashtable = ExecChooseHashTable((MultiHashState *) hashNode,
												node->chj_HashOperators,
												node->chj_InnerHashKeys);
				node->chj_HashTable = hashtable;


				/*
				 * execute the Hash node, to build the hash table
				 */

//				if(hashtable->totalTuples == 0.0){

	//			(void) MultiExecProcNode((PlanState *) hashNode);

	//			}
				node->js.ps.instrument->card1=hashtable->totalTuples;

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */

					if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
						return NULL;


				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->chj_OuterNotEmpty = false;

				node->chj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */

				outerTupleSlot = ExecMultiHashJoinOuterGetTuple(outerNode,
														   node,
														   &hashvalue);


				if (TupIsNull(outerTupleSlot))
				{
//					/* end of batch, or maybe whole join */
//					if (HJ_FILL_INNER(node))
//					{
//						/* set up to scan for unmatched inner tuples */
//						ExecPrepHashTableForUnmatched(node);
//						node->chj_JoinState = HJ_FILL_INNER_TUPLES;
//					}
//					else
						node->chj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}
//		    	printf("GOt new outer!\n");
//				fflush(stdout);
				econtext->ecxt_outertuple = outerTupleSlot;
				node->chj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->chj_CurHashValue = hashvalue;
				ExecMultiHashGetBucket(hashtable, hashvalue,
														  &node->chj_CurBucketNo);
				node->chj_CurTuple = NULL;

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */


				/* OK, let's scan the bucket for matches */
				node->chj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

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
				if (!ExecScanMultiHashBucket(node, econtext))
				{
					node->chj_JoinState = HJ_FILL_OUTER_TUPLE;
//					if(node->js.ps.state->replan == true){
//
//						return NULL;
//					}


					continue;
				}

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
					node->chj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(node->chj_CurTuple->mtuple);

//					/* In an antijoin, we never return a matched tuple */
//					if (node->js.jointype == JOIN_ANTI)
//					{
//						node->chj_JoinState = HJ_NEED_NEW_OUTER;
//						continue;
//					}
//
//					/*
//					 * In a semijoin, we'll consider returning the first
//					 * match, but after that we're done with this outer tuple.
//					 */
//					if (node->js.jointype == JOIN_SEMI)
//						node->chj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NIL ||
						ExecQual(otherqual, econtext, false))
					{
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult)
						{
							node->js.ps.ps_TupFromTlist =
								(isDone == ExprMultipleResult);
							InstrStopNode(node->js.ps.instrument, 1.0);
							InstrStopNode(node->js.ps.state->unique_instr, 1.0);
							if (node->chj_CurTuple == NULL || node->chj_CurTuple->next == NULL){

								InstrEndMultiJoinLoop(node->js.ps.state->unique_instr, node->js.ps.instrument);
								InstrEndLoop(node->js.ps.instrument);}

							if(node->js.ps.state->unique_instr->tuplecount == 0.0){
//									printf("Result  node  %d: \n", node->seq_num);
//									if(node->js.ps.state->started)
//									node->js.ps.state->replan = true;
//									show_instrumentation_count_b(&node->js.ps.state->unique_instr[4]);
//									show_instrumentation_count_b(&node->js.ps.state->unique_instr[3]);
//									show_instrumentation_count_b(&node->js.ps.state->unique_instr[2]);
//									show_instrumentation_count_b(&node->js.ps.state->unique_instr[1]);


								}


							return result;
						}
					}
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->chj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->chj_MatchedOuter &&
					HJ_FILL_OUTER(node))
				{
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->chj_NullInnerTupleSlot;

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
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					node->chj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->chj_NullOuterTupleSlot;

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
				break;

			case HJ_NEED_NEW_BATCH:
				node->chj_JoinState  = HJ_BUILD_HASHTABLE;
				node->chj_HashTable = NULL;
				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				//if (!ExecHashJoinNewBatch(node))
				return NULL; /* end of join */
				//node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d", (int) node->chj_JoinState);
		}
	}
}

TupleTableSlot *
ExecMultiJoin(MultiJoinState *node) {

	for (;;) {
		switch (node->mhj_JoinState) {

			case MHJ_BUILD_SUBPLANS:
				printf("----------------------------------\n");
				ExecMultiJoinPrepareSubplans(node);
				node->mhj_JoinState = MHJ_NEED_NEW_SUBPLAN;

			case MHJ_NEED_NEW_SUBPLAN: {
				ChunkedSubPlan *subplan = ExecMultiJoinGetNewSubplan(node);
				if (subplan == NULL) {

					node->mhj_JoinState = MHJ_NEED_NEW_CHUNK;
					continue;

				}
				node->mhj_JoinState = MHJ_EXEC_JOIN;
				ExecMultiJoiSetSubplan(node,subplan);

				break;

			}
			case MHJ_NEED_NEW_CHUNK:
				if (node->pendingSubplans == NULL) {
					node->mhj_JoinState = MHJ_END;
					continue;

				}
				ExecMultiJoinGetNewChunk(node);
				node->mhj_JoinState = MHJ_BUILD_SUBPLANS;
				break;
			case MHJ_EXEC_JOIN: {
				TupleTableSlot * slot = ExecProcNode(node->current_ps);

				node->js.ps.state->started = true;
				if (TupIsNull(slot)) {
					ExecMultiJoinEndSubPlan(node,  linitial(node->chunkedSubplans));
					printf("GOT NULL TUPLE :processed tuples %.0f!\n", node->js.ps.state->unique_instr[0].tuplecount);

					node->mhj_JoinState = MHJ_NEED_NEW_SUBPLAN;
					printf("----------------------------------\n");
					fflush(stdout);
					continue;

//					if (node->js.ps.state->replan) {
//						CHashJoinState * newPlan = NULL;
//						ListCell *lc;
//						int idx = 0;
//						printf("REPLANNING !\n");
//						foreach(lc, node->plans[4]) {
//							newPlan = (CHashJoinState *) lfirst(lc);
//							if (newPlan != node->current_ps)
//								break;
//							idx++;
//						}
//						printf("GOT PLAN %d  !\n", idx);
//
//						node->current_ps = newPlan;
//						node->js.ps.state->replan = false;
//						node->js.ps.state->started = false;
//						slot = ExecProcNode(node->current_ps);
//					}
//
//					fflush(stdout);
				}

				return slot;
			}
			case MHJ_END:
				return NULL;
				break;
			default:
				elog(ERROR, "unrecognized  Multi hjoin state: %d", (int) node->mhj_JoinState);


		}

	}

}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
CHashJoinState *
ExecInitCHashJoin(HashJoin *node, EState *estate, int eflags)
{
	CHashJoinState *chjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	List	   *lclauses;
	List	   *rclauses;
	List	   *hoperators;
	ListCell   *l;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));


	if(node->ps != NULL)
		return (CHashJoinState *)node->ps;
	/*
	 * create state structure
	 */
	chjstate = makeNode(CHashJoinState);
	chjstate->js.ps.plan = (Plan *) node;
	chjstate->js.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &chjstate->js.ps);

	/*
	 * initialize child expressions
	 */
	chjstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) chjstate);
	chjstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) chjstate);
	chjstate->js.jointype = node->join.jointype;
	chjstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) chjstate);
	chjstate->hashclauses = (List *)
		ExecInitExpr((Expr *) node->hashclauses,
					 (PlanState *) chjstate);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);
	ExecSetSeqNumber(chjstate,estate);
	outerPlanState(chjstate) = ExecInitNode(outerNode, estate, eflags);
	innerPlanState(chjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);


	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &chjstate->js.ps);
	chjstate->chj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	chjstate->chj_NullOuterTupleSlot = NULL;

	chjstate->chj_NullInnerTupleSlot = NULL;


	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.	-cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(chjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		chjstate->chj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&chjstate->js.ps);
	ExecAssignProjectionInfo(&chjstate->js.ps, NULL);

	ExecSetSlotDescriptor(chjstate->chj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(chjstate)));

	/*
	 * initialize hash-specific info
	 */
	chjstate->chj_HashTable = NULL;
	chjstate->chj_FirstOuterTupleSlot = NULL;

	chjstate->chj_CurHashValue = 0;
	chjstate->chj_CurBucketNo = 0;
	chjstate->chj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	lclauses = NIL;
	rclauses = NIL;
	hoperators = NIL;
	foreach(l, chjstate->hashclauses)
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
	chjstate->chj_OuterHashKeys = lclauses;
	chjstate->chj_InnerHashKeys = rclauses;
	chjstate->chj_HashOperators = hoperators;

	/* child Hash node needs to evaluate inner hash keys, too */
	((HashState *) innerPlanState(chjstate))->hashkeys = rclauses;
	add_hashinfo(((MultiHashState *) innerPlanState(chjstate)), rclauses, hoperators);


	chjstate->js.ps.ps_TupFromTlist = false;
	chjstate->chj_JoinState = HJ_BUILD_HASHTABLE;
	chjstate->chj_MatchedOuter = false;
	chjstate->chj_OuterNotEmpty = false;

	node->ps = (PlanState *)chjstate;
	return chjstate;
}
//
/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
MultiJoinState *
ExecInitMultiJoin(MultiJoin *node, EState *estate, int eflags) {
	MultiJoinState *mhjstate;
	List *all_plans = NIL;
	ListCell *mhash;
	ListCell *hjplan;
	MultiHashState ** hashnodes;
	List ** plans;
	List *mhnodes = NIL;
	List * hash_list = node->hash_plans;
	List * plan_list = node->plan_list;
	int ps_index = 1;
	int join_depth = 1;
	int i;
	Instrumentation *u_instruments;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	mhjstate = makeNode(MultiJoinState);
	mhjstate->js.ps.plan = (Plan *) node;
	mhjstate->js.ps.state = estate;
	mhjstate->planlist = NIL;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	//ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */

	hashnodes = (MultiHashState **) palloc((list_length(hash_list)+1) * sizeof(MultiHashState *));
	plans = (List **) palloc((list_length(hash_list)+1) * sizeof(List *));
	hashnodes[0] = NULL;

	foreach(mhash, hash_list) {
		Plan * plan = (Plan *) lfirst(mhash);
		MultiHashState * ps = (MultiHashState *) ExecInitNode((Plan *) plan, estate, eflags);
		plans[ps_index] = NIL;
		hashnodes[ps_index] = ps;

		ps_index++;
	}
	u_instruments = InstrAlloc((ps_index - 1), INSTRUMENT_ROWS);
	mhjstate->mhashnodes = hashnodes;
	estate->unique_instr = u_instruments;

	ExecInitResultTupleSlot(estate, &mhjstate->js.ps);

	((Node *) node)->type = T_HashJoin;

	plan_list = lappend(plan_list, node);

	foreach(hjplan, plan_list) {

		HashJoin *hplan = (HashJoin*) lfirst(hjplan);
		//pprint(hplan);
		HashJoinState * hj_state = (HashJoinState *) ExecInitNode((Plan *) hplan, estate, eflags);
		PlanState * deep_outer = hj_state->js.ps.lefttree;
		Index driverId = -1;
		while (deep_outer->lefttree != NULL) {

			deep_outer = deep_outer->lefttree;
		}
		driverId = ((Scan *) deep_outer->plan)->scanrelid;


		estate->join_depth = 0;
		plans[driverId] = lappend(plans[driverId], hj_state);
		all_plans = lappend(all_plans, hj_state);

	}

	printf("GENERATED %d plan states \n", list_length(all_plans));
	fflush(stdout);

	for (i = 1; i < ps_index; i++) {

		ExecMultiHashCreateHashTables((MultiHashState *) hashnodes[i]);

	}

	mhjstate->current_ps = node->hashjoin.ps;

	mhjstate->plans = plans;
	mhjstate->js.ps.state->replan = false;
	mhjstate->js.ps.state->started = false;

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&mhjstate->js.ps);
	ExecAssignProjectionInfo(&mhjstate->js.ps, NULL);
	mhjstate->hashnodes_array_size= ps_index;

	mhjstate->pendingSubplans = node->subplans;

	mhjstate->mhj_JoinState = MHJ_BUILD_SUBPLANS;
	ExecInitJoinCache(mhjstate);

	return mhjstate;
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
ExecMultiHashJoinOuterGetTuple(PlanState *outerNode,
						  CHashJoinState *chjstate,
						  uint32 *hashvalue)
{
	SimpleHashTable hashtable = chjstate->chj_HashTable;
	TupleTableSlot *slot;


		/*
		 * Check to see if first outer tuple was already fetched by
		 * ExecHashJoin() and not used yet.
		 */

		slot = ExecProcNode(outerNode);

		for(;;)
		{
			 if(TupIsNull(slot))
				 break;
			/*
			 * We have to compute the tuple's hash value.
			 */
			ExprContext *econtext = chjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecMultiHashGetHashValue(hashtable, econtext,
									 chjstate->chj_OuterHashKeys,
									 true,		/* outer tuple */
									 false,
									 hashvalue))
			{

				/* remember outer relation is not empty for possible rescan */
				chjstate->chj_OuterNotEmpty = true;

				return slot;
			}

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */

		}


	/* End of this batch */
	return NULL;
}

//
///* ----------------------------------------------------------------
// *		ExecEndCHashJoin
// *
// *		clean up routine for HashJoin node
// * ----------------------------------------------------------------
// */

void ExecEndCHashJoin(CHashJoinState *node) {

	/*
		 * Free the exprcontext
		 */
		ExecFreeExprContext(&node->js.ps);

//	/*
//	 * Free hash table
//	 */
//	MemoryContextStats(node->mhj_InnerHashTable->hashCxt);
//	MemoryContextStats(node->mhj_OuterHashTable->hashCxt);
//	for (; bno < node->mhj_InnerHashTable->nbatch; bno++) {
//
//		printf("Info pour inner ;\n");
//		printf("nentries : %d \n", node->mhj_InnerHashTable->batches[bno]->nentries);
//		printf("Info pour outer ;\n");
//		printf("nentries : %d \n", node->mhj_OuterHashTable->batches[bno]->nentries);
//
//	}
//
//	printf(" \nENDING MJOIN");
//	fflush(stdout);
//	if (node->mhj_InnerHashTable) {
//		ExecMHashTableDestroy(node->mhj_InnerHashTable);
//
//	}
//	if (node->mhj_OuterHashTable) {
//		ExecMHashTableDestroy(node->mhj_OuterHashTable);
//
//	}
//	/*
//	 * Free the exprcontext
//	 */
//	ExecFreeExprContext(&node->js.ps);
//
//	/*
//	 * clean out the tuple table
//	 */
//	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
//	ExecClearTuple(node->mhj_OuterTupleSlot);
//	ExecClearTuple(node->mhj_InnerTupleSlot);
//
//	/*
//	 * clean up subtrees
//	 */
//	ExecEndNode(outerPlanState(node));
//	ExecEndNode(innerPlanState(node));

}
void ExecEndMultiJoin(MultiJoinState *node) {
	int i;

	/*
		 * Free the exprcontext
		 */
		ExecFreeExprContext(&node->js.ps);
		for(i = 1; i<node->hashnodes_array_size ; i++){

			ExecEndNode(node->mhashnodes[i]);

		}


//	/*
//	 * Free hash table
//	 */

	JC_EndCache();
//	for (; bno < node->mhj_InnerHashTable->nbatch; bno++) {
//
//		printf("Info pour inner ;\n");
//		printf("nentries : %d \n", node->mhj_InnerHashTable->batches[bno]->nentries);
//		printf("Info pour outer ;\n");
//		printf("nentries : %d \n", node->mhj_OuterHashTable->batches[bno]->nentries);
//
//	}
//
//	printf(" \nENDING MJOIN");
//	fflush(stdout);
//	if (node->mhj_InnerHashTable) {
//		ExecMHashTableDestroy(node->mhj_InnerHashTable);
//
//	}
//	if (node->mhj_OuterHashTable) {
//		ExecMHashTableDestroy(node->mhj_OuterHashTable);
//
//	}
//	/*
//	 * Free the exprcontext
//	 */
//	ExecFreeExprContext(&node->js.ps);
//
//	/*
//	 * clean out the tuple table
//	 */
//	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
//	ExecClearTuple(node->mhj_OuterTupleSlot);
//	ExecClearTuple(node->mhj_InnerTupleSlot);
//
//	/*
//	 * clean up subtrees
//	 */
//	ExecEndNode(outerPlanState(node));
//	ExecEndNode(innerPlanState(node));
}


static void
show_instrumentation_count( PlanState *planstate)
{
	double		nfiltered1;
	double		nfiltered2;
	double		nloops;
	double      tuple_count;
	double      ntuples;

	if ( !planstate->instrument)
		return;

	nfiltered1 = planstate->instrument->nfiltered2;

	nfiltered2 = planstate->instrument->nfiltered1;
	nloops = planstate->instrument->nloops;
	tuple_count = planstate->instrument->tuplecount;
	ntuples = planstate->instrument->ntuples;
	printf("\nnfiltered1 : %.2lf \n", nfiltered1);
	printf("nfiltered2 : %.2lf \n", nfiltered2);
	printf("tuple_count : %.2lf \n", tuple_count);
	printf("ntuples : %.2lf \n", ntuples);
	printf("nloops : %.2lf \n", nloops);
	fflush(stdout);

}
static void
show_instrumentation_count_b( Instrumentation *instrument)
{
	double		sel;
	double		card1;
	double		nloops;
	double      tuple_count;
	double      ntuples;





	card1 = instrument->card1;
	nloops = instrument->nloops;
	tuple_count = instrument->tuplecount;
	ntuples = instrument->ntuples;
	if (card1 == 0.0 || nloops == 0)
		sel = 0.0;
	else
		sel = (ntuples / (card1 * nloops));
//	printf("\nnfiltered1 : %.2lf \n", nfiltered1);
//	printf("nfiltered2 : %.2lf \n", nfiltered2);
	printf("-------------------------------------- \n");
	printf("tuple_count : %.2lf \n", tuple_count);
	printf("ntuples : %.2lf \n", ntuples);
	printf("card inner : %.2lf \n", card1);
	printf("card outer : %.2lf \n", nloops);
	printf("selectivity : %.8lf \n", sel);
	printf("-------------------------------------- \n");
	fflush(stdout);

}
static void ExecSetSeqNumber(CHashJoinState * chjoinstate, EState *estate ){

	chjoinstate->seq_num = estate->join_depth;
	chjoinstate->js.ps.instrument = &estate->unique_instr[chjoinstate->seq_num + 1];
	estate->join_depth++;

}

static void ExecMultiJoinCleanUpChunk(MultiJoinState * mhjoinstate, MultiHashState *mhstate) {
	Bitmapset *tmpset;
	ListCell *lc;
	List *freelist = NIL;
	List *endSubplans = NIL;
	bool mustclean = false;

	foreach(lc, mhstate->allChunks) {

		RelChunk *chunk = (RelChunk *) lfirst(lc);

		if (!bms_is_member(ChunkGetID(chunk), mhstate->chunkIds)) {
			endSubplans = list_union( chunk->subplans, endSubplans);
			freelist = lappend(freelist, chunk);
			mustclean = true;

		}
	}

	if (mustclean) {
		printf("Pending subplans before clean up: %d\n", list_length(mhjoinstate->pendingSubplans));
		foreach(lc, endSubplans) {

			ChunkedSubPlan *subplan = (ChunkedSubPlan *) lfirst(lc);
			ExecMultiJoinEndSubPlan(mhjoinstate, subplan);

		}
		list_free(endSubplans);
		foreach(lc, freelist) {

			RelChunk *chunk = (RelChunk *) lfirst(lc);
			mhstate->allChunks = list_delete(mhstate->allChunks , chunk);
			JC_DeleteChunk(chunk);

		}
		printf("Pending subplans after clean up: %d\n", list_length(mhjoinstate->pendingSubplans));

	}

}


static void ExecPrepareChunk(MultiJoinState * mhjoinstate ,MultiHashState *mhstate, RelChunk *chunk) {


	mhstate->hashable_array = mhstate->chunk_hashables[ChunkGetID(chunk)];;
	printf("PREPARING rel : %d chunk : %d\n", ChunkGetRelid(chunk),ChunkGetID(chunk));
	fflush(stdout);

	mhstate->currChunk = chunk;
	if (chunk->state == CH_DROPPED) {

		ExecResetMultiHashtable(mhstate, mhstate->chunk_hashables[ChunkGetID(chunk)]);
	}




	if (chunk->state != CH_READ)
		(void) MultiExecProcNode((PlanState *) mhstate);

	if(mhstate->needUpdate)
		ExecMultiJoinCleanUpChunk(mhjoinstate, mhstate);

	mhstate->currTuple = list_head(mhstate->currChunk->tuple_list);


}

static void ExecInitJoinCache(MultiJoinState * mhjoinstate) {
	int chunksLeft = chunks_per_cycle;

	while(chunksLeft){

		ExecMultiJoinGetNewChunk(mhjoinstate);
		chunksLeft--;

	}

}
static void ExecMultiJoiSetSubplan(MultiJoinState * mhjoinstate, ChunkedSubPlan *subplan) {
	ListCell *lc;
	foreach(lc,subplan->chunks ){
		RelChunk *chunk = (RelChunk *)lfirst(lc);
		MultiHashState *mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(chunk)];

	    ExecPrepareChunk(mhjoinstate,mhstate,chunk);
	    if(!mhstate->started)
	    	mhjoinstate->mhj_JoinState = MHJ_END;

	}
}

static void ExecMultiJoinEndSubPlan(MultiJoinState * mhjoinstate, ChunkedSubPlan * subplan){

	//ChunkedSubPlan * subplan = linitial(mhjoinstate->chunkedSubplans);
	List *lchunks = subplan->chunks;
	ListCell *lc;
	mhjoinstate->chunkedSubplans = list_delete(mhjoinstate->chunkedSubplans, subplan);
	mhjoinstate->pendingSubplans = list_delete(mhjoinstate->pendingSubplans, subplan);
	foreach(lc, lchunks){

		RelChunk * chunk = (RelChunk *) lfirst(lc);
		chunk->subplans  = list_delete(chunk->subplans, subplan);

	}
	list_free(subplan->chunks);
	pfree(subplan);
	printf("ENING SUBPLAN !\n");



}

static ChunkedSubPlan * ExecMultiJoinGetNewSubplan(MultiJoinState * mhjoinstate){

	if(mhjoinstate->chunkedSubplans == NIL)
		return NULL;

	return  linitial(mhjoinstate->chunkedSubplans);
}

static void  ExecMultiJoinGetNewChunk(MultiJoinState * mhjoinstate){

	RelChunk * chunk = JC_processNextChunk();
	RelChunk * toDrop = NULL;
	MultiHashState *mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(chunk)];
	if (list_length(mhstate->lchunks) > 0) {
		if (jc_cache_policy == 1)
			toDrop = linitial( mhstate->lchunks);
		else
			toDrop = ExecMultiJoinChooseDroppedChunk(mhjoinstate, chunk);
	}
	JC_InitChunkMemoryContext(chunk, toDrop);
	if(toDrop != NULL && toDrop->state  == CH_DROPPED){

		mhstate->lchunks = list_delete_first( mhstate->lchunks);
		mhstate->hasDropped = true;

	}

	ExecPrepareChunk(mhjoinstate,mhstate, chunk);

}
static void ExecMultiJoinPrepareSubplans(MultiJoinState * mhjoinstate){

	int i;
	List *result = NIL;
	printf("Preparing SUBPLAN !\n");

	for (i = 1; i < mhjoinstate->hashnodes_array_size ; i++){

		List *lchunks = mhjoinstate->mhashnodes[i]->lchunks;
		List *subplans = NIL;
		ListCell *lc;

		foreach(lc,lchunks) {
			RelChunk *chunk = (RelChunk *)lfirst(lc);


			subplans = list_union(subplans, chunk->subplans);

		}
		printf("got %d subplans for rel : %d \n", list_length(subplans), i );
		if (subplans == NIL) {
			result = NIL;
			break;

		}
		if (result == NIL)
			result = subplans;
		else
			result = list_intersection(result, subplans);


	}
	printf("GOT %d SUBPLANS !\n", list_length(result));

	mhjoinstate->chunkedSubplans = result;


}
static RelChunk * ExecMerge(RelChunk ** chunk_array, int size, int m) {

	int i, j, k;
	RelChunk **tmp = palloc(size * sizeof (RelChunk *));

	for (i = 0, j = m, k = 0; k < size; k++) {
		tmp[k] =
				j == size ? chunk_array[i++] :
				i == m ? chunk_array[j++] :
				chunk_array[j]->priority < chunk_array[i]->priority ?	chunk_array[j++] :
						chunk_array[i++];
	}
	for (i = 0; i < size; i++) {

		chunk_array[i] = tmp[i];

	}

	pfree(tmp);
	return chunk_array[0];
}
static RelChunk * ExecSortChuks(RelChunk ** chunk_array, int size){
	int m ;
	int right;
	if(size < 2)
		return chunk_array[0];

	m = size / 2;
	right = m * sizeof(RelChunk *) ;

	ExecSortChuks(chunk_array, m);
	ExecSortChuks( &chunk_array[m] , size - m);
	return ExecMerge(chunk_array, size, m);


}

static RelChunk * ExecMultiJoinChooseDroppedChunk(MultiJoinState  * mhjoinstate, RelChunk *newChunk){

	int i;
	List *result = NIL;
	RelChunk **chunk_array;
	RelChunk *toDrop;
	List  * chunks = JC_GetChunks();
	ListCell *lc;


	for (i = 1; i < mhjoinstate->hashnodes_array_size ; i++){

		List *lchunks = mhjoinstate->mhashnodes[i]->lchunks;

		List *subplans = NIL;
		ListCell *lc;

		foreach(lc,lchunks) {


			subplans = list_union(subplans, ((RelChunk *)lfirst(lc))->subplans);

		}
		if(i == ChunkGetRelid(newChunk))
			subplans= list_union(subplans, newChunk->subplans);

		printf("got %d subplans for rel : %d \n", list_length(subplans), i );
		if(subplans == NIL){
			result = NIL;
			break;

		}

		if (result == NIL)
			result = subplans;
		else
			result = list_intersection(result, subplans);


	}
	printf("GOT %d SUBPLANS  with new !\n", list_length(result));
	chunk_array = (RelChunk **) palloc(list_length(chunks) * sizeof(RelChunk *));
	i = 0;
	foreach( lc,chunks) {

		chunk_array[i] = lfirst(lc);
		i++;

	}
	foreach(lc,result) {

		ChunkedSubPlan *subplan = lfirst(lc);
		ListCell *ch;
		foreach(ch, subplan->chunks) {
			RelChunk * chunk = lfirst(ch);
			chunk->priority++;

		}

	}

	toDrop = ExecSortChuks(chunk_array, list_length(chunks));
	foreach( lc,chunks) {

		RelChunk *chunk = lfirst(lc);
		printf("rel : %d chunk : %d,  prio %d\n", ChunkGetRelid(chunk),ChunkGetID(chunk), chunk->priority);
		fflush(stdout);
		chunk->priority = 0;

	}

	return toDrop;




}

