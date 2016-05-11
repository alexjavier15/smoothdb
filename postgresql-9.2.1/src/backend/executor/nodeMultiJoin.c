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
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "access/relscan.h"

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
ExecMultiHashJoinOuterGetTuple(PlanState *outerNode, CHashJoinState *chjstate, uint32 *hashvalue);

static void ExecInitJoinCache(MultiJoinState * mhjoinstate);
static void
show_instrumentation_count(Instrumentation *instrument);

static void ExecSetSeqNumber(CHashJoinState * chjoinstate, EState *estate);
static void
show_instrumentation_count_b(Instrumentation *instrument);

static List * ExecMultiJoinPrepareSubplans(MultiJoinState * mhjoinstate, int skipid, List *startList);
static void ExecMultiJoinEndSubPlan(MultiJoinState * mhjoinstate, ChunkedSubPlan * subplan);
static ChunkedSubPlan * ExecMultiJoinGetNewSubplan(MultiJoinState * mhjoinstate);
static void ExecMultiJoinGetNewChunk(MultiJoinState * mhjoinstate);
static void ExecMultiJoiSetSubplan(MultiJoinState * mhjoinstate, ChunkedSubPlan *subplan);
static RelChunk * ExecMultiJoinChooseDroppedChunk(MultiJoinState * mhjoinstate, RelChunk *newChunk);
static RelChunk * ExecMerge(RelChunk ** chunk_array, int size, int m);
static RelChunk * ExecSortChuks(RelChunk ** chunk_array, int size);
static Selectivity ExecGetSelectivity(Instrumentation *instrument);
static CHashJoinState * ExecChoseBestPlan(MultiJoinState *node);
static void ExecCleanInfeasibleSubplans(MultiJoinState *node);
static void ExecMultiJoinMarkCurrentHashInfos(CHashJoinState * chjstate);
static void print_chunk_stats(MultiJoinState * node);

TupleTableSlot * /* return: a tuple or NULL */
ExecCHashJoin(CHashJoinState *node) {
	PlanState *outerNode;
	MultiHashState *hashNode;
	List *joinqual;
	List *otherqual;
	ExprContext *econtext;
	ExprDoneCond isDone;
	SimpleHashTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32 hashvalue;
	HashInfo *hinfo;

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
	if (node->js.ps.ps_TupFromTlist) {
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

	for (;;) {
		switch (node->chj_JoinState) {
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

				hashtable = ExecMultiHashSelectHashTable((MultiHashState *) hashNode,
						node->chj_HashOperators,
						node->chj_InnerHashKeys,
						&hinfo);
				node->chj_HashTable = hashtable;



				/*
				 * execute the Hash node, to build the hash table
				 */

//				if(hashtable->totalTuples == 0.0){
//			(void) MultiExecProcNode((PlanState *) hashNode);
				//			}
				node->js.ps.instrument->card1 = hashtable->totalTuples;
				node->js.ps.instrument->nloops = 0;
				node->js.ps.instrument->ntuples = 0;

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

				outerTupleSlot = ExecMultiHashJoinOuterGetTuple(outerNode, node, &hashvalue);

				if (TupIsNull(outerTupleSlot)) {

					node->chj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}
				node->js.ps.instrument->nloops++;

				econtext->ecxt_outertuple = outerTupleSlot;
				node->chj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->chj_CurHashValue = hashvalue;
				ExecMultiHashGetBucket(hashtable, hashvalue, &node->chj_CurBucketNo);
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
				if (!ExecMultiHashScanBucket(node, econtext)) {
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
				if (joinqual == NIL || ExecQual(joinqual, econtext, false)) {
					node->chj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(JC_ReadMinmalTuple(node->chj_HashTable->chunk,node->chj_CurTuple->mtuple));

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

					if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult) {
							node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
							//	InstrStopNode(node->js.ps.instrument, 1.0);
							node->js.ps.instrument->ntuples++;


							return result;
						}
					} else
						InstrCountFiltered2(node, 1);
				} else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->chj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->chj_MatchedOuter && HJ_FILL_OUTER(node)) {
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->chj_NullInnerTupleSlot;

					if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult) {
							node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
							return result;
						}
					} else
						InstrCountFiltered2(node, 1);
				}
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext)) {
					/* no more unmatched tuples */
					node->chj_JoinState = HJ_NEED_NEW_BATCH;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->chj_NullOuterTupleSlot;

				if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
					TupleTableSlot *result;

					result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

					if (isDone != ExprEndResult) {
						node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
						return result;
					}
				} else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:
				node->chj_JoinState = HJ_BUILD_HASHTABLE;
				node->chj_HashTable = NULL;
				node->inner_hinfo->sel = ExecGetSelectivity(node->js.ps.instrument);
				node->inner_hinfo->sel *= node->js.ps.instrument->nloops;
				if (node->outer_hinfo) {
					node->outer_hinfo->sel = ExecGetSelectivity(node->js.ps.instrument);
					node->outer_hinfo->sel *= node->js.ps.instrument->card1;
				}

				/*
				 *
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
				elog(INFO_MJOIN2,"----------------------------------");
				elog(INFO_MJOIN2,"OPTIMAL PLAN IS : ");
				elog(INFO_MJOIN2,node->current_ps->plan_relids);



				node->mhj_JoinState = MHJ_NEED_NEW_SUBPLAN;

			case MHJ_NEED_NEW_SUBPLAN: {

				ChunkedSubPlan *subplan = ExecMultiJoinGetNewSubplan(node);
				INSTR_TIME_SET_CURRENT(node->null_plans_startTime);
				InstrStartNode(node->js.ps.state->unique_instr);
				elog(INFO,"Pending Subplans : %d ", list_length(node->pendingSubplans));
				if (subplan == NULL) {

					node->mhj_JoinState = MHJ_NEED_NEW_CHUNK;
					continue;

				}
				node->mhj_JoinState = MHJ_EXEC_JOIN;
				ExecMultiJoiSetSubplan(node, subplan);
				InstrStartNode(node->counter);
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
				int ntuples =node->js.ps.state->unique_instr->ntuples;
				node->js.ps.state->started = true;


				if (TupIsNull(slot)) {

					ChunkedSubPlan *curr_subplan =  linitial(node->chunkedSubplans);
					instr_time	endtime;
					INSTR_TIME_SET_CURRENT(endtime);
					if (INSTR_TIME_IS_ZERO(node->null_plans_startTime))
					{
						elog(INFO, "Null plan timer called to stop without start");

					}
					INSTR_TIME_ACCUM_DIFF(node->null_plans_time, endtime, node->null_plans_startTime);
					INSTR_TIME_SET_ZERO(node->null_plans_startTime);
					ExecMultiJoinEndSubPlan(node,curr_subplan);
					pfree( curr_subplan);
					elog(INFO_MJOIN2,"GOT NULL TUPLE :processed tuples ");

					node->mhj_JoinState = MHJ_NEED_NEW_SUBPLAN;
					elog(INFO_MJOIN2,"----------------------------------");
					InstrEndLoop(node->js.ps.state->unique_instr);
					elog(INFO_MJOIN2,":----------------------------------\nTotal Subplan stats");
					show_instrumentation_count(&node->js.ps.state->unique_instr[0]);

					elog(INFO_MJOIN2,":-------------END---------------------");


					if (node->js.ps.state->unique_instr->ntuples ==  ntuples) {
						node->null_plans_executed++;
						InstrStopNode(node->counter,0.0);
						InstrEndLoop(node->counter);

						elog(INFO_MJOIN2,":----------------------------------\n NULL Subplan stats");

						show_instrumentation_count(node->counter);
						elog(INFO_MJOIN2,":-------------END---------------------");
						if(enable_cleaning_subplan)
						ExecCleanInfeasibleSubplans(node);

					}

					continue;


				}
				INSTR_TIME_SET_ZERO(node->null_plans_time);
				InstrStopNode(&node->js.ps.state->unique_instr[0], 1.0);


				return slot;
			}
			case MHJ_END:{
				print_chunk_stats(node);
				return NULL;
				break;
			}
			default:
				elog(ERROR, "unrecognized  Multi hjoin state: %d", (int) node->mhj_JoinState);

		}

	}

}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for CHashJoin node ( chunked join). Note: hash childs
 *		must be proper initialized by MultiJoin before initializing this nodes(
 *		all hash tables structure must be initialized and allocated as well tuples
 *		must be in cache)
 * ----------------------------------------------------------------
 */
CHashJoinState *
ExecInitCHashJoin(HashJoin *node, EState *estate, int eflags) {
	CHashJoinState *chjstate;
	Plan *outerNode;
	Hash *hashNode;
	List *lclauses;
	List *rclauses;
	List *hoperators;
	ListCell *l;
	Bitmapset *relids = NULL;
	bool addInner = false;
	HashInfo *inner_hinfo;


	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	if (node->ps != NULL)
		return (CHashJoinState *) node->ps;
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
	chjstate->js.ps.targetlist = (List *) ExecInitExpr((Expr *) node->join.plan.targetlist,
			(PlanState *) chjstate);
	chjstate->js.ps.qual = (List *) ExecInitExpr((Expr *) node->join.plan.qual,
			(PlanState *) chjstate);
	chjstate->js.jointype = node->join.jointype;
	chjstate->js.joinqual = (List *) ExecInitExpr((Expr *) node->join.joinqual,
			(PlanState *) chjstate);
	chjstate->hashclauses = (List *) ExecInitExpr((Expr *) node->hashclauses,
			(PlanState *) chjstate);

	chjstate->selstate = NULL;

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);
	ExecSetSeqNumber(chjstate, estate);
	outerPlanState(chjstate) = ExecInitNode(outerNode, estate, eflags);

	innerPlanState(chjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	chjstate->plan_relids = NIL;


	if (IsA(outerNode,HashJoin)) {

		chjstate->selstate = ((CHashJoinState *) outerPlanState(chjstate))->selstate;
	}

	if (IsA(outerPlanState(chjstate),MultiHashState)){

		relids = bms_add_member(relids, ((MultiHashState *) outerPlanState(chjstate))->relid);
		chjstate->plan_relids=lappend_int(chjstate->plan_relids,((MultiHashState *) outerPlanState(chjstate))->relid);
		addInner = true;
	}else{

		chjstate->plan_relids=lappend_int(list_copy(((CHashJoinState *) outerPlanState(chjstate))->plan_relids),((MultiHashState *) innerPlanState(chjstate))->relid);

	}

	if (IsA(innerPlanState(chjstate),MultiHashState)){

		relids = bms_add_member(relids, ((MultiHashState *) innerPlanState(chjstate))->relid);
		if(addInner)
		chjstate->plan_relids=lappend_int(chjstate->plan_relids,((MultiHashState *) innerPlanState(chjstate))->relid);
	}



	if (IsA(hashNode,HashJoin)) {

		chjstate->selstate = ((CHashJoinState *) innerPlanState(chjstate))->selstate;
	}

	Assert(relids > 0);
	if (!chjstate->selstate) {
		chjstate->selstate = makeNode(SelectivityState);
	}

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
		HashState *hashstate = (HashState *) innerPlanState(chjstate);
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
	 chjstate->outer_hinfo = NULL;
	 chjstate->inner_hinfo = NULL;
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
	foreach(l, chjstate->hashclauses) {
		FuncExprState *fstate = (FuncExprState *) lfirst(l);
		OpExpr *hclause;

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

	inner_hinfo = add_hashinfo(((MultiHashState *) innerPlanState(chjstate)),
			rclauses,
			hoperators,
			relids);


	chjstate->js.ps.ps_TupFromTlist = false;
	chjstate->chj_JoinState = HJ_BUILD_HASHTABLE;
	chjstate->chj_MatchedOuter = false;
	chjstate->chj_OuterNotEmpty = false;
	if (IsA(outerPlanState(chjstate),MultiHashState)) {
		List *copyhashkeys = copyObject(chjstate->chj_OuterHashKeys);

		Var * var = ((ExprState *) linitial(copyhashkeys))->expr;

		var->varno = INNER_VAR;
		((ExprState *) linitial(copyhashkeys))->evalfunc =
				((ExprState *) linitial(chjstate->chj_OuterHashKeys))->evalfunc;
		chjstate->outer_hinfo = add_hashinfo((MultiHashState *) outerPlanState(chjstate),
				copyhashkeys,
				chjstate->chj_HashOperators,
				NULL);

		//Safely
		chjstate->outer_hinfo->isCurrent=false;

	}

	 Assert(inner_hinfo !=  NULL);
	 chjstate->selstate->hinfo = list_append_unique(chjstate->selstate->hinfo, inner_hinfo);
	 chjstate->inner_hinfo = inner_hinfo;
		//Safely
		chjstate->inner_hinfo->isCurrent=false;
	node->ps = (PlanState *) chjstate;
	return chjstate;
}
//
/* ----------------------------------------------------------------
 *		ExecInitCHashJoin
 *
 *		Init routine for MJOIN node. All the child hash nodes are initialized
 *		here as well all the alternative join plans.
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
	u_instruments = InstrAlloc((ps_index - 1), INSTRUMENT_ROWS | INSTRUMENT_TIMER);
	mhjstate->mhashnodes = hashnodes;
	estate->unique_instr = u_instruments;

	ExecInitResultTupleSlot(estate, &mhjstate->js.ps);

	((Node *) node)->type = T_HashJoin;

	plan_list = lappend(plan_list, node);


	// Initialization of alternative join orderings

	foreach(hjplan, plan_list) {

		HashJoin *hplan = (HashJoin*) lfirst(hjplan);
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

	elog(INFO_MJOIN2,"GENERATED %d plan states ", list_length(all_plans));
	mhjstate->planlist = all_plans;
;

	// initialization of MultiHash node arrays. We do here because
	// the array depends all the hash keys from teh alterantive
	// reordered joins.
	for (i = 1; i < ps_index; i++) {

		ExecMultiHashCreateHashTablesArray((MultiHashState *) hashnodes[i]);

	}

	//Current hash join to be executed ( optimizer result  plan)
	mhjstate->current_ps = node->hashjoin.ps;
	ExecMultiJoinMarkCurrentHashInfos(mhjstate->current_ps);

	mhjstate->plans = plans;
	mhjstate->js.ps.state->replan = false;
	mhjstate->js.ps.state->started = false;

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&mhjstate->js.ps);
	ExecAssignProjectionInfo(&mhjstate->js.ps, NULL);
	mhjstate->hashnodes_array_size = ps_index;

	mhjstate->pendingSubplans = node->subplans;

	/*Set the hash join execution state machine at theright position*/
	mhjstate->mhj_JoinState = MHJ_BUILD_SUBPLANS;


	ExecInitJoinCache(mhjstate);
	mhjstate->counter =InstrAlloc(1, INSTRUMENT_ROWS | INSTRUMENT_TIMER);

	return mhjstate;
}

/* ----------------------------------------------------------------
 *		ExecMultiJoinMarkCurrentHashInfos
 *
 *		Mark the hash infos to be effective used in this join. This method must be
 *		called before reading tuples from chunks. hash table allocation and tuples insertion
 *		were only be done over marked hash tables (res hash infos).
 *
 * ----------------------------------------------------------------
 */
static void ExecMultiJoinMarkCurrentHashInfos(CHashJoinState * chjstate){

	// Mark the inner  hash info(left deep plans hash always MultiHashStates as inner
	if (IsA(innerPlanState(chjstate),MultiHashState)){

		chjstate->inner_hinfo->isCurrent=true;
	}
	// Mark the outer hash info ( deepest join case )
	if (IsA(outerPlanState(chjstate),MultiHashState)){

		chjstate->outer_hinfo->isCurrent=true;
	}else{
		// Go down in the join tree
		ExecMultiJoinMarkCurrentHashInfos(outerPlanState(chjstate));
	}

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
ExecMultiHashJoinOuterGetTuple(PlanState *outerNode, CHashJoinState *chjstate, uint32 *hashvalue) {
	SimpleHashTable hashtable = chjstate->chj_HashTable;
	TupleTableSlot *slot;

	/*
	 * Check to see if first outer tuple was already fetched by
	 * ExecHashJoin() and not used yet.
	 */

	slot = ExecProcNode(outerNode);

	for (;;) {
		if (TupIsNull(slot))
			break;
		/*
		 * We have to compute the tuple's hash value.
		 */
		ExprContext *econtext = chjstate->js.ps.ps_ExprContext;

		econtext->ecxt_outertuple = slot;
		if (ExecMultiHashGetHashValue(hashtable, econtext, chjstate->chj_OuterHashKeys, true, /* outer tuple */
		false, hashvalue)) {

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



}
void ExecEndMultiJoin(MultiJoinState *node) {
	int i;

	/*
	 * Free the exprcontext
	 */
	//MemoryContextStats(TopMemoryContext);
	ExecFreeExprContext(&node->js.ps);
	for (i = 1; i < node->hashnodes_array_size; i++) {

		ExecEndNode(node->mhashnodes[i]);

	}

	/*
	 * Free hash table
	 */

	JC_EndCache();
}

static void show_instrumentation_count(Instrumentation *instrument) {

	double nloops;
	double ntuples;

	if (!instrument)
		return;


	nloops = instrument->nloops;

	ntuples = instrument->ntuples;


	elog(INFO_MJOIN2,"ntuples : %.2lf ", ntuples);
	elog(INFO_MJOIN2,"nloops : %.2lf ", nloops);
	elog(INFO_MJOIN2,"time : %.6lf ", instrument->total);

}
static void show_instrumentation_count_b(Instrumentation *instrument) {
	double sel;
	double card1;
	double nloops;
	double tuple_count;
	double ntuples;

	card1 = instrument->card1;
	nloops = instrument->nloops;
	tuple_count = instrument->tuplecount;
	ntuples = instrument->ntuples;
	if (card1 == 0.0 || nloops == 0)
		sel = 0.0;
	else
		sel = (ntuples / (card1 * nloops));
	elog(INFO_MJOIN2,"-------------------------------------- ");
	elog(INFO_MJOIN2,"tuple_count : %.2lf ", tuple_count);
	elog(INFO_MJOIN2,"ntuples : %.2lf ", ntuples);
	elog(INFO_MJOIN2,"card inner : %.2lf ", card1);
	elog(INFO_MJOIN2,"card outer : %.2lf ", nloops);
	elog(INFO_MJOIN2,"selectivity : %.8lf ", sel);
	elog(INFO_MJOIN2,"-------------------------------------- ");

}
static void ExecSetSeqNumber(CHashJoinState * chjoinstate, EState *estate) {

	chjoinstate->seq_num = estate->join_depth;
	chjoinstate->js.ps.instrument = &estate->unique_instr[chjoinstate->seq_num + 1];
	estate->join_depth++;

}


static void ExecPrepareChunk(MultiJoinState * mhjoinstate, MultiHashState *mhstate, RelChunk *chunk) {
	SeqScanState  *outerNode;
	HeapScanDescData *scan;
	instr_time endtime;
	mhstate->hashable_array = mhstate->chunk_hashables[ChunkGetID(chunk)];
	
	elog(INFO,"PREPARING rel : %d chunk : %d", ChunkGetRelid(chunk), ChunkGetID(chunk));
	INSTR_TIME_SET_CURRENT(mhjoinstate->preparing_startTime);

	mhstate->currChunk = chunk;

	//before we read data we have to set starting point of the block
	/*
	 * get state info from node
	 */
	outerNode = (SeqScanState *) outerPlanState(mhstate);
	scan = 	(HeapScanDescData *)outerNode->ss_currentScanDesc;
	scan->num_total_blocks = chunk->numBlocks;
	scan->rs_startblock = ChunkGetID(chunk)*  multi_join_chunk_size * 1024L / BLCKSZ;
	scan->rs_inited = false;

	if (chunk->state != CH_READ){
		(void) ExecMultiHashFillTupleCache(mhstate);
		chunk->num_reads++;

	}
	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_ACCUM_DIFF(mhjoinstate->preparing_time, endtime, mhjoinstate->preparing_startTime);
	INSTR_TIME_SET_ZERO(mhjoinstate->preparing_startTime);

//	if (mhstate->needUpdate)
//		ExecMultiJoinCleanUpChunk(mhjoinstate, mhstate);

	mhstate->currTuple = (MinimalTuple ) mhstate->currChunk->head;

}

static void ExecInitJoinCache(MultiJoinState * mhjoinstate) {
	int chunksLeft = chunks_per_cycle;
	while (chunksLeft) {

		RelChunk * chunk =  JC_processNextChunk();
		MultiHashState *mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(chunk)];
		JC_InitChunkMemoryContext(chunk, NULL);
		ExecPrepareChunk(mhjoinstate, mhstate, chunk);
		chunksLeft--;

	}
	MemoryContextStats(TopMemoryContext);
	mhjoinstate->chunkedSubplans = ExecMultiJoinPrepareSubplans(mhjoinstate,0, NIL);

}
static void ExecMultiJoiSetSubplan(MultiJoinState * mhjoinstate, ChunkedSubPlan *subplan) {
	ListCell *lc;
	foreach(lc,subplan->chunks ) {
		RelChunk *chunk = (RelChunk *) lfirst(lc);
		MultiHashState *mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(chunk)];

		ExecPrepareChunk(mhjoinstate, mhstate, chunk);
		if (!mhstate->started)
			mhjoinstate->mhj_JoinState = MHJ_END;

	}

}

static void ExecMultiJoinEndSubPlan(MultiJoinState * mhjoinstate, ChunkedSubPlan * subplan) {

	List *lchunks = subplan->chunks;
	ListCell *lc;
	mhjoinstate->chunkedSubplans = list_delete(mhjoinstate->chunkedSubplans, subplan);
	mhjoinstate->pendingSubplans = list_delete(mhjoinstate->pendingSubplans, subplan);
	foreach(lc, lchunks) {

		RelChunk * chunk = (RelChunk *) lfirst(lc);
		chunk->subplans = list_delete(chunk->subplans, subplan);

	}
	list_free(subplan->chunks);



}

static ChunkedSubPlan * ExecMultiJoinGetNewSubplan(MultiJoinState * mhjoinstate) {

	if (mhjoinstate->chunkedSubplans == NIL)
		return NULL;

	return linitial(mhjoinstate->chunkedSubplans);
}
/*
 * Get a new chunk from the simulator dropping a existing chunk if needed and filling the hashtables for
 * this chunk.
 */
static void ExecMultiJoinGetNewChunk(MultiJoinState * mhjoinstate) {
	RelChunk * chunk = NULL;
	MultiHashState *mhstate = NULL;

	// loop the simulator until we find a chunk with feasible(s) join(s)
	for (;;) {
		RelChunk * toDrop = NULL;
		chunk = JC_processNextChunk();
		mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(chunk)];

		// Do we have any chunk to drop?
		if (list_length(mhstate->lchunks) >= 0) {

			toDrop = ExecMultiJoinChooseDroppedChunk(mhjoinstate, chunk);
		}

		if ((toDrop == NULL) && JC_isFull()){
			JC_removeChunk(chunk);
			continue;
		}


		JC_InitChunkMemoryContext(chunk, toDrop);

		// Cleaning stuff for dropping chunks. We handle two mean cases. If we are about reusing an already allocated
		// hash table we have to

		//persistent hash table code here

		if (toDrop != NULL && toDrop->state == CH_DROPPED) {
			MultiHashState *dropped_mhstate = mhjoinstate->mhashnodes[ChunkGetRelid(toDrop)];
			dropped_mhstate->lchunks = list_delete(dropped_mhstate->lchunks, toDrop);
			dropped_mhstate->hasDropped = true;
		}
		break;
	}
	ExecPrepareChunk(mhjoinstate, mhstate, chunk);
	if(mhjoinstate->chunkedSubplans == NIL)
	mhjoinstate->chunkedSubplans = ExecMultiJoinPrepareSubplans(mhjoinstate,0, NIL);

}

/*
 * Build the intersection of subplans for every relation in the MJOIN. User can pass an initial list of subplans for a relation
 * and the relation id to be skipped.
 *
 */

static List * ExecMultiJoinPrepareSubplans(MultiJoinState * mhjoinstate, int skipid, List *startList) {

	List *result = startList;
	int i;
	int start = skipid == 0 ? 2 :1;
	printf("skipping %d ", skipid);

	if(result == NIL){

		List *lchunks = mhjoinstate->mhashnodes[1]->lchunks;
		ListCell *lc;

		foreach(lc,lchunks) {

			result = list_union(result, ((RelChunk *) lfirst(lc))->subplans);

		}

	}

	for (i = start ; i < mhjoinstate->hashnodes_array_size; i++) {

			List *lchunks = mhjoinstate->mhashnodes[i]->lchunks;

			List *subplans = NIL;
			ListCell *lc;

			// skip if  chunks for the parent relation of the new chunk as we have already counted it at the initialization.
			if(i == skipid)
				continue;


			foreach(lc,lchunks) {
				RelChunk *chunk =(RelChunk *) lfirst(lc);
				if(list_length(chunk->subplans) == 0){
					//JC_dropChunk(chunk);
					continue;
				}
				subplans = list_union(subplans, chunk->subplans);


			}


			elog(INFO_MJOIN2,"got %d subplans for rel : %d ", list_length(subplans), i);
			// if we don't have more pending subplans for relations !=  to the parent relation of the new chunk
			//  then we don't have any possible join with this new chunk. report to the caller
			if (subplans == NIL) {
				elog(INFO_MJOIN2,"NOT more subplans for relation %d!",  i);

				result = NIL;
				break;

			}


			result = list_intersection(result, subplans);


		}
		elog(INFO,"GOT %d SUBPLANS  with new !", list_length(result));

		return result;

}






/*
 * Choose the optimal chunk to be dropped given a new comming chunk. We decide which chunk must be dropped
 * base in the algorith set by the user . We set the feasible joins in the succeful case
 *
 * 1 =  FIFO dropping strategy
 * 2 =  Lower priority strategy.
 *
 * This function return NULL if there's not any feasible join involving  the new comming chunk and the already
 * stored chunks otherwise chunkedSubplans is set as the result of the intersection of subplans for every relation
 */

static RelChunk * ExecMultiJoinChooseDroppedChunk(MultiJoinState * mhjoinstate, RelChunk *newChunk) {

	int i, pri;
		List *result = NIL;
		RelChunk **chunk_array;
		RelChunk *toDrop = NULL;
		List * chunks = JC_GetChunks();
		ListCell *lc;
		int newChunk_relid =ChunkGetRelid(newChunk);
		List *lchunks = mhjoinstate->mhashnodes[newChunk_relid]->lchunks;

		// Initialize the result with the pending subplans from the new chunk and all the pending subplans
		// of the stored chunks for the parent relation of this new chunk




		if(lchunks == NIL){

			return newChunk;
		}

		foreach(lc,lchunks) {

			result = list_union(result, ((RelChunk *) lfirst(lc))->subplans);

		}
		if(list_length(result) == 0){
			mhjoinstate->chunkedSubplans = NIL;
			return linitial(lchunks);
		}
		result = list_union(result, newChunk->subplans);


		// Paranoia : if we dont have any pending subplans with the old and new chunks for the new chunk relation return NULL.
		// it should never happens because the simulator is in charge of cleaning the useless chunks from the circular list.
		Assert (result != NIL);
		if (result == NIL){

			return NULL;

		}
		result = ExecMultiJoinPrepareSubplans(mhjoinstate,newChunk_relid, result);
		mhjoinstate->chunkedSubplans = result;

		if (result == NIL) {

			elog(INFO_MJOIN2,"NOT JOIN FOUND for chunk [ rel : %d, id : %d ] !",
					ChunkGetRelid(newChunk),
					ChunkGetID(newChunk));

			return NULL;

		}

		elog(INFO_MJOIN2,"GOT %d SUBPLANS  with new !", list_length(result));


		switch (jc_cache_policy) {

			case FIFO_DROP:
				toDrop = linitial(lchunks);
				break;
			case PRIO_DROP: {
				chunk_array = (RelChunk **) palloc((list_length(chunks) + 1) * sizeof(RelChunk *));
				i = 0;
				foreach( lc,chunks) {

					chunk_array[i] = lfirst(lc);
					i++;

				}
				chunk_array[i] = newChunk;
				foreach(lc,result) {

					ChunkedSubPlan *subplan = lfirst(lc);
					ListCell *ch;
					foreach(ch, subplan->chunks) {
						RelChunk * chunk = lfirst(ch);
						chunk->priority++;

					}

				}


				toDrop = ExecSortChuks(chunk_array, list_length(chunks));
				pri = toDrop->priority;

				// Don't delete! Implementation for choosing a lowet priority chunk
				// from the same relation as the new chunk

				bool done = false ;
				foreach( lc,chunks) {

					RelChunk *chunk = lfirst(lc);

	#if 0
					if (!done
						&& ChunkGetRelid(toDrop) != ChunkGetRelid(newChunk)
						&& chunk->priority== toDrop->priority
						&& ChunkGetRelid(chunk) == ChunkGetRelid(newChunk)) {

						toDrop = chunk;
						done = true ;
					}
	#endif
					if (chunk->priority == pri &&
					    (list_length(chunk->subplans) <
					    list_length(toDrop->subplans))) {
					    toDrop = chunk;
					}

					chunk->priority = 0;

				}

			}
				break;
			default:
				elog(ERROR, "unrecognized drop policy : %d", (int)jc_cache_policy);
				break;

		}

		Assert (toDrop != NULL);

		return toDrop;

}

static Selectivity ExecGetSelectivity(Instrumentation *instrument) {
	Selectivity sel;
	double card1;
	double nloops;

	double ntuples;

	card1 = instrument->card1;
	nloops = instrument->nloops;

	ntuples = instrument->ntuples;
	if (card1 == 0.0 || nloops == 0)
		sel = 0.0;
	else
		sel = (ntuples / (card1 * nloops));

	return sel;
}

static CHashJoinState * ExecChoseBestPlan(MultiJoinState *node) {
	ListCell *lc;
	ListCell *pc;
	CHashJoinState *best_plan = NULL;
	Selectivity last = (double) INT_MAX;

	foreach(pc,node->planlist) {
		CHashJoinState *planstate = (CHashJoinState *) lfirst(pc);

		Selectivity curr = 0.0;
		int order = 1 <<  (node->hashnodes_array_size -1);

		printf("selectivity for plan : ");
		bool found = true;

		foreach(lc, planstate->selstate->hinfo) {
			HashInfo *hinfo = (HashInfo *) lfirst(lc);
			//printf("ptr : %X", hinfo);
			printf("...........order %d ...................",order);
			if (hinfo->sel == 0) {
				curr =(double) INT_MAX;
				break;
			}

			curr += ((double)order *hinfo->sel);
			order = order >> 1;

		}

		if (curr < last) {
			printf("Cost is: %.0lf", curr);
			last = curr;

			best_plan = planstate;

		}


	}

	return node->current_ps;
}
static void * ExecRecostPlan(CHashJoinState *node) {
//	Plan *innerPlan;
//	Plan *outerPlan;
//	Plan *currPlan;
//	double
//
//	switch(node->js.ps->type){
//
//		case T_CHashJoinState :
//			ExecRecostPlan(outerPlanState(node));
//			ExecRecostPlan(innerPlanState(node));
//			currPlan = node->js.ps->plan;
//
//			currPlan->total_cost =
//
//
//	}
//

}
/* ----------- Helping methods for sorting and prioritizing chunks -------------*/
static RelChunk * ExecMerge(RelChunk ** chunk_array, int size, int m) {

	int i, j, k;
	RelChunk **tmp = palloc(size * sizeof (RelChunk *));

	for (i = 0, j = m, k = 0; k < size; k++) {
		tmp[k] =
				j == size ? chunk_array[i++] :
				i == m ? chunk_array[j++] :
				chunk_array[j]->priority < chunk_array[i]->priority ?
						chunk_array[j++] : chunk_array[i++];
	}
	for (i = 0; i < size; i++) {

		chunk_array[i] = tmp[i];

	}

	pfree(tmp);
	return chunk_array[0];
}
static RelChunk * ExecSortChuks(RelChunk ** chunk_array, int size) {
	int m;
	int right;
	if (size < 2)
		return chunk_array[0];

	m = size / 2;
	right = m * sizeof(RelChunk *);

	ExecSortChuks(chunk_array, m);
	ExecSortChuks(&chunk_array[m], size - m);
	return ExecMerge(chunk_array, size, m);

}
 static void ExecCleanInfeasibleSubplans(MultiJoinState *node){
	int start = node->hashnodes_array_size - 2 ; // reverse order
	int i = 0;
	int njoin_rel = 0;
	for (i = start; i >= 1; i--) {
		show_instrumentation_count_b(&node->js.ps.state->unique_instr[i]);

		// Detected a join who has produced zero tuples
		if (node->js.ps.state->unique_instr[i].ntuples == 0.0) {

			njoin_rel = node->hashnodes_array_size - i;

			break;
		}
	}
	node->null_plans_cleaned+=list_length(node->pendingSubplans);
	// if we detected un infaisible join extract the relations
	if (njoin_rel) {
		ListCell *lc;

		ChunkedSubPlan *p_subplan = makeNode(ChunkedSubPlan);
		ChunkedSubPlan * subplan = NULL;
		List *endSubplans = NIL;

		int i = 0;

		elog(INFO_MJOIN2,"Pending subplans before clean up: %d",
							list_length(node->pendingSubplans));
		foreach(lc,node->current_ps->plan_relids) {

			// add the cunrrent chunks not producing tuples
			p_subplan->chunks = lappend(p_subplan->chunks, node->mhashnodes[lfirst_int(lc)]->currChunk);
			i++;
			if (i == njoin_rel)
				break;
		}
		elog(INFO_MJOIN2,"Not producing tuples chunks :");

		foreach(lc,node->pendingSubplans) {
			List *lchunks = NIL;
			subplan = lfirst(lc);
			lchunks = list_difference(p_subplan->chunks, subplan->chunks);
			if (lchunks == NIL) {
				elog(INFO_MJOIN2,"Preparing for cleaning subplan with zero tuples:");
				endSubplans = lappend(endSubplans, subplan);

			}else
				list_free(lchunks);


		}
		if (endSubplans != NIL) {
			foreach(lc,endSubplans) {
				subplan = lfirst(lc);
				ExecMultiJoinEndSubPlan(node, subplan);

			}
			list_free(endSubplans);

		}
		list_free(p_subplan->chunks);
		pfree(p_subplan);
		elog(INFO_MJOIN2,"Pending subplans after clean up: %d",
						list_length(node->pendingSubplans));
	}
	node->null_plans_cleaned-=list_length(node->pendingSubplans);

}

static void print_chunk_stats(MultiJoinState * node){
	int num_requests = 0;
	int num_refuse= 0;
	int num_reads = 0;
	int num_drops=0;
	int i= 0;

	for (i = 1; i < node->hashnodes_array_size; i++) {
		ListCell *lc = NULL;
		foreach(lc,node->mhashnodes[i]->allChunks)
		{
			num_requests+=((RelChunk*)lfirst(lc))->num_request;
			num_refuse+=((RelChunk*)lfirst(lc))->num_refuse;
			num_reads+=((RelChunk*)lfirst(lc))->num_reads;
			num_drops+=((RelChunk*)lfirst(lc))->num_drops;

		}

	}
	elog(INFO,"*********Overall Chunk Stats*******");
	elog(INFO,"*********Num of requests: %d",num_requests);
	elog(INFO,"*********Num of refuses: %d",num_refuse);
	elog(INFO,"*********Num of reads: %d",num_reads);
	elog(INFO,"*********Num of drops: %d",num_drops);
	elog(INFO,"*********Num of executed subplans with NULL result : %d",node->null_plans_executed);
	elog(INFO,"*********Num of pruned subplans with NULL result : %d",node->null_plans_cleaned);
	elog(INFO,"*********Overall time for Null result plans : %.6lf ", INSTR_TIME_GET_DOUBLE(node->null_plans_time));
	elog(INFO,"*********Overall time for plans preparation(Read/Hash) : %.6lf ", INSTR_TIME_GET_DOUBLE(node->preparing_time));
	elog(INFO,"***********************************");
}
