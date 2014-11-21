/*-------------------------------------------------------------------------
 * renata
 * nodeSmoothIndexscan.c
 *	  Routines to support bitmapped index scans of relations
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSmoothIndexscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		MultiExecSmoothIndexScan	scans a relation using index.
 *		ExecInitSmoothIndexScan		creates and initializes state info.
 *		ExecReScanSmoothIndexScan	prepares to rescan the plan.
 *		ExecEndSmoothIndexScan		releases all storage.
 */
#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeSmoothIndexscan.h"
#include "executor/nodeIndexscan.h"
#include "miscadmin.h"
#include "utils/memutils.h"


/* ----------------------------------------------------------------
 *		MultiExecSmoothIndexScan(node)
 * ----------------------------------------------------------------
 */
Node *
MultiExecSmoothIndexScan(SmoothIndexScanState *node)
{
	TIDBitmap  *tbm;
	IndexScanDesc scandesc;
	double		nTuples = 0;
	bool		doscan;

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStartNode(node->ss.ps.instrument);

	/*
	 * extract necessary information from index scan node
	 */
	scandesc = node->sis_ScanDesc;

	/*
	 * If we have runtime keys and they've not already been set up, do it now.
	 * Array keys are also treated as runtime keys; note that if ExecReScan
	 * returns with sis_RuntimeKeysReady still false, then there is an empty
	 * array key so we should do nothing.
	 */
	if (!node->sis_RuntimeKeysReady &&
		(node->sis_NumRuntimeKeys != 0 || node->sis_NumArrayKeys != 0))
	{
		ExecReScan((PlanState *) node);
		doscan = node->sis_RuntimeKeysReady;
	}
	else
		doscan = true;

	/*
	 * Prepare the result bitmap.  Normally we just create a new one to pass
	 * back; however, our parent node is allowed to store a pre-made one into
	 * node->sis_result, in which case we just OR our tuple IDs into the
	 * existing bitmap.  (This saves needing explicit UNION steps.)
	 */
	if (node->sis_result)
	{
		tbm = node->sis_result;
		node->sis_result = NULL;		/* reset for next time */
	}
	else
	{
		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(work_mem * 1024L);
	}

	/*
	 * Get TIDs from index and insert into bitmap
	 */
	while (doscan)
	{
		nTuples += (double) index_getbitmap(scandesc, tbm);

		CHECK_FOR_INTERRUPTS();

		doscan = ExecIndexAdvanceArrayKeys(node->sis_ArrayKeys,
										   node->sis_NumArrayKeys);
		if (doscan)				/* reset index scan */
			index_rescan(node->sis_ScanDesc,
						 node->sis_ScanKeys, node->sis_NumScanKeys,
						 NULL, 0);
	}

	/* must provide our own instrumentation support */
	if (node->ss.ps.instrument)
		InstrStopNode(node->ss.ps.instrument, nTuples);

	return (Node *) tbm;
}

/* ----------------------------------------------------------------
 *		ExecReScanSmoothIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSmoothIndexScan(SmoothIndexScanState *node)
{
	ExprContext *econtext = node->sis_RuntimeContext;

	/*
	 * Reset the runtime-key context so we don't leak memory as each outer
	 * tuple is scanned.  Note this assumes that we will recalculate *all*
	 * runtime keys on each call.
	 */
	if (econtext)
		ResetExprContext(econtext);

	/*
	 * If we are doing runtime key calculations (ie, any of the index key
	 * values weren't simple Consts), compute the new key values.
	 *
	 * Array keys are also treated as runtime keys; note that if we return
	 * with sis_RuntimeKeysReady still false, then there is an empty array
	 * key so no index scan is needed.
	 */
	if (node->sis_NumRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext,
								 node->sis_RuntimeKeys,
								 node->sis_NumRuntimeKeys);
	if (node->sis_NumArrayKeys != 0)
		node->sis_RuntimeKeysReady =
			ExecIndexEvalArrayKeys(econtext,
								   node->sis_ArrayKeys,
								   node->sis_NumArrayKeys);
	else
		node->sis_RuntimeKeysReady = true;

	/* reset index scan */
	if (node->sis_RuntimeKeysReady)
		index_rescan(node->sis_ScanDesc,
					 node->sis_ScanKeys, node->sis_NumScanKeys,
					 NULL, 0);
}

/* ----------------------------------------------------------------
 *		ExecEndSmoothIndexScan
 * ----------------------------------------------------------------
 */
void
ExecEndSmoothIndexScan(SmoothIndexScanState *node)
{
	Relation	indexRelationDesc;
	IndexScanDesc indexScanDesc;

	/*
	 * extract information from the node
	 */
	indexRelationDesc = node->sis_RelationDesc;
	indexScanDesc = node->sis_ScanDesc;

	/*
	 * Free the exprcontext ... now dead code, see ExecFreeExprContext
	 */
#ifdef NOT_USED
	if (node->sis_RuntimeContext)
		FreeExprContext(node->sis_RuntimeContext, true);
#endif

	/*
	 * close the index relation (no-op if we didn't open it)
	 */
	if (indexScanDesc)
		index_endscan(indexScanDesc);
	if (indexRelationDesc)
		index_close(indexRelationDesc, NoLock);
}

/* ----------------------------------------------------------------
 *		ExecInitSmoothIndexScan
 *
 *		Initializes the index scan's state information.
 * ----------------------------------------------------------------
 */
SmoothIndexScanState *
ExecInitSmoothIndexScan(SmoothIndexScan *node, EState *estate, int eflags)
{
	SmoothIndexScanState *indexstate;
	bool		relistarget;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	indexstate = makeNode(SmoothIndexScanState);
	indexstate->ss.ps.plan = (Plan *) node;
	indexstate->ss.ps.state = estate;

	/* normally we don't make the result bitmap till runtime */
	indexstate->sis_result = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * We do not need a standard exprcontext for this node, though we may
	 * decide below to create a runtime-key exprcontext
	 */

	/*
	 * initialize child expressions
	 *
	 * We don't need to initialize targetlist or qual since neither are used.
	 *
	 * Note: we don't initialize all of the indexqual expression, only the
	 * sub-parts corresponding to runtime keys (see below).
	 */

	/*
	 * We do not open or lock the base relation here.  We assume that an
	 * ancestor SmoothHeapScan node is holding AccessShareLock (or better) on
	 * the heap relation throughout the execution of the plan tree.
	 */

	indexstate->ss.ss_currentRelation = NULL;
	indexstate->ss.ss_currentScanDesc = NULL;

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
	indexstate->sis_RelationDesc = index_open(node->sis_indexid,
									 relistarget ? NoLock : AccessShareLock);

	/*
	 * Initialize index-specific scan state
	 */
	indexstate->sis_RuntimeKeysReady = false;
	indexstate->sis_RuntimeKeys = NULL;
	indexstate->sis_NumRuntimeKeys = 0;

	/*
	 * build the index scan keys from the index qualification
	 */
	ExecIndexBuildScanKeys((PlanState *) indexstate,
						   indexstate->sis_RelationDesc,
						   node->sis_indexqual,
						   false,
						   &indexstate->sis_ScanKeys,
						   &indexstate->sis_NumScanKeys,
						   &indexstate->sis_RuntimeKeys,
						   &indexstate->sis_NumRuntimeKeys,
						   &indexstate->sis_ArrayKeys,
						   &indexstate->sis_NumArrayKeys);

	/*
	 * If we have runtime keys or array keys, we need an ExprContext to
	 * evaluate them. We could just create a "standard" plan node exprcontext,
	 * but to keep the code looking similar to nodeIndexscan.c, it seems
	 * better to stick with the approach of using a separate ExprContext.
	 */
	if (indexstate->sis_NumRuntimeKeys != 0 ||
		indexstate->sis_NumArrayKeys != 0)
	{
		ExprContext *stdecontext = indexstate->ss.ps.ps_ExprContext;

		ExecAssignExprContext(estate, &indexstate->ss.ps);
		indexstate->sis_RuntimeContext = indexstate->ss.ps.ps_ExprContext;
		indexstate->ss.ps.ps_ExprContext = stdecontext;
	}
	else
	{
		indexstate->sis_RuntimeContext = NULL;
	}

	/*
	 * Initialize scan descriptor.
	 */
	indexstate->sis_ScanDesc =
		index_beginscan_bitmap(indexstate->sis_RelationDesc,
							   estate->es_snapshot,
							   indexstate->sis_NumScanKeys);

	/*
	 * If no run-time keys to calculate, go ahead and pass the scankeys to the
	 * index AM.
	 */
	if (indexstate->sis_NumRuntimeKeys == 0 &&
		indexstate->sis_NumArrayKeys == 0)
		index_rescan(indexstate->sis_ScanDesc,
					 indexstate->sis_ScanKeys, indexstate->sis_NumScanKeys,
					 NULL, 0);

	/*
	 * all done.
	 */
	return indexstate;
}
