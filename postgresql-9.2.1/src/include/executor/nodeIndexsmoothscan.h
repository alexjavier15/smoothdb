/*-------------------------------------------------------------------------
 * renata
 * nodeIndexsmoothscan.h

 *
 * src/include/executor/nodeIndexsmoothscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEINDEXSMOOTHSCAN_H
#define NODEINDEXSMOOTHSCAN_H

#include "nodes/execnodes.h"

#define SHASHKEY  MAXALIGN(sizeof(TID))

#define SHASHENTRY_MINTUPLE(shashen)  \
	((MinimalTuple) ((char *) (shashen) + SHASHKEY))

extern IndexSmoothScanState *ExecInitIndexSmoothScan(IndexSmoothScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecIndexSmoothScan(IndexSmoothScanState *node);
extern void ExecEndIndexSmoothScan(IndexSmoothScanState *node);
extern void ExecIndexSmoothMarkPos(IndexSmoothScanState *node);
extern void ExecIndexSmoothRestrPos(IndexSmoothScanState *node);
extern void ExecReScanIndexSmoothScan(IndexSmoothScanState *node);
extern bool ExecHashJoinNewBatch(IndexScanDesc scan, int batchindex);
extern void ExecResultCacheGetBatch(IndexScanDesc scan, MinimalTuple tuple,  int *batchno);
extern void ExecResultCacheGetBatchFromIndex(IndexScanDesc scan, IndexTuple tuple,  int *batchno);
extern void ExecResultCacheSwitchPartition(IndexScanDesc scan, SmoothScanOpaque smoothDesc, IndexTuple ituple );
/* renata: this is added because with smooth scan with have to follow ScanKeys for Heap Scan and not Index Scan
 * attno = 1 is actually first attribute in the table and not in the index */
extern void print_tuple(TupleDesc tupdesc, IndexTuple itup);
extern void print_heaptuple(TupleDesc tupdesc, HeapTuple tup);
extern void ExecIndexBuildSmoothScanKeys(PlanState *planstate, Relation index,
					   List *quals, bool isorderby,
					   ScanKey *scanKeys, int *numScanKeys,
					   IndexRuntimeKeyInfo **runtimeKeys, int *numRuntimeKeys,
					   IndexArrayKeyInfo **arrayKeys, int *numArrayKeys);


extern bool
smooth_resultcache_find_tuple(IndexScanDesc scan, HeapTuple tpl, BlockNumber blkn);

extern bool
smooth_resultcache_add_tuple(IndexScanDesc scan, const BlockNumber blknum, const OffsetNumber off, const HeapTuple tpl, const TupleDesc tupleDesc, List *target_list, List *qual_list, Index index, bool *pageHasOneResultTuple);

extern MinimalTuple project_tuple(const HeapTuple tuple, const TupleDesc tupleDesc, List *target_list, List *qual_list,
		Index index, Datum *values, bool * isnull);
#endif   /* NODEINDEXSMOOTHSCAN_H */
