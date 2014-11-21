/*-------------------------------------------------------------------------
 * renata
 * nodeSmoothHeapscan.h
 *
 *
 *
 * src/include/executor/nodeSmoothHeapscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESMOOTHHEAPSCAN_H
#define NODESMOOTHHEAPSCAN_H

#include "nodes/execnodes.h"

extern SmoothHeapScanState *ExecInitSmoothHeapScan(SmoothHeapScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSmoothHeapScan(SmoothHeapScanState *node);
extern void ExecEndSmoothHeapScan(SmoothHeapScanState *node);
extern void ExecReScanSmoothHeapScan(SmoothHeapScanState *node);

#endif   /* NODESMOOTHHEAPSCAN_H */
