/*-------------------------------------------------------------------------
 * renata
 * nodeSmoothIndexscan.h
 *
 *
 *
 * src/include/executor/nodeSmoothIndexscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESMOOTHINDEXSCAN_H
#define NODESMOOTHINDEXSCAN_H

#include "nodes/execnodes.h"

extern SmoothIndexScanState *ExecInitSmoothIndexScan(SmoothIndexScan *node, EState *estate, int eflags);
extern Node *MultiExecSmoothIndexScan(SmoothIndexScanState *node);
extern void ExecEndSmoothIndexScan(SmoothIndexScanState *node);
extern void ExecReScanSmoothIndexScan(SmoothIndexScanState *node);

#endif   /* NODESMOOTHINDEXSCAN_H */
