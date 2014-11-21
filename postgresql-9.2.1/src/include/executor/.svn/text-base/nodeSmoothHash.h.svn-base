/*-------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHash.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESMOOTHHASH_H
#define NODESMOOTHHASH_H

#include "nodes/execnodes.h"

extern HashState *ExecInitSmoothHash(Hash *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSmoothHash(HashState *node);
extern Node *MultiExecSmoothHash(HashState *node);
extern void ExecEndSmoothHash(HashState *node);
extern void ExecReScanSmoothHash(HashState *node);

extern HashJoinTable ExecSmoothHashTableCreate(Hash *node, List *hashOperators,
					bool keepNulls);
extern void ExecSmoothHashTableDestroy(HashJoinTable hashtable);
extern void ExecSmoothHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue);
extern bool ExecSmoothHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue);
extern void ExecSmoothHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno);
extern bool ExecScanSmoothHashBucket(HashJoinState *hjstate, ExprContext *econtext);
extern void ExecPrepSmoothHashTableForUnmatched(HashJoinState *hjstate);
extern bool ExecScanSmoothHashTableForUnmatched(HashJoinState *hjstate,
							  ExprContext *econtext);
extern void ExecSmoothHashTableReset(HashJoinTable hashtable);
extern void ExecSmoothHashTableResetMatchFlags(HashJoinTable hashtable);
extern void ExecChooseSmoothHashTableSize(double ntuples, int tupwidth, bool useskew,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs);
extern int	ExecSmoothHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue);

#endif   /* NODESMOOTHHASH_H */
