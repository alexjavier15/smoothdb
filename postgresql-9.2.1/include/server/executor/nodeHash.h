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
#ifndef NODEHASH_H
#define NODEHASH_H

#include "nodes/execnodes.h"
#include "smooth/joincache.h"


/**
 * Original Hash node methods
 *
 **/
extern HashState *ExecInitHash(Hash *node, EState *estate, int eflags);
extern TupleTableSlot *ExecHash(HashState *node);
extern Node *MultiExecHash(HashState *node);
extern void ExecEndHash(HashState *node);
extern void ExecReScanHash(HashState *node);
extern HashJoinTable ExecHashTableCreate(Hash *node, List *hashOperators,
					bool keepNulls);


extern void ExecHashTableDestroy(HashJoinTable hashtable);

extern void ExecHashTableInsert(HashJoinTable hashtable,
					TupleTableSlot *slot,
					uint32 hashvalue);
extern bool ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue);

extern void ExecHashGetBucketAndBatch(HashJoinTable hashtable,
						  uint32 hashvalue,
						  int *bucketno,
						  int *batchno);

extern bool ExecScanHashBucket(HashJoinState *hjstate, ExprContext *econtext);

extern void ExecPrepHashTableForUnmatched(HashJoinState *hjstate);
extern bool ExecScanHashTableForUnmatched(HashJoinState *hjstate,
							  ExprContext *econtext);
extern void ExecHashTableReset(HashJoinTable hashtable);


extern void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew,
						int *numbuckets,
						int *numbatches,
						int *num_skew_mcvs);
extern int	ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue);
/**
 * Specialized Hash node methods for Symetric hash join
 *
 **/
extern MJoinTable ExecMHashTableCreate(Hash *node, List *hashOperators, bool keepNulls, bool isLeft,
		int nbuckets, int nbatch);
extern void ExecMHashTableInsert(SymHashJoinState *mhjstate,MJoinTable hashtabledest,
					TupleTableSlot *slot,
					uint32 hashvalue, bool saved);
extern void ExecMHashTableDestroy(MJoinTable hashtable);
extern bool ExecMHashGetHashValue(MJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool keep_nulls,
					 uint32 *hashvalue);
extern void ExecMHashTableReset(MJoinTable hashtable);
extern void ExecHashTableResetMatchFlags(HashJoinTable hashtable);
extern void ExecMHashIncreaseNumBatches(SymHashJoinState *mhjstate);

/**
 * MultiHash node methods for MJOIN
 * **/

extern HashState *ExecInitMultiHash(MultiHash *node, EState *estate, int eflags);
extern void ExecMultiHashCreateHashTablesArray(MultiHashState * mhstate);
extern void ExecMultiHashTableCreate(MultiHashState *node, List *hashOperators, bool keepNulls,
		SimpleHashTable * hashtableptr);
extern Node *ExecMultiHashFillTupleCache(MultiHashState *node);
extern TupleTableSlot *ExecMultiHash(MultiHashState *node);
extern void ExecMultiHashTableInsert(SimpleHashTable hashtable, MinimalTuple tuple, uint32 hashvalue);
extern void ExecMultiHashGetBucket(SimpleHashTable hashtable,
						  uint32 hashvalue,
						  int *bucketno);
extern bool ExecMultiHashScanBucket(CHashJoinState *chjstate, ExprContext *econtext);
extern bool ExecMultiHashGetHashValue(SimpleHashTable hashtable, ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue);
extern SimpleHashTable ExecMultiHashSelectHashTable(MultiHashState * mhstate, List *hoperators, List *hashkeys,
		HashInfo **hinfo);
extern void ExecMultiHashSetHashInfo(MultiHashState *node , HashInfo **hinfo, List *hashkeys, List * hoperators);
extern HashInfo *add_hashinfo(MultiHashState *mhstate , List * clauses, List *hoperators, Bitmapset *relids);
extern HashInfo *GetUniqueHashInfo (MultiHashState *mhstate , List * clauses, List *hoperators, bool *found);

#endif   /* NODEHASH_H */
