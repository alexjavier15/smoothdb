
/*
 * nodeMHashJoin.h
 *
 *  Created on: 5 mars 2015
 *      Author: alex
 *	  prototypes for nodeMHashjoin.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeMHashjoin.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODEMHASHJOIN_H_
#define NODEMHASHJOIN_H_





#include "nodes/execnodes.h"
#include "storage/buffile.h"

extern MJoinState *ExecInitMJoin(HashJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMJoin(MJoinState *node);
extern void ExecEndMJoin(MJoinState *node);
extern void ExecReScanMHashJoin(MJoinState *node);


typedef enum
{
	MHJ_EMPTY,				/* no entries in the hashtable*/
	MHJ_HASH,				/* we have has table */
	MHJ_FULL,				/* No more tuples can be added in the in memory batch*/
	MHJ_EXAHUSTED 			/* No more tuples can be fetched from the child plan */
} HashTableStatus;

typedef struct MJoinBatchData{

	BufFile *batchFile;
	BufFile *savedFile;

	int		nentries;


}MJoinBatchData;

typedef struct MJoinBatchData *MJoinBatchDesc;

typedef struct MJoinTableData

{
	int			nbuckets;		/* # buckets in the in-memory hash table */
	int			log2_nbuckets;	/* its log2 (nbuckets must be a power of 2) */

	/* buckets[i] is head of list of tuples in i'th in-memory bucket */
	struct HashJoinTupleData **buckets;
	/* buckets array is per-batch storage, as are all the tuples */

	bool		keepNulls;		/* true to store unmatchable NULL tuples */
	int			nbatch;			/* number of batches */
	int			curbatch;		/* current batch #; 0 during 1st pass */

	int			nbatch_original;	/* nbatch when we started inner scan */
	int			nbatch_outstart;	/* nbatch when we started outer scan */

	bool		growEnabled;	/* flag to shut off nbatch increases */

	double		totalTuples;	/* # tuples obtained from inner plan */


	/*
	 * These arrays are allocated for the life of the hash join, but only if
	 * nbatch > 1.	A file is opened only when we first write a tuple into it
	 * (otherwise its pointer remains NULL).  Note that the zero'th array
	 * elements never get used, since we will process rather than dump out any
	 * tuples of batch zero.
	 */
	MJoinBatchDesc   *batches; /* buffered virtual temp file per batch */

	/*
	 * Info about the datatype-specific hash functions for the datatypes being
	 * hashed. These are arrays of the same length as the number of hash join
	 * clauses (hash keys).
	 */
	FmgrInfo   *hashfunctions;	/* lookup data for hash functions */
	bool	   *hashStrict;		/* is each hash join operator strict? */

	Size		spaceUsed;		/* memory space currently used by tuples */
	Size		spaceAllowed;	/* upper limit for space used */
	Size		spacePeak;		/* peak space used */


	MemoryContext hashCxt;		/* context for whole-hash-join storage */
	MemoryContext batchCxt;		/* context for this-batch-only storage */
	/* ^ Alex Field above must be the same as HashJoinTableData  ^*/
	struct HashJoinTupleData *bufferedBuckets;
	MJoinState *parent;
	HashTableStatus status;
}	MJoinTableData;

#endif /* NODEMHASHJOIN_H_ */

