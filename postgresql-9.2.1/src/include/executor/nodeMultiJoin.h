/*
 * nodeMultiJoin.h
 *
 *  Created on: 6 avr. 2015
 *      Author: alex
 */

#ifndef NODEMULTIJOIN_H_
#define NODEMULTIJOIN_H_


#include "nodes/execnodes.h"
#include "storage/buffile.h"
#include "smooth/joincache.h"



typedef struct JoinTupleData
{
	struct JoinTupleData *next;		/* link to next tuple in same bucket */
	uint32		hashvalue;		/* tuple's hash code */
	MinimalTuple mtuple;
}	JoinTupleData;

typedef struct JoinTupleData32
{
	struct JoinTupleData32 *next;		/* link to next tuple in same bucket */
	uint32		hashvalue;		/* tuple's hash code */
	uint32		mtuple;
}	JoinTupleData32;

#define JTUPLESIZE  MAXALIGN(sizeof(JoinTupleData))

#define JTUPLESIZE32  MAXALIGN(sizeof(JoinTupleData32))





typedef struct  SimpleHashTableData

{
	int			nbuckets;		/* # buckets in the in-memory hash table */
	int			log2_nbuckets;	/* its log2 (nbuckets must be a power of 2) */
	/* buckets[i] is head of list of tuples in i'th in-memory bucket */
	struct JoinTupleData32 **buckets;
	double		totalTuples;	/* # tuples obtained from inner plan */

//	JoinTuple	firstElement;
//	JoinTuple	freeList;
	JoinTuple32	firstElement;
	JoinTuple32	freeList;
	Size		spaceUsed;		/* memory space currently used by tuples */
	Size		spaceAllowed;	/* upper limit for space used */
	MemoryContext hashCxt;		/* context for whole-hash-join storage */
	bool	   *hashStrict;		/* is each hash join operator strict? */
	bool		growEnabled;	/* flag to shut off nbatch increases */
	bool		keepNulls;		/* true to store unmatchable NULL tuples */
	char  		pad;
	/* ^ Alex Field above must be the same as HashJoinTableData  ^*/
	FmgrInfo   *hashfunctions;	/* lookup data for hash functions */
	RelChunk   *chunk;
}	SimpleHashTableData;



typedef struct SequenceJoin{

	Node  *inner;
	Node  *dest;
	List  *project_info;
	List  *joinclause;
	List  *joinquals;





}SequenceJoin;



extern MultiJoinState *ExecInitMultiJoin(MultiJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMultiJoin(MultiJoinState *node);
extern void ExecEndMultiJoin(MultiJoinState *node);
extern void  ExecEndCHashJoin(CHashJoinState *node);
extern  TupleTableSlot *	ExecCHashJoin(CHashJoinState *node);
extern CHashJoinState *ExecInitCHashJoin(HashJoin *node, EState *estate, int eflags);
extern void ExecReScanMultiHashJoin(MultiJoinState *node);
extern void ExecResetMultiHashtable(MultiHashState *node, SimpleHashTable  * hashtables);


#endif /* NODEMULTIJOIN_H_ */
