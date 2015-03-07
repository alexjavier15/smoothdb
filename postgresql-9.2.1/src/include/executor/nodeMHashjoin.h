
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

extern HashJoinState *ExecInitMHashJoin(HashJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecMHashJoin(HashJoinState *node);
extern void ExecEndMHashJoin(HashJoinState *node);
extern void ExecReScanMHashJoin(HashJoinState *node);

extern void ExecMHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr);

#endif /* NODEMHASHJOIN_H_ */

