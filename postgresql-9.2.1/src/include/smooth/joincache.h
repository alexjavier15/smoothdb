/*
 * joincache.h
 *
 *  Created on: 7 mai 2015
 *      Author: alex
 *
 *
 *     "smooth/joincache.h"
 */

#ifndef JOINCACHE_H_
#define JOINCACHE_H_

#include "postgres.h"
#include "utils/rel.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "access/htup.h"
#define MAX_REL_NAME 128

typedef struct JoinCacheKey JoinCacheKey;
typedef struct JChunkData JChunkData;
typedef struct JChunkData *JChunk;
typedef struct RelationChunks RelationChunks;
typedef struct RelationChunks *RelationChunksDesc;



#define JCACHE_ENTRY_SIZE  MAXALIGN(sizeof(JoinCacheEntry))

#define JCReceiveChunk(chunk) chunk->state = CH_RECEIVED;

#define JCDropChunk(chunk) chunk->state = CH_DROPPED;

#define ChunkGetRelid(chunk)  ( (uint32)(chunk->chunkID >> 16))

#define ChunkGetID(chunk)  ( (uint16)(chunk->chunkID ))

struct JoinCacheKey{

	ChunkID chunkID;  // 32 bit 16bit_hi : relid, 16_bit_lo :chunk id
};

typedef struct JoinCacheEntry
{
	ChunkID chunkID;			/* Tuple id number (hashtable key) */			/* tuple = value*/
	RelChunk *relChunk;
} JoinCacheEntry;


struct RelationChunks{

	uint32	total_mem;
	uint32	chunksize;
	uint32	num_chunks;
	uint32	alloc_chunks;
	JChunk  *chunk_array;


};

#define RELCHUNCKS_SIZE(nchunks)	 (MAXALIGN(sizeof(RelationChunks) + \
									(nchunks -1)*sizeof(JChunk)))

#define JCHUNK_HAS_FREE	0x0001
#define JChunkHasFreeSpace(chunk)
#define JCHUNK_OVERHEAD	MAXALIGN(sizeof(JChunkData))

#define JCHUNK_HEAD(chunk) ( (char *)chunk + JCHUNK_OVERHEAD)

#define JCHUNK_NEXT(mtup) ( (char *)mtup +   MAXALIGN(mtup->t_len))

#define JCHUNK_SIZE(chunk)	(JCHUNK_OVERHEAD + (chunk->jc_len))
#define JCHUNK_MAX_OFFSET(chunk) ( (size_t)((chunk) + JCHUNK_OVERHEAD \
									+ ((chunk)->jc_len)))

#define foreach_mtup(mtup, chunk)	\
	for ((mtup) = JCHUNK_HEAD(chunk); (mtup) <  JCHUNK_MAX_OFFSET(chunk); JCHUNK_NEXT(mtup))
struct JChunkData{

	uint32	jc_id;
	uint32	jc_len;
	uint32  freeoffset;
	/* Data follows */

};

typedef struct JCacheMemHeader	/* standard header for all Postgres shmem */
{
	Index		nextID;		/* PID of creating process */
	MemoryContext mctx;
	Size		totalsize;		/* total size of segment */
	Size		freesize;		/* offset to first free space */
	List   		*chunks;
	List		*freeList;

} JCacheMemHeader;



extern void JC_InitCache(void);
extern void JC_EndCache(void);
extern List *JC_GetChunks(void);
extern void JC_AddChunkedSubPlan(ChunkedSubPlan *subplan);
extern List *JC_GetChunkedSubPlans( RelChunk *chunk);
extern void JC_dropChunk( RelChunk *chunk);
extern void  JC_InitChunkMemoryContext(RelChunk *chunk, RelChunk * toDrop);
void JC_AddChunkMemoryContext(MemoryContext mcxt);
extern RelationChunksDesc  JC_InitRelationChunks(uint32 size, Relation relation);
extern MinimalTuple JC_StoreMinmalTuple(RelChunk *chunk , MinimalTuple mtuple);

extern void JC_InitChunk(uint32 size, Relation relation, Index index);
extern void JC_ResetChunk(Relation relation, Index index);
extern RelChunk * JC_processNextChunk(void);
extern void make_random_seq(RelOptInfo ** rel_array, int size) ;
extern void JC_DeleteChunk(RelChunk *chunk);
#endif /* JOINCACHE_H_ */
