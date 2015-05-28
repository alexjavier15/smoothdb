/*
 * joincache.c
 *
 *  Created on: 7 mai 2015
 *      Author: alex
 */

#include "postgres.h"
#include "smooth/joincache.h"
#include "storage/pg_shmem.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

#include "miscadmin.h"
#include "optimizer/cost.h"
#include "utils/rel.h"
#include "lib/stringinfo.h"
#include "utils/dynahash.h"

#define  MAX_NUM_RELATIONS 16

#define  CHUNK_PREFIX	"JC_"

#define get_chuck_name(name, str)
#define ChunkGetHashKey(chunk)  ((uint32) (chunk->ch_id & 0xFFFF | chunk->relid << 16 ));

List * make_random_list(int max_relid);
MemoryContext JC_GetChunkMemoryContext(void);

 // chunks of cache for a relation */
static JCacheMemHeader *JCacheSegHdr;

static HTAB * RelationChunksIndex = NULL;
//static List *JChunkQueue =  NIL;
int  chunk_size;


static List * seq_cycle;
static ListCell *nextChunk;




void JC_InitCache(void){
	int hash_tag = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;
	HASHCTL * hctl = NULL;
	StringInfoData str;
	MemoryContext mcxt;
	MemoryContext parent_mcxt;
	MemoryContext oldcxt;
	double num_chunks;
	int i;

	parent_mcxt = 	AllocSetContextCreate(TopMemoryContext,
						"Multi Join Cache",
						ALLOCSET_SMALL_MINSIZE,
						ALLOCSET_SMALL_INITSIZE,
						ALLOCSET_SMALL_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(parent_mcxt);
	hctl= (HASHCTL*)palloc0(sizeof(HASHCTL));

	JCacheSegHdr = palloc(sizeof(JCacheMemHeader));


	hctl->keysize = sizeof(JoinCacheKey);
	hctl->entrysize = JCACHE_ENTRY_SIZE;
	hctl->hash = tag_hash;
	hctl->hcxt = parent_mcxt;
	JCacheSegHdr->chunks = NIL;


	RelationChunksIndex = hash_create("JoinCache Hash",
										32,
										hctl,hash_tag);
	chunk_size = multi_join_chunk_size * 1024L;

	JCacheSegHdr->mctx = parent_mcxt;
	JCacheSegHdr->nextID = 0;
	JCacheSegHdr->freesize = multi_join_cache_size;
	//Compue num of chunks;
	num_chunks =  floor((double)multi_join_cache_size / multi_join_chunk_size);
	JCacheSegHdr->totalsize  = num_chunks * chunk_size;
	JCacheSegHdr->freesize = JCacheSegHdr->totalsize;
	JCacheSegHdr->freeList = NIL;
	// create chunk contexts
	initStringInfo(&str);
	appendStringInfoString(&str, CHUNK_PREFIX);
	printf("NUM OF ALLOCATED CHUNKS : %.0f , Free mem: %ld \n", num_chunks, JCacheSegHdr->freesize);
	for (i = 0; i < num_chunks; i++) {
		appendStringInfo(&str, "%d", i);
		mcxt = AllocSetContextCreate(parent_mcxt,
				str.data,
				ALLOCSET_SMALL_MINSIZE,
				ALLOCSET_SMALL_INITSIZE,
				ALLOCSET_SMALL_MAXSIZE);

		JC_AddChunkMemoryContext(mcxt);
		resetStringInfo(&str);

	}
	JCacheSegHdr->nextID=i;

	pfree(str.data);
	MemoryContextSwitchTo(oldcxt);


}


void JC_AddChunkedSubPlan(ChunkedSubPlan *subplan){

	ListCell *lc;

//	bool found;
//	JoinCacheEntry *jcentry;
	foreach(lc, subplan->chunks) {
		RelChunk * relchunk = lfirst(lc);
//		jcentry = (JoinCacheEntry *)hash_search(RelationChunksIndex,&relchunk->chunkID,HASH_ENTER,&found);
//		if (jcentry != NULL) {
//			memcpy(jcentry, &(relchunk->chunkID), sizeof(ChunkID));
//			if (found) {
//
//				relchunk->subplans = lappend(jcentry->subplans, subplan);
//
//			} else {
//				jcentry->subplans = lappend(NIL, subplan);
//				jcentry->relChunk = relchunk;
//
//			}
//			printf("%d.[%d] ", ChunkGetRelid(relchunk), ChunkGetID(relchunk));
//
//		}
//
		relchunk->subplans = lappend(relchunk->subplans, subplan);
		relchunk->state =CH_WAITTING;
	}



}


RelChunk * JC_processNextChunk(void){

//	JoinCacheEntry *jcentry;
	RelChunk *result = (RelChunk *) lfirst(nextChunk);
	while(list_length( result->subplans) == 0 || result->state == CH_READ){

		nextChunk = nextChunk->next;
		result = (RelChunk *) lfirst(nextChunk);

	}


	printf("RECEIVING CHUNK: \n");
	printf("rel : %d chunk : %d\n", ChunkGetRelid(result),ChunkGetID(result));
	fflush(stdout);
	JCacheSegHdr->chunks = lappend(JCacheSegHdr->chunks, result);
	nextChunk = nextChunk->next;
	return result;


}




void JC_dropChunk( RelChunk *chunk){

	printf("Dropping chunk : \n");
    printf("rel : %d chunk : %d , state : %d\n", ChunkGetRelid(chunk),ChunkGetID(chunk), chunk->state);
    list_free(chunk->tuple_list);

	MemoryContextReset(chunk->mcxt);
	chunk->tuple_list = NIL;
	chunk->state = CH_DROPPED;
	JCacheSegHdr->chunks = list_delete(JCacheSegHdr->chunks, chunk);
	JC_AddChunkMemoryContext(chunk->mcxt);
}

MemoryContext JC_GetChunkMemoryContext(void){

	/* use volatile pointer to prevent code rearrangement */
	volatile JCacheMemHeader *jcacheSegHdr = JCacheSegHdr;
	MemoryContext result;
	if (list_length(jcacheSegHdr->freeList) == 0) {
		return NULL;
	}
	result = linitial(jcacheSegHdr->freeList);
	jcacheSegHdr->freeList =list_delete_first(jcacheSegHdr->freeList);
	return result;

}

void JC_InitChunkMemoryContext(RelChunk *chunk, RelChunk * toDrop) {

	MemoryContext mcxt = JC_GetChunkMemoryContext();

//	volatile JCacheMemHeader *jcacheSegHdr = JCacheSegHdr;

	if(mcxt == NULL){

		JC_dropChunk(toDrop);
		mcxt = JC_GetChunkMemoryContext();
	}

	chunk->mcxt = mcxt;
}

MinimalTuple JC_StoreMinmalTuple(RelChunk *chunk , MinimalTuple mtuple){
	MemoryContext oldcxt;
	MinimalTuple copyTuple;

	volatile JCacheMemHeader *jcacheSegHdr = JCacheSegHdr;

	oldcxt = MemoryContextSwitchTo(chunk->mcxt);

	copyTuple = (MinimalTuple ) palloc0(mtuple->t_len);
	memcpy(copyTuple, mtuple, mtuple->t_len);
	chunk->tuple_list = lappend(chunk->tuple_list, copyTuple);

	MemoryContextSwitchTo(oldcxt);
	jcacheSegHdr->freesize = jcacheSegHdr->freesize - mtuple->t_len;
	return copyTuple;

}
void JC_AddChunkMemoryContext(MemoryContext mcxt) {

	/* use volatile pointer to prevent code rearrangement */
	volatile JCacheMemHeader *jcacheSegHdr = JCacheSegHdr;

	jcacheSegHdr->freeList = lappend(jcacheSegHdr->freeList, mcxt);

}


/* Join Cache Simulator */



List * make_random_list(int max_relid) {

	Bitmapset * relids = NULL;
	List *result = NIL;
	int size = max_relid - 1;
	int nextid;

	while (bms_num_members(relids) != size) {

		nextid = (rand() % max_relid);
		if (nextid != 0 && !bms_is_member(nextid,relids)) {
			relids = bms_add_member(relids, nextid);
			result = lappend_int(result, nextid);
		}

	}
	return result;

}

void make_random_seq(RelOptInfo ** rel_array, int size) {

	Bitmapset * allChunks = NULL;
	List *result = NIL;
	List  *relSeq = NIL;
	int  total_chunks = 0;
	int i;

	for (i = 1 ; i < size; i++){

		total_chunks +=  list_length(rel_array[i]->chunks);

	}

	relSeq = make_random_list(size);

	pprint(relSeq);
	while (bms_num_members(allChunks) != total_chunks) {
		ListCell *lc;

		foreach(lc, relSeq) {

			uint32 relid = lfirst_int(lc);
			int nextChunk = 0;
			uint32 chunkid;
			RelOptInfo * rel = rel_array[relid];
			nextChunk = rand() % list_length(rel->chunks);
		//	printf( " chunk for rel %d = %d \n", relid,nextChunk);
			chunkid = (uint32) (relid << 16) | nextChunk;
			if ( chunkid != 0 && !bms_is_member(chunkid, allChunks)) {
				RelChunk *chunk = (RelChunk * )list_nth(rel->chunks, (int)nextChunk);

				allChunks =	bms_add_member(allChunks, chunkid);
			//	printf(" rel : %d, id : %d \n", ChunkGetRelid(chunk), ChunkGetID(chunk));

				result = lappend(result, chunk);
			}

		}

	}
//	ListCell *chk;
//	printf("Ordering seq is : \n");
//	foreach(chk,seq_cycle) {
//
//		RelChunk *chunk = (RelChunk *) lfirst(chk);
//
//		printf(" rel : %d, id : %d \n", ChunkGetRelid(chunk), ChunkGetID(chunk));
//
//	}
	/* Make a circular list*/

	{
		ListCell * last = list_tail(result);
		last->next = list_head(result);

		seq_cycle = result;
	}
	nextChunk = list_head(result);




}

void JC_EndCache(void){


	MemoryContextDelete(JCacheSegHdr->mctx);



}

List *JC_GetChunks(void){
	return JCacheSegHdr->chunks;

}


