/*
 * smoothcachemgr.h
 *
 *  Created on: 2 févr. 2015
 *      Author: alex
 */

#ifndef SMOOTHCACHEMGR_H_
#define SMOOTHCACHEMGR_H_

typedef struct SmoothCacheMgr{

	int max_size;
	int available_mem;
	struct SmoothRelationCache **rel_cache_array;


}SmoothCacheMgr;
typedef struct SmoothRelationCache{

	int relation_size;
	int available_mem;
	int alloc_mem;
	int tup_size;
	Relation relation;
	ResultCache *rel_cache;


}SmoothRelationCache;
extern void InitRelationCache(Index relid );




#endif /* SMOOTHCACHEMGR_H_ */
