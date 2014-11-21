/*
 * resultcache.c
 *
 *  Created on: May 22, 2013
 *      Author: renata
 */
#include "smooth/resultcache.h"

//
//struct ResultCache
//{
//	NodeTag		type;			/* to make it a valid Node */
//	MemoryContext mcxt;			/* memory context containing me */
//	CacheStatus	status;			/* see codes above */
//	HTAB	   *hashtable;		/* hash table of PagetableEntry's */
//	int			nentries;		/* number of entries in hastable */
//	int			maxentries;		/* limit on same to meet maxbytes */
//	int			npages;			/* number of exact entries in pagetable */
//};

//static void
//smooth_create_resultcache(SmoothScanOpaque smoothInfo)
//{
//	HASHCTL		hash_ctl;
//
//	Assert(smoothInfo->result_cache == NULL);
//
//	/* Create the hashtable proper */
//	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
//	hash_ctl.keysize = sizeof(BlockNumber);
//	hash_ctl.entrysize = sizeof(HeapTupleData);
//	hash_ctl.hash = tag_hash;
//	hash_ctl.hcxt = smoothInfo->result_cache->mcxt;
//	smoothInfo->result_cache = hash_create("ResultCache",
//								 128,	/* start small and extend */
//								 &hash_ctl,
//								 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
//
//
//}
