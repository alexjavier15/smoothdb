/*
 * smoothscanopaque.h
 *
 *  Created on: May 22, 2013
 *      Author: renata
 */

#ifndef SMOOTHSCANOPAQUE_H_
#define SMOOTHSCANOPAQUE_H_




//typedef struct SmoothScanPosItem	/* what we remember about each match */
//{
//	ItemPointerData heapTid;	/* TID of referenced heap item */
//	OffsetNumber tuplePageOffset;	/* index item's location within page */
//	LocationIndex tupleOffset;	/* Tuple's offset in workspace, if any */
//
//} SmoothScanPosItem;
//
//typedef struct SmoothScanPosData
//{
//	Buffer		buf;			/* if valid, the buffer is pinned */
//
//	BlockNumber nextPage;		/* page's right link when we scanned it */
//
//	/*
//	 * moreLeft and moreRight track whether we think there may be matching
//	 * index entries to the left and right of the current page, respectively.
//	 * We can clear the appropriate one of these flags when _bt_checkkeys()
//	 * returns continuescan = false.
//	 */
//	bool		moreLeft;
//	bool		moreRight;
//
//	/*
//	 * If we are doing an index-only scan, nextTupleOffset is the first free
//	 * location in the associated tuple storage workspace.
//	 */
//	int			nextTupleOffset;
//
//	/*
//	 * The items array is always ordered in index order (ie, increasing
//	 * indexoffset).  When scanning backwards it is convenient to fill the
//	 * array back-to-front, so we start at the last slot and fill downwards.
//	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
//	 * itemIndex is a cursor showing which entry was last returned to caller.
//	 */
//	int			firstHeapItem;		/* first valid index in items[] */
//	int			lastHeapItem;		/* last valid index in items[] */
//	int			itemHeapIndex;		/* current index in items[] */
//
//	SmoothScanPosItem items[MaxIndexTuplesPerPage]; /* MUST BE LAST */
//} SmoothScanPosData;
//
//typedef SmoothScanPosData *SmoothScanPos;
//
//
//
//typedef struct SmoothScanOpaqueData
//{
//	/* these fields are set by _bt_preprocess_keys(): */
//	bool		qual_ok;		/* false if qual can never be satisfied */
//	int			numberOfKeys;	/* number of preprocessed scan keys */
//	ScanKey		keyData;		/* array of preprocessed scan keys */
//
//	/* workspace for SK_SEARCHARRAY support */
//	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */
//	int			numArrayKeys;	/* number of equality-type array keys (-1 if
//								 * there are any unsatisfiable array keys) */
//	BTArrayKeyInfo *arrayKeys;	/* info about each equality-type array key */
//	MemoryContext arrayContext; /* scan-lifespan context for array data */
//
//	/* info about killed items if any (killedItems is NULL if never used) */
//	int		   *killedItems;	/* currPos.items indexes of killed items */
//	int			numKilled;		/* number of currently stored items */
//
//	/*
//	 * If we are doing an index-only scan, these are the tuple storage
//	 * workspaces for the currPos and markPos respectively.  Each is of size
//	 * BLCKSZ, so it can hold as much as a full page's worth of tuples.
//	 */
//	char	   *currTuples;		/* tuple storage for currPos */
//	char	   *markTuples;		/* tuple storage for markPos */
//
//	/*
//	 * If the marked position is on the same page as current position, we
//	 * don't use markPos, but just keep the marked itemIndex in markItemIndex
//	 * (all the rest of currPos is valid for the mark position). Hence, to
//	 * determine if there is a mark, first look at markItemIndex, then at
//	 * markPos.
//	 */
//	int			markItemIndex;	/* itemIndex, or -1 if not valid */
//
//	bool 		more_data_for_smooth; /* do we have more items to return before going to the next page */
//
//	/*information for pre-fetcher*/
//	int			prefetch_pages; 	/* how many pages in advance am I AT THE MOMENT */
//	int			prefetch_target;	/* my current GOAL is to this many pages prefetched
//	 	 	 	 	 	 	 	 	   This number will increased - for instance 2, 4, 8, 16 etc. */
//	int 		prefetch_cumul; 	/* this is just for testing purpose */
//	BlockNumber nextPageId;			/*next page to process with smooth - this page is supposed to be already PREFETCHED*/
//
//	BlockNumber rel_nblocks;		/* number of block that relation has*/
//	bool 		orderby;		    /* should the order be respected */
//
//	/* keep information about all pages you visited with smooth scan */
//	/* option with static array */
//	//PageBitmap vispages[MAXPAGES];	/* their offsets */
//
//	Bitmapset  *bs_vispages;		/* keep track of all visited pages  */
//	//Bitmapset  *bs_tovispages;		/* keep track of all pages to visit */
//	/* keep these last in struct for efficiency */
//	SmoothScanPosData currPos;		/* current position data */
//	SmoothScanPosData markPos;		/* marked position, if any */
//	ResultCache  *result_cache;		/* when we have order imposed we stored tuples in a hash table*/
//
//} SmoothScanOpaqueData;
//
//typedef SmoothScanOpaqueData *SmoothScanOpaque;
//
//#define SmoothScanPosIsValid(scanpos) BufferIsValid((scanpos).buf)
//
///* Save an index item into so->currPos.items[itemIndex] */
//extern void
//_bt_saveheapitem(SmoothScanOpaque ss, int itemIndex,
//			 OffsetNumber offnum, HeapTuple htup);


#endif /* SMOOTHSCANOPAQUE_H_ */
