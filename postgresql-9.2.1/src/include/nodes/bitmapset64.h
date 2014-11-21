/*-------------------------------------------------------------------------
 *
 * bitmapset.h
 *	  PostgreSQL generic bitmap set package
 *
 *
 *renata: bitmap set for 64 bytes
 * src/include/nodes/bitmapset64.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BITMAPSET64_H
#define BITMAPSET64_H

/*
 * Data representation
 */

/* The unit size can be adjusted by changing these three declarations: */
#define BITS_PER_BITMAPWORD64 64
typedef uint64 bitmapword64;		/* must be an unsigned type */
typedef int64 signedbitmapword64; /* must be the matching signed type */

/*renata change for TID */
//#define BITS_PER_BITMAPWORD 64
//typedef uint64 bitmapword;		/* must be an unsigned type */
//typedef int64 signedbitmapword; /* must be the matching signed type */


typedef struct Bitmapset64
{
	long			nwords;			/* number of words in array */
	bitmapword64	words[1];		/* really [nwords] */
} Bitmapset64;					/* VARIABLE LENGTH STRUCT */





/*
 * function prototypes in nodes/bitmapset.c
 */

extern Bitmapset64 *bms64_make_singleton(uint64 x);
extern void bms64_free(Bitmapset64 *a);

extern bool bms64_is_member(uint64 x, const Bitmapset64 *a);
//extern uint64	bms64_singleton_member(const Bitmapset64 *a);
extern int	bms64_num_members(const Bitmapset64 *a);

/* optimized tests when we don't need to know exact membership count: */
extern bool bms64_is_empty(const Bitmapset64 *a);

/* these routines recycle (modify or free) their non-const inputs: */

extern Bitmapset64 *bms64_add_member(Bitmapset64 *a, uint64 x);
extern Bitmapset64 *bms64_del_member(Bitmapset64 *a, uint64 x);

#endif   /* BITMAPSET64_H */
