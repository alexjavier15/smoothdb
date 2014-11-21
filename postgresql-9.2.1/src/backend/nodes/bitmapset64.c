/*-------------------------------------------------------------------------
 *
 * bitmapset.c
 *	  PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of nonnegative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, a NULL pointer is always
 * accepted by all operations to represent the empty set.  (But beware
 * that this is not the only representation of the empty set.  Use
 * bms_is_empty() in preference to testing for NULL.)
 *
 *
 * Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/nodes/bitmapset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "nodes/bitmapset64.h"

#define WORDNUM64(x)	((x) / BITS_PER_BITMAPWORD64)
#define BITNUM64(x)	((x) % BITS_PER_BITMAPWORD64)

#define BITMAPSET_SIZE64(nwords)	\
	(offsetof(Bitmapset64, words) + (nwords) * sizeof(bitmapword64))

/*----------
 * This is a well-known cute trick for isolating the rightmost one-bit
 * in a word.  It assumes two's complement arithmetic.  Consider any
 * nonzero value, and focus attention on the rightmost one.  The value is
 * then something like
 *				xxxxxx10000
 * where x's are unspecified bits.  The two's complement negative is formed
 * by inverting all the bits and adding one.  Inversion gives
 *				yyyyyy01111
 * where each y is the inverse of the corresponding x.	Incrementing gives
 *				yyyyyy10000
 * and then ANDing with the original value gives
 *				00000010000
 * This works for all cases except original value = zero, where of course
 * we get zero.
 *----------
 */
#define RIGHTMOST_ONE64(x) ((signedbitmapword64) (x) & -((signedbitmapword64) (x)))

#define HAS_MULTIPLE_ONES64(x)	((bitmapword64) RIGHTMOST_ONE64(x) != (x))


static const uint8 rightmost_one_pos64[256] = {
	0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
};

static const uint8 number_of_ones64[256] = {
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};


/*
 * bms_make_singleton - build a bitmapset containing a single member
 */
Bitmapset64 *
bms64_make_singleton(uint64 x)
{
	Bitmapset64  *result;
	long		wordnum,
				bitnum;

	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	wordnum = WORDNUM64(x);
	bitnum = BITNUM64(x);
	result = (Bitmapset64 *) palloc0(BITMAPSET_SIZE64(wordnum + 1));
	result->nwords = wordnum + 1;
	result->words[wordnum] = ((bitmapword64) 1 << bitnum);
	return result;
}

/*
 * bms64_free - free a bitmapset
 *
 * Same as pfree except for allowing NULL input
 */
void
bms64_free(Bitmapset64 *a)
{
	if (a)
		pfree(a);
	/* renata added to be sure */
	a = NULL;
}



/*
 * bms64_is_member - is X a member of A?
 */
bool
bms64_is_member(uint64 x, const Bitmapset64 *a)
{
	long		wordnum,
				bitnum;

	/* XXX better to just return false for x<0 ? */
	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	if (a == NULL)
		return false;
	wordnum = WORDNUM64(x);
	bitnum = BITNUM64(x);
	if (wordnum >= a->nwords)
		return false;
	if ((a->words[wordnum] & ((bitmapword64) 1 << bitnum)) != 0)
		return true;
	return false;
}



/*
 * bms64_singleton_member - return the sole integer member of set
 *
 * Raises error if |a| is not 1.
 */
//uint64
//bms64_singleton_member(const Bitmapset64 *a)
//{
//	int			result = -1;
//	int			nwords;
//	int			wordnum;
//
//	if (a == NULL)
//		elog(ERROR, "bitmapset is empty");
//	nwords = a->nwords;
//	for (wordnum = 0; wordnum < nwords; wordnum++)
//	{
//		bitmapword64	w = a->words[wordnum];
//
//		if (w != 0)
//		{
//			if (result >= 0 || HAS_MULTIPLE_ONES64(w))
//				elog(ERROR, "bitmapset has multiple members");
//			result = wordnum * BITS_PER_BITMAPWORD64;
//			while ((w & 255) == 0)
//			{
//				w >>= 8;
//				result += 8;
//			}
//			result += rightmost_one_pos64[w & 255];
//		}
//	}
//	if (result < 0)
//		elog(ERROR, "bitmapset is empty");
//	return result;
//}

/*
 * bms64_num_members - count members of set
 */
int
bms64_num_members(const Bitmapset64 *a)
{
	int			result = 0;
	int			nwords;
	int			wordnum;

	if (a == NULL)
		return 0;
	nwords = a->nwords;
	for (wordnum = 0; wordnum < nwords; wordnum++)
	{
		bitmapword64	w = a->words[wordnum];

		/* we assume here that bitmapword64 is an unsigned type */
		while (w != 0)
		{
			result += number_of_ones64[w & 255];
			w >>= 8;
		}
	}
	return result;
}


/*
 * bms64_is_empty - is a set empty?
 *
 * This is even faster than bms64_membership().
 */
bool
bms64_is_empty(const Bitmapset64 *a)
{
	int			nwords;
	int			wordnum;

	if (a == NULL)
		return true;
	nwords = a->nwords;
	for (wordnum = 0; wordnum < nwords; wordnum++)
	{
		bitmapword64	w = a->words[wordnum];

		if (w != 0)
			return false;
	}
	return true;
}


/*
 * These operations all "recycle" their non-const inputs, ie, either
 * return the modified input or pfree it if it can't hold the result.
 *
 * These should generally be used in the style
 *
 *		foo = bms64_add_member(foo, x);
 */


/*
 * bms64_add_member - add a specified member to set
 *
 * Input set is modified or recycled!
 */
Bitmapset64 *
bms64_add_member(Bitmapset64 *a, uint64 x)
{
	int			wordnum,
				bitnum;

	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	if (a == NULL)
		return bms64_make_singleton(x);
	wordnum = WORDNUM64(x);
	bitnum = BITNUM64(x);
	if (wordnum >= a->nwords)
	{
		/* Slow path: make a larger set and union the input set into it */
		Bitmapset64  *result;
		int			nwords;
		int			i;

		result = bms64_make_singleton(x);
		nwords = a->nwords;
		for (i = 0; i < nwords; i++)
			result->words[i] |= a->words[i];
		pfree(a);
		return result;
	}
	/* Fast path: x fits in existing set */
	a->words[wordnum] |= ((bitmapword64) 1 << bitnum);
	return a;
}

/*
 * bms64_del_member - remove a specified member from set
 *
 * No error if x is not currently a member of set
 *
 * Input set is modified in-place!
 */
Bitmapset64 *
bms64_del_member(Bitmapset64 *a, uint64 x)
{
	int			wordnum,
				bitnum;

	if (x < 0)
		elog(ERROR, "negative bitmapset member not allowed");
	if (a == NULL)
		return NULL;
	wordnum = WORDNUM64(x);
	bitnum = BITNUM64(x);
	if (wordnum < a->nwords)
		a->words[wordnum] &= ~((bitmapword64) 1 << bitnum);
	return a;
}





