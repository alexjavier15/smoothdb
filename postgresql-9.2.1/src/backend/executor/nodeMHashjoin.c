/*
 * nodeMHashJoin.c
 *
 *  Created on: 5 mars 2015
 *      Author: alex

 *	  Routines to handle Mhash join nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMHashjoin.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeMHashjoin.h"
#include "miscadmin.h"
#include "utils/memutils.h"

