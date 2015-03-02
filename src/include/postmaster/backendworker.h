/*--------------------------------------------------------------------
 * backendworker.h
 *		POSTGRES backend workers interface
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/postmaster/backendworker.h
 *--------------------------------------------------------------------
 */
#ifndef BACKENDWORKER_H
#define BACKENDWORKER_H

/*---------------------------------------------------------------------
 * External module API.
 *---------------------------------------------------------------------
 */

#include "libpq/pqmq.h"

/* Table-of-contents constants for our dynamic shared memory segment. */
/*#define PARALLEL_KEY_SCANRELID		0
#define PARALLEL_KEY_TARGETLIST		1
#define PARALLEL_KEY_QUAL			2
#define	PARALLEL_KEY_RANGETBL		3*/
#define	PARALLEL_KEY_PLANNEDSTMT	0
#define	PARALLEL_KEY_PARAMS			1
#define PARALLEL_KEY_INST_OPTIONS	2
#define PARALLEL_KEY_INST_INFO		3
#define PARALLEL_KEY_TUPLE_QUEUE	4
#define PARALLEL_KEY_SCAN			5
//#define PARALLEL_KEY_OPERATION		6

extern int	parallel_seqscan_degree;

extern void InitializeParallelWorkers(Plan *plan, EState *estate,
									  Relation rel, char **inst_options_space,
									  shm_mq_handle ***responseqp,
									  ParallelContext **pcxtp,
									  ParallelHeapScanDesc *pscan,
									  int nWorkers);

#endif   /* BACKENDWORKER_H */
