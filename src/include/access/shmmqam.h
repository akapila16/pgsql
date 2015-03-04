/*-------------------------------------------------------------------------
 *
 * shmmqam.h
 *	  POSTGRES shared memory queue access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/shmmqam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHMMQAM_H
#define SHMMQAM_H

#include "access/relscan.h"
#include "executor/tqueue.h"
#include "libpq/pqmq.h"


/* Private state maintained across calls to shm_getnext. */
typedef struct worker_result_state
{
	bool		all_workers_done;
	bool		local_scan_done;
} worker_result_state;

typedef struct worker_result_state *worker_result;

extern worker_result ExecInitWorkerResult(void);
extern HeapTuple shm_getnext(HeapScanDesc scanDesc, worker_result resultState,
							 TupleQueueFunnel *funnel, ScanDirection direction,
							 bool *fromheap);

#endif   /* SHMMQAM_H */
