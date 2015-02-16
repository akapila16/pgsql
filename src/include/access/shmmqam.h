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
#include "libpq/pqmq.h"


/* Private state maintained across calls to shm_getnext. */
typedef struct worker_result_state
{
	FmgrInfo   *receive_functions;
	Oid		   *typioparams;
	HeapTuple  tuple;
	int		   num_shm_queues;
	bool	   *queue_detached;
	bool	   all_queues_detached;
	bool	   all_heap_fetched;
} worker_result_state;

typedef struct worker_result_state *worker_result;

typedef struct ShmScanDescData *ShmScanDesc;

extern worker_result ExecInitWorkerResult(TupleDesc tupdesc, int nWorkers);
extern ShmScanDesc shm_beginscan(int num_queues);
extern HeapTuple shm_getnext(HeapScanDesc scanDesc, ShmScanDesc shmScan,
							 worker_result resultState, shm_mq_handle **responseq,
							 TupleDesc tupdesc, ScanDirection direction, bool *fromheap);

#endif   /* SHMMQAM_H */
