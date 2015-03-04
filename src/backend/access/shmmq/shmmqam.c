/*-------------------------------------------------------------------------
 *
 * shmmqam.c
 *	  shared memory queue access method code
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/shmmq/shmmqam.c
 *
 *
 * INTERFACE ROUTINES
 *		shm_getnext	- retrieve next tuple in queue
 *
 * NOTES
 *	  This file contains the shmmq_ routines which implement
 *	  the POSTGRES shared memory access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/shmmqam.h"
//#include "access/tupdesc.h"
#include "fmgr.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/lsyscache.h"



/*
 * ExecInitWorkerResult
 *
 * Initializes the result state to retrieve tuples from worker backends. 
 */
worker_result
ExecInitWorkerResult(void)
{
	worker_result	workerResult;

	workerResult = palloc0(sizeof(worker_result_state));

	return workerResult;
}

/*
 * shm_getnext
 *
 *	Get the next tuple from shared memory queue.  This function
 *	is reponsible for fetching tuples from all the queues associated
 *	with worker backends used in parallel sequential scan.
 */
HeapTuple
shm_getnext(HeapScanDesc scanDesc, worker_result resultState,
			TupleQueueFunnel *funnel, ScanDirection direction,
			bool *fromheap)
{
	HeapTuple	tup;

	while (!resultState->all_workers_done || !resultState->local_scan_done)
	{
		if (!resultState->all_workers_done)
		{
			/* wait only if local scan is done */
			tup = TupleQueueFunnelNext(funnel, !resultState->local_scan_done,
									   &resultState->all_workers_done);
			if (HeapTupleIsValid(tup))
			{
				*fromheap = false;
				return tup;
			}
		}
		if (!resultState->local_scan_done)
		{
			tup = heap_getnext(scanDesc, direction);
			if (HeapTupleIsValid(tup))
			{
				*fromheap = true;
				return tup;
			}
			resultState->local_scan_done = true;
		}
	}

	return NULL;
}
