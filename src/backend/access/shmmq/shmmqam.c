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
#include "access/tupdesc.h"
#include "fmgr.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/lsyscache.h"


static bool
HandleParallelTupleMessage(worker_result resultState, TupleDesc tupdesc,
						   StringInfo msg, int queueId);
static HeapTuple
form_result_tuple(worker_result resultState, TupleDesc tupdesc,
				  StringInfo msg, int queueId);

/*
 * shm_beginscan
 *
 * Initializes the shared memory scan descriptor to retrieve tuples
 * from worker backends. 
 */
ShmScanDesc
shm_beginscan(int num_queues)
{
	ShmScanDesc		shmscan;

	shmscan = palloc(sizeof(ShmScanDescData));

	shmscan->num_shm_queues = num_queues;
	shmscan->ss_cqueue = -1;
	shmscan->shmscan_inited	= false;

	return shmscan;
}

/*
 * ExecInitWorkerResult
 *
 * Initializes the result state to retrieve tuples from worker backends. 
 */
worker_result
ExecInitWorkerResult(TupleDesc tupdesc, int nWorkers)
{
	worker_result	workerResult;
	int				i;
	int	natts = tupdesc->natts;

	workerResult = palloc0(sizeof(worker_result_state));
	workerResult->receive_functions = palloc(sizeof(FmgrInfo) * natts);
	workerResult->typioparams = palloc(sizeof(Oid) * natts);
	workerResult->num_shm_queues = nWorkers;
	workerResult->queue_detached = palloc0(sizeof(bool) * nWorkers);

	for (i = 0;	i < natts; ++i)
	{
		Oid	receive_function_id;

		getTypeBinaryInputInfo(tupdesc->attrs[i]->atttypid,
							   &receive_function_id,
							   &workerResult->typioparams[i]);
		fmgr_info(receive_function_id, &workerResult->receive_functions[i]);
	}

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
shm_getnext(HeapScanDesc scanDesc, ShmScanDesc shmScan,
			worker_result resultState, shm_mq_handle **responseq,
			TupleDesc tupdesc, ScanDirection direction, bool *fromheap)
{
	shm_mq_result	res;
	Size			nbytes;
	void			*data;
	StringInfoData	msg;
	int				queueId = 0;

	/*
	 * calculate next starting queue used for fetching tuples
	 */
	if(!shmScan->shmscan_inited)
	{
		shmScan->shmscan_inited = true;
		Assert(shmScan->num_shm_queues > 0);
		queueId = 0;
	}
	else
		queueId = shmScan->ss_cqueue;

	/* Read and processes messages from the shared memory queues. */
	for(;;)
	{
		if (!resultState->all_queues_detached)
		{
			if (queueId == shmScan->num_shm_queues)
				queueId = 0;

			/*
			 * Don't fetch from detached queue.  This loop could continue
			 * forever, if we reach a situation such that all queue's are
			 * detached, however we won't reach here if that is the case.
			 */
			while (resultState->queue_detached[queueId])
			{
				++queueId;
				if (queueId == shmScan->num_shm_queues)
					queueId = 0;
			}

			for (;;)
			{
				/*
				 * mark current queue used for fetching tuples, this is used
				 * to fetch consecutive tuples from queue used in previous
				 * fetch.
				 */
				shmScan->ss_cqueue = queueId;

				/* Get next message. */
				res = shm_mq_receive(responseq[queueId], &nbytes, &data, true);
				if (res == SHM_MQ_DETACHED)
				{
					/*
					 * mark the queue that got detached, so that we don't
					 * try to fetch from it again.
					 */
					resultState->queue_detached[queueId] = true;
					--resultState->num_shm_queues;
					/*
					 * if we have exhausted data from all worker queues, then don't
					 * process data from queues.
					 */
					if (resultState->num_shm_queues <= 0)
						resultState->all_queues_detached = true;
					break;
				}
				else if (res == SHM_MQ_WOULD_BLOCK)
					break;
				else if (res == SHM_MQ_SUCCESS)
				{
					bool rettuple;
					initStringInfo(&msg);
					appendBinaryStringInfo(&msg, data, nbytes);
					rettuple = HandleParallelTupleMessage(resultState, tupdesc, &msg, queueId);
					pfree(msg.data);
					if (rettuple)
					{
						*fromheap = false;
						return resultState->tuple;
					}
				}
			}
		}

		/*
		 * if we have checked all the message queue's and didn't find
		 * any message or we have already fetched all the data from queue's,
		 * then it's time to fetch directly from heap.  Reset the current
		 * queue as the first queue from which we need to receive tuples.
		 */
		if ((queueId == shmScan->num_shm_queues - 1 ||
			 resultState->all_queues_detached) &&
			 !resultState->all_heap_fetched)
		{
			HeapTuple	tuple;
			shmScan->ss_cqueue = 0;
			tuple = heap_getnext(scanDesc, direction);
			if (tuple)
			{
				*fromheap = true;
				return tuple;
			}
			else if (tuple == NULL && resultState->all_queues_detached)
				break;
			else
				resultState->all_heap_fetched = true;
		}
		else if (resultState->all_queues_detached &&
				 resultState->all_heap_fetched)
			break;

		/* check the data in next queue. */
		++queueId;
	}

	return NULL;
}

/*
 * HandleParallelTupleMessage
 *
 * Handle a single tuple related protocol message received from
 * a single parallel worker.
 */
static bool
HandleParallelTupleMessage(worker_result resultState, TupleDesc tupdesc,
						   StringInfo msg, int queueId)
{
	char	msgtype;
	bool	rettuple = false;

	msgtype = pq_getmsgbyte(msg);

	/* Dispatch on message type. */
	switch (msgtype)
	{
		case 'D':
			{
				/* Handle DataRow message. */
				resultState->tuple = form_result_tuple(resultState, tupdesc, msg, queueId);
				rettuple = true;
				break;
			}
		case 'C':
			{
				/*
				 * Handle CommandComplete message. Ignore tags sent by
				 * worker backend as we are anyway going to use tag of
				 * master backend for sending the same to client.
				 */
				(void) pq_getmsgstring(msg);
				break;
			}
		case 'G':
		case 'H':
		case 'W':
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY protocol not allowed in worker")));
			}
		default:
			elog(WARNING, "unknown message type: %c", msg->data[0]);
			break;
	}

	return rettuple;
}

/*
 * form_result_tuple
 *
 * Parse a DataRow message and form a result tuple.
 */
static HeapTuple
form_result_tuple(worker_result resultState, TupleDesc tupdesc,
				  StringInfo msg, int queueId)
{
	/* Handle DataRow message. */
	int16	natts = pq_getmsgint(msg, 2);
	int16	i;
	Datum  *values = NULL;
	bool   *isnull = NULL;
	HeapTuple	tuple;
	StringInfoData	buf;

	if (natts != tupdesc->natts)
		elog(ERROR, "malformed DataRow");
	if (natts > 0)
	{
		values = palloc(natts * sizeof(Datum));
		isnull = palloc(natts * sizeof(bool));
	}
	initStringInfo(&buf);

	for (i = 0; i < natts; ++i)
	{
		int32	bytes = pq_getmsgint(msg, 4);

		if (bytes < 0)
		{
			values[i] = ReceiveFunctionCall(&resultState->receive_functions[i],
											NULL,
											resultState->typioparams[i],
											tupdesc->attrs[i]->atttypmod);
			isnull[i] = true;
		}
		else
		{
			resetStringInfo(&buf);
			appendBinaryStringInfo(&buf, pq_getmsgbytes(msg, bytes), bytes);
			values[i] = ReceiveFunctionCall(&resultState->receive_functions[i],
											&buf,
											resultState->typioparams[i],
											tupdesc->attrs[i]->atttypmod);
			isnull[i] = false;
		}
	}

	pq_getmsgend(msg);

	tuple = heap_form_tuple(tupdesc, values, isnull);

	/*
	 * Release locally palloc'd space.  XXX would probably be good to pfree
	 * values of pass-by-reference datums, as well.
	 */
	pfree(values);
	pfree(isnull);

	pfree(buf.data);

	return tuple;
}
