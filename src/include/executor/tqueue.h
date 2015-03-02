/*-------------------------------------------------------------------------
 *
 * tqueue.h
 *	  Use shm_mq to send & receive tuples between parallel backends
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/tqueue.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TQUEUE_H
#define TQUEUE_H

#include "storage/shm_mq.h"
#include "tcop/dest.h"

/* Use this to send tuples to a shm_mq. */
extern DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *);

/* Use these to receive tuples from a shm_mq. */
typedef struct TupleQueueFunnel TupleQueueFunnel;
extern TupleQueueFunnel *CreateTupleQueueFunnel(void);
extern void RegisterTupleQueueOnFunnel(TupleQueueFunnel *, shm_mq_handle *);
extern HeapTuple TupleQueueFunnelNext(TupleQueueFunnel *, bool nowait,
					 bool *done);

#endif   /* TQUEUE_H */
