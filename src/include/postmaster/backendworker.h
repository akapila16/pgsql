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

extern int	parallel_seqscan_degree;

extern void InitializeParallelWorkers(Index scanrelId, List *targetList,
									  List *qual, EState *estate,
									  Relation rel, char **inst_options_space,
									  shm_mq_handle ***responseqp,
									  ParallelContext **pcxtp,
									  ParallelHeapScanDesc *pscan,
									  int nWorkers);

#endif   /* BACKENDWORKER_H */
