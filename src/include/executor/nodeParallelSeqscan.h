/*-------------------------------------------------------------------------
 *
 * nodeparallelSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeParallelSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEPARALLELSEQSCAN_H
#define NODEPARALLELSEQSCAN_H

#include "nodes/execnodes.h"

extern ParallelSeqScanState *ExecInitParallelSeqScan(ParallelSeqScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecParallelSeqScan(ParallelSeqScanState *node);
extern void ExecEndParallelSeqScan(ParallelSeqScanState *node);

extern Size EstimateScanRelationIdSpace(Oid relId);
extern void SerializeScanRelationId(Oid relId, Size maxsize,
									char *start_address);
extern void RestoreScanRelationId(Oid *relId, char *start_address);

extern Size EstimateTargetListSpace(List *targetList);
extern void SerializeTargetList(List *targetList, Size maxsize,
								char *start_address);
extern void RestoreTargetList(List **targetList, char *start_address);

#endif   /* NODEPARALLELSEQSCAN_H */
