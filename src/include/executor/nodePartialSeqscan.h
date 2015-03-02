/*-------------------------------------------------------------------------
 *
 * nodePartialSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodePartialSeqscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEPARTIALSEQSCAN_H
#define NODEPARTIALSEQSCAN_H

#include "nodes/execnodes.h"

extern PartialSeqScanState *ExecInitPartialSeqScan(PartialSeqScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecPartialSeqScan(PartialSeqScanState *node);
extern void ExecEndPartialSeqScan(PartialSeqScanState *node);

#endif   /* NODEPARTIALSEQSCAN_H */
