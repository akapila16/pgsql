/*-------------------------------------------------------------------------
 *
 * nodefunnel.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeFunnel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEFUNNEL_H
#define NODEFUNNEL_H

#include "nodes/execnodes.h"

extern FunnelState *ExecInitFunnel(Funnel *node, EState *estate, int eflags);
extern TupleTableSlot *ExecFunnel(FunnelState *node);
extern void ExecEndFunnel(FunnelState *node);


#endif   /* NODEFUNNEL_H */
