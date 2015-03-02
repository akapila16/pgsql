/*-------------------------------------------------------------------------
 *
 * nodePartialSeqscan.c
 *	  Support routines for parallel sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFunnel.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecPartialSeqScan				scans a relation.
 *		PartialSeqNext					retrieve next tuple from either heap.
 *		ExecInitPartialSeqScan			creates and initializes a partial seqscan node.
 *		ExecEndPartialSeqScan			releases any storage allocated.
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/shmmqam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodePartialSeqscan.h"
#include "postmaster/backendworker.h"
#include "utils/rel.h"



/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		PartialSeqNext
 *
 *		This is a workhorse for ExecPartialSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
PartialSeqNext(PartialSeqScanState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss_currentScanDesc;
	estate = node->ps.state;
	direction = estate->es_direction;
	slot = node->ss_ScanTupleSlot;

	/*
	 * get the next tuple from the table
	 */
	tuple = heap_getnext(scandesc, direction);

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreTuple(tuple,	/* tuple to store */
					   slot,	/* slot to store in */
					   scandesc->rs_cbuf,		/* buffer associated with this
												 * tuple */
					   false);	/* don't pfree this pointer */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * PartialSeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
PartialSeqRecheck(PartialSeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, PartialSeqScan never use keys in
	 * shm_beginscan/heap_beginscan (and this is very bad) - so, here
	 * we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		InitPartialScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitPartialScanRelation(PartialSeqScanState *node, EState *estate, int eflags)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;
	ParallelHeapScanDesc pscan;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
										   ((Scan *) node->ps.plan)->scanrelid,
										   eflags);

	/*
	 * Parallel scan descriptor is initialized and stored in dynamic shared
	 * memory segment by master backend and parallel workers retrieve it
	 * from shared memory.
	 */
	Assert(estate->toc);
	
	pscan = shm_toc_lookup(estate->toc, PARALLEL_KEY_SCAN);

	currentScanDesc = heap_beginscan_parallel(currentRelation, pscan);

	node->ss_currentRelation = currentRelation;
	node->ss_currentScanDesc = currentScanDesc;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(node, RelationGetDescr(currentRelation));
}

/* ----------------------------------------------------------------
 *		ExecInitPartialSeqScan
 * ----------------------------------------------------------------
 */
PartialSeqScanState *
ExecInitPartialSeqScan(PartialSeqScan *node, EState *estate, int eflags)
{
	PartialSeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(PartialSeqScanState);
	scanstate->ps.plan = (Plan *) node;
	scanstate->ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ps);
	ExecInitScanTupleSlot(estate, scanstate);

	/*
	 * initialize scan relation
	 */
	InitPartialScanRelation(scanstate, estate, eflags);

	scanstate->ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ps);
	ExecAssignScanProjectionInfo(scanstate);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecPartialSeqScan(node)
 *
 *		Scans the relation via multiple workers and returns
 *		the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecPartialSeqScan(PartialSeqScanState *node)
{
	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) PartialSeqNext,
					(ExecScanRecheckMtd) PartialSeqRecheck);
}

/* ----------------------------------------------------------------
 *		ExecEndPartialSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndPartialSeqScan(PartialSeqScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss_currentRelation;
	scanDesc = node->ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}
