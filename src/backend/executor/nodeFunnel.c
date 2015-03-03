/*-------------------------------------------------------------------------
 *
 * nodeFunnel.c
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
 *		ExecFunnel				scans a relation.
 *		FunnelNext				retrieve next tuple from either heap or shared memory segment.
 *		ExecInitFunnel			creates and initializes a parallel seqscan node.
 *		ExecEndFunnel			releases any storage allocated.
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/shmmqam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeFunnel.h"
#include "postmaster/backendworker.h"
#include "utils/rel.h"



/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		FunnelNext
 *
 *		This is a workhorse for ExecFunnel
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
FunnelNext(FunnelState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;
	bool			fromheap = true;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	/*
	 * get the next tuple from the table based on result tuple descriptor.
	 */
	tuple = shm_getnext(scandesc,
						node->pss_workerResult,
						node->funnel,
						direction,
						&fromheap);

	slot->tts_fromheap = fromheap;

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass '!fromheap'
	 * because tuples returned by shm_getnext() are either pointers that are
	 * created with palloc() or are pointers onto disk pages and so it should
	 * be pfree()'d accordingly.  Note also that ExecStoreTuple will increment
	 * the refcount of the buffer; the refcount will not be dropped until the
	 * tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreTuple(tuple,	/* tuple to store */
					   slot,	/* slot to store in */
					   fromheap ? scandesc->rs_cbuf : InvalidBuffer, /* buffer associated with this
																	  * tuple */
					   !fromheap);	/* pfree this pointer if not from heap */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * FunnelRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
FunnelRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, Funnel never use keys in
	 * heap_beginscan (and this is very bad) - so, here
	 * we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		InitFunnelRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitFunnelRelation(FunnelState *node, EState *estate, int eflags)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;
	ParallelHeapScanDesc pscan;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
										   ((SeqScan *) node->ss.ps.plan)->scanrelid,
										   eflags);

	/* Initialize the workers required to perform parallel scan. */
	InitializeParallelWorkers(node->ss.ps.plan->lefttree,
								estate,
								currentRelation,
								&node->inst_options_space,
								&node->responseq,
								&node->pcxt,
								&pscan,
								((Funnel *)(node->ss.ps.plan))->num_workers);

	currentScanDesc = heap_beginscan_parallel(currentRelation, pscan);

	node->ss.ss_currentRelation = currentRelation;
	node->ss.ss_currentScanDesc = currentScanDesc;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(&node->ss, RelationGetDescr(currentRelation));
}

/* ----------------------------------------------------------------
 *		ExecInitFunnel
 * ----------------------------------------------------------------
 */
FunnelState *
ExecInitFunnel(Funnel *node, EState *estate, int eflags)
{
	FunnelState *funnelstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	funnelstate = makeNode(FunnelState);
	funnelstate->ss.ps.plan = (Plan *) node;
	funnelstate->ss.ps.state = estate;
	funnelstate->fs_workersReady = false;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &funnelstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	funnelstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) funnelstate);
	funnelstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) funnelstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &funnelstate->ss.ps);
	ExecInitScanTupleSlot(estate, &funnelstate->ss);

	InitFunnelRelation(funnelstate, estate, eflags);

	funnelstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&funnelstate->ss.ps);
	ExecAssignScanProjectionInfo(&funnelstate->ss);

	funnelstate->pss_workerResult = ExecInitWorkerResult();

	return funnelstate;
}

/* ----------------------------------------------------------------
 *		ExecFunnel(node)
 *
 *		Scans the relation via multiple workers and returns
 *		the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecFunnel(FunnelState *node)
{
	int			i;

	/*
	 * if parallel context is set and workers are not
	 * registered, register them now.
	 */
	if (node->pcxt && !node->fs_workersReady)
	{
		/* Register backend workers. */
		LaunchParallelWorkers(node->pcxt);

		node->funnel = CreateTupleQueueFunnel();

		for (i = 0; i < node->pcxt->nworkers; ++i)
		{
			 shm_mq_set_handle((node->responseq)[i], node->pcxt->worker[i].bgwhandle);
			 RegisterTupleQueueOnFunnel(node->funnel, (node->responseq)[i]);
		}

		node->fs_workersReady = true;
	}

	return ExecScan((ScanState *) &node->ss,
					(ExecScanAccessMtd) FunnelNext,
					(ExecScanRecheckMtd) FunnelRecheck);
}

/* ----------------------------------------------------------------
 *		ExecEndFunnel
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndFunnel(FunnelState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);

	if (node->pcxt)
	{
		/* destroy the tuple queue */
		DestroyTupleQueueFunnel(node->funnel);

		/* destroy parallel context. */
		DestroyParallelContext(node->pcxt);

		ExitParallelMode();
	}
}
