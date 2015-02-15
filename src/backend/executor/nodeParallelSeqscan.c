/*-------------------------------------------------------------------------
 *
 * nodeParallelSeqscan.c
 *	  Support routines for parallel sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeParallelSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecParallelSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitParallelSeqScan			creates and initializes a parallel seqscan node.
 *		ExecEndParallelSeqScan			releases any storage allocated.
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/shmmqam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeParallelSeqscan.h"
#include "postmaster/backendworker.h"
#include "utils/rel.h"



/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ParallelSeqNext
 *
 *		This is a workhorse for ExecParallelSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ParallelSeqNext(ParallelSeqScanState *node)
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

	if(((ParallelSeqScan*)node->ss.ps.plan)->shm_toc_key)
	{
		/*
		 * get the next tuple from the table
		 */
		tuple = heap_getnext(scandesc, direction);
	}
	else
	{
		/*while (1)
		{
			pg_usleep(10000);
		}*/
		/*
		 * get the next tuple from the table based on result tuple descriptor.
		 */
		tuple = shm_getnext(scandesc, node->pss_currentShmScanDesc,
							node->pss_workerResult,
							node->responseq,
							node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
							direction, &fromheap);
	}

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
 * ParallelSeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
ParallelSeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, ParallelSeqScan never use keys in
	 * shm_beginscan/heap_beginscan (and this is very bad) - so, here
	 * we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		InitParallelScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitParallelScanRelation(ParallelSeqScanState *node, EState *estate, int eflags)
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

	/*
	 * For Explain statement, we don't want to initialize workers as
	 * those are maily needed to execute the plan, however scan descriptor
	 * still needs to be initialized for the purpose of InitNode functionality
	 * (as EnNode functionality assumes that scan descriptor and scan relation
	 * must be initialized, probably we can change that but that will make
	 * the code EndParallelSeqScan look different than other node's end
	 * functionality.
	 *
	 * XXX - If we want executorstart to initilize workers as well, then we
	 * need to have a provision for waiting till all the workers get started
	 * otherwise while doing endscan, it will try to wait for termination of
	 * workers which are not even started (and will neither get started).
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		/* initialize a heapscan */
		currentScanDesc = heap_beginscan(currentRelation,
										 estate->es_snapshot,
										 0,
										 NULL);
	}
	else
	{
		/*
		 * Parallel scan descriptor is initialized and stored in dynamic shared
		 * memory segment by master backend and parallel workers retrieve it
		 * from shared memory.
		 */
		if (((ParallelSeqScan *) node->ss.ps.plan)->shm_toc_key != 0)
		{
			Assert(!pscan);

			pscan = shm_toc_lookup(((ParallelSeqScan *) node->ss.ps.plan)->toc,
								   ((ParallelSeqScan *) node->ss.ps.plan)->shm_toc_key);
		}
		else
		{
			/* Initialize the workers required to perform parallel scan. */
			InitializeParallelWorkers(((SeqScan *) node->ss.ps.plan)->scanrelid,
									  node->ss.ps.plan->targetlist,
									  node->ss.ps.plan->qual,
									  estate,
									  currentRelation,
									  &node->inst_options_space,
									  &node->responseq,
									  &node->pcxt,
									  &pscan,
									  ((ParallelSeqScan *)(node->ss.ps.plan))->num_workers);
		}

		currentScanDesc = heap_beginscan_parallel(currentRelation, pscan);
	}

	node->ss.ss_currentRelation = currentRelation;
	node->ss.ss_currentScanDesc = currentScanDesc;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(&node->ss, RelationGetDescr(currentRelation));
}

/* ----------------------------------------------------------------
 *		InitShmScan
 *
 *		Set up to access the scan for shared memory segment.
 * ----------------------------------------------------------------
 */
static void
InitShmScan(ParallelSeqScanState *node)
{
	ShmScanDesc			 currentShmScanDesc;
	worker_result		 workerResult;

	/*
	 * Shared memory scan needs to be initialized only for
	 * master backend as worker backends scans only heap.
	 */
	if (((ParallelSeqScan *) node->ss.ps.plan)->shm_toc_key == 0)
	{
		/*
		 * Use result tuple descriptor to fetch data from shared memory queues
		 * as the worker backends would have put the data after projection.
		 * Number of queue's must be equal to number of worker backends.
		 */
		currentShmScanDesc = shm_beginscan(node->pcxt->nworkers);
		workerResult = ExecInitWorkerResult(node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
											node->pcxt->nworkers);

		node->pss_currentShmScanDesc = currentShmScanDesc;
		node->pss_workerResult	= workerResult;
	}
}

/* ----------------------------------------------------------------
 *		ExecInitParallelSeqScan
 * ----------------------------------------------------------------
 */
ParallelSeqScanState *
ExecInitParallelSeqScan(ParallelSeqScan *node, EState *estate, int eflags)
{
	ParallelSeqScanState *parallelscanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	parallelscanstate = makeNode(ParallelSeqScanState);
	parallelscanstate->ss.ps.plan = (Plan *) node;
	parallelscanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &parallelscanstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	parallelscanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) parallelscanstate);
	parallelscanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) parallelscanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &parallelscanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &parallelscanstate->ss);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here, no need to start workers.
	 */
	/*if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return parallelscanstate;*/

	InitParallelScanRelation(parallelscanstate, estate, eflags);

	parallelscanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&parallelscanstate->ss.ps);
	ExecAssignScanProjectionInfo(&parallelscanstate->ss);

	/*
	 * For Explain, we don't initialize the parallel workers, so
	 * accordingly don't need initialize the shared memory scan.
	 */
	if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		InitShmScan(parallelscanstate);

	return parallelscanstate;
}

/* ----------------------------------------------------------------
 *		ExecParallelSeqScan(node)
 *
 *		Scans the relation sequentially from multiple workers and returns
 *		the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecParallelSeqScan(ParallelSeqScanState *node)
{
	return ExecScan((ScanState *) &node->ss,
					(ExecScanAccessMtd) ParallelSeqNext,
					(ExecScanRecheckMtd) ParallelSeqRecheck);
}

/* ----------------------------------------------------------------
 *		ExecEndParallelSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndParallelSeqScan(ParallelSeqScanState *node)
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
		/* destroy parallel context. */
		DestroyParallelContext(node->pcxt);

		ExitParallelMode();
	}
}
