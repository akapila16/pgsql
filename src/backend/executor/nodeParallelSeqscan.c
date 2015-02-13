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

	/*
	 * get the next tuple from the table based on result tuple descriptor.
	 */
	tuple = shm_getnext(scandesc, node->pss_currentShmScanDesc,
						node->pss_workerResult,
						node->responseq,
						node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
						direction, &fromheap);

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
InitParallelScanRelation(SeqScanState *node, EState *estate, int eflags)
{
	Relation	currentRelation;
	HeapScanDesc currentScanDesc;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
									  ((SeqScan *) node->ps.plan)->scanrelid,
										   eflags);

	/* initialize a heapscan */
	currentScanDesc = heap_beginscan(currentRelation,
									 estate->es_snapshot,
									 0,
									 NULL);

	/*
	 * Each backend worker participating in parallel sequiantial
	 * scan operate on different set of blocks, so there doesn't
	 * seem to much benefit in allowing sync scans.
	 */
	heap_setsyncscan(currentScanDesc, false);

	node->ss_currentRelation = currentRelation;
	node->ss_currentScanDesc = currentScanDesc;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(node, RelationGetDescr(currentRelation));
}


/* ----------------------------------------------------------------
 *		ExecInitParallelSeqScan
 * ----------------------------------------------------------------
 */
ParallelSeqScanState *
ExecInitParallelSeqScan(ParallelSeqScan *node, EState *estate, int eflags)
{
	ParallelSeqScanState *parallelscanstate;
	ShmScanDesc			 currentShmScanDesc;
	worker_result		 workerResult;
	BlockNumber			 end_block;

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
	 * initialize scan relation
	 */
	InitParallelScanRelation(&parallelscanstate->ss, estate, eflags);

	parallelscanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&parallelscanstate->ss.ps);
	ExecAssignScanProjectionInfo(&parallelscanstate->ss);

	/*
	 * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
	 * here, no need to start workers.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return parallelscanstate;


	/* Initialize the workers required to perform parallel scan. */
	InitializeParallelWorkers(((SeqScan *) parallelscanstate->ss.ps.plan)->scanrelid,
							  node->scan.plan.targetlist,
							  node->scan.plan.qual,
							  estate->es_range_table,
							  estate->es_param_list_info,
							  estate->es_instrument,
							  &parallelscanstate->inst_options_space,
							  &parallelscanstate->responseq,
							  &parallelscanstate->pcxt,
							  node->num_blocks_per_worker,
							  node->num_workers);

	/* Initialize the blocks to be scanned by master backend. */
	end_block = (parallelscanstate->pcxt->nworkers + 1) *
				node->num_blocks_per_worker;
	((SeqScan*) parallelscanstate->ss.ps.plan)->startblock =
								end_block - node->num_blocks_per_worker;
	/*
	 * As master backend is the last backend to scan the blocks, it
	 * should scan all the blocks.
	 */
	((SeqScan*) parallelscanstate->ss.ps.plan)->endblock = InvalidBlockNumber;

	/* Set the scan limits for master backend. */
	heap_setscanlimits(parallelscanstate->ss.ss_currentScanDesc,
					   ((SeqScan*) parallelscanstate->ss.ps.plan)->startblock,
					   (parallelscanstate->ss.ss_currentScanDesc->rs_nblocks -
					   ((SeqScan*) parallelscanstate->ss.ps.plan)->startblock));

	/*
	 * Use result tuple descriptor to fetch data from shared memory queues
	 * as the worker backends would have put the data after projection.
	 * Number of queue's must be equal to number of worker backends.
	 */
	currentShmScanDesc = shm_beginscan(parallelscanstate->pcxt->nworkers);
	workerResult = ExecInitWorkerResult(parallelscanstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
										parallelscanstate->pcxt->nworkers);

	parallelscanstate->pss_currentShmScanDesc = currentShmScanDesc;
	parallelscanstate->pss_workerResult	= workerResult;

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
