/*-------------------------------------------------------------------------
 *
 * backendworker.c
 *	  Support routines for setting up backend workers.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/backendworker.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		InitializeParallelWorkers				Setup dynamic shared memory and parallel backend workers.
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/parallel.h"
#include "commands/dbcommands.h"
#include "commands/async.h"
#include "executor/nodeFunnel.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "postmaster/backendworker.h"
#include "storage/ipc.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"


#define PARALLEL_TUPLE_QUEUE_SIZE					65536

static void ParallelQueryMain(dsm_segment *seg, shm_toc *toc);
static void
EstimateParallelSupportInfoSpace(ParallelContext *pcxt, ParamListInfo params,
								 int instOptions, Size *params_size);
static void
StoreParallelSupportInfo(ParallelContext *pcxt, ParamListInfo params,
						 int instOptions, int params_size,
						 char **inst_options_space);
static void
EstimatePartialSeqScanSpace(ParallelContext *pcxt, EState *estate,
							char *plannedstmt_str, Size *plannedstmt_len,
							Size *pscan_size);
static void
StorePartialSeqScan(ParallelContext *pcxt, EState *estate, Relation rel,
					 char *plannedstmt_str, ParallelHeapScanDesc *pscan,
					 Size plannedstmt_size, Size pscan_size);
static void EstimateResponseQueueSpace(ParallelContext *pcxt);
static void
StoreResponseQueue(ParallelContext *pcxt,
				   shm_mq_handle ***responseqp);
static void
GetPlannedStmt(shm_toc *toc, PlannedStmt **plannedstmt);
static void
GetParallelSupportInfo(shm_toc *toc, ParamListInfo *params,
					   int *inst_options, char **instrument);
static void
SetupResponseQueue(dsm_segment *seg, shm_toc *toc, shm_mq **mq);


/*
 * EstimateParallelSupportInfoSpace
 *
 * Estimate the amount of space required to record information of
 * bind parameters and instrumentation information that need to be
 * retrieved from parallel workers.
 */
void
EstimateParallelSupportInfoSpace(ParallelContext *pcxt, ParamListInfo params,
								 int instOptions, Size *params_size)
{
	*params_size = EstimateBoundParametersSpace(params);
	shm_toc_estimate_chunk(&pcxt->estimator, *params_size);

	/* account for instrumentation options. */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(int));

	/*
	 * We expect each worker to populate the instrumentation structure
	 * allocated by master backend and then master backend will aggregate
	 * all the information, so account it for each worker.
	 */
	if (instOptions)
	{
		shm_toc_estimate_chunk(&pcxt->estimator,
							   sizeof(Instrumentation) * pcxt->nworkers);
		/* keys for parallel support information. */
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}

	/* keys for parallel support information. */
	shm_toc_estimate_keys(&pcxt->estimator, 2);
}

/*
 * StoreParallelSupportInfo
 * 
 * Sets up the bind parameters and instrumentation information
 * required for parallel execution.
 */
void
StoreParallelSupportInfo(ParallelContext *pcxt, ParamListInfo params,
						 int instOptions, int params_size,
						 char **inst_options_space)
{
	char	*paramsdata;
	int		*inst_options;

	/*
	 * Store bind parameter's list in dynamic shared memory.  This is
	 * used for parameters in prepared query.
	 */
	paramsdata = shm_toc_allocate(pcxt->toc, params_size);
	SerializeBoundParams(params, params_size, paramsdata);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_PARAMS, paramsdata);

	/* Store instrument options in dynamic shared memory. */
	inst_options = shm_toc_allocate(pcxt->toc, sizeof(int));
	*inst_options = instOptions;
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_INST_OPTIONS, inst_options);

	/*
	 * Allocate space for instrumentation information to be filled by
	 * each worker.
	 */
	if (instOptions)
	{
		*inst_options_space =
			shm_toc_allocate(pcxt->toc, sizeof(Instrumentation) * pcxt->nworkers);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_INST_INFO, *inst_options_space);
	}
}

/*
 * EstimatePartialSeqScanSpace
 *
 * Estimate the amount of space required to record information of
 * planned statement and parallel heap scan descriptor that need
 * to be copied to parallel workers.
 */
void
EstimatePartialSeqScanSpace(ParallelContext *pcxt, EState *estate,
							char *plannedstmt_str, Size *plannedstmt_len,
							Size *pscan_size)
{
	/* Estimate space for partial seq. scan specific contents. */
	*plannedstmt_len = strlen(plannedstmt_str) + 1;
	shm_toc_estimate_chunk(&pcxt->estimator, *plannedstmt_len);

	*pscan_size = heap_parallelscan_estimate(estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, *pscan_size);

	/* keys for parallel support information. */
	shm_toc_estimate_keys(&pcxt->estimator, 2);
}

/*
 * StorePartialSeqScan
 * 
 * Sets up the planned statement and block range for parallel
 * sequence scan.
 */
void
StorePartialSeqScan(ParallelContext *pcxt, EState *estate, Relation rel,
					 char *plannedstmt_str, ParallelHeapScanDesc *pscan,
					 Size plannedstmt_size, Size pscan_size)
{
	char		*plannedstmtdata;

	/* Store range table list in dynamic shared memory. */
	plannedstmtdata = shm_toc_allocate(pcxt->toc, plannedstmt_size);
	memcpy(plannedstmtdata, plannedstmt_str, plannedstmt_size);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_PLANNEDSTMT, plannedstmtdata);

	/* Store parallel heap scan descriptor in dynamic shared memory. */
	*pscan = shm_toc_allocate(pcxt->toc, pscan_size);
	heap_parallelscan_initialize(*pscan, rel, estate->es_snapshot);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_SCAN, *pscan);
}

/*
 * EstimateResponseQueueSpace
 *
 * Estimate the amount of space required to record information of
 * tuple queues that need to be established between parallel workers
 * and master backend.
 */
void
EstimateResponseQueueSpace(ParallelContext *pcxt)
{
	/* Estimate space for parallel seq. scan specific contents. */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   (Size) PARALLEL_TUPLE_QUEUE_SIZE * pcxt->nworkers);

	/* keys for response queue. */
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/*
 * StoreResponseQueue
 * 
 * It sets up the response queue's for backend worker's to
 * return tuples to the main backend and start the workers.
 * This function must be called after setting up all the other
 * necessary parallel execution related information as it start
 * the workers after which we can't initialize or pass the parallel
 * state information.
 */
void
StoreResponseQueue(ParallelContext *pcxt,
				   shm_mq_handle ***responseqp)
{
	shm_mq		*mq;
	char		*tuple_queue_space;
	int			i;

	/* Allocate memory for shared memory queue handles. */
	*responseqp = (shm_mq_handle**) palloc(pcxt->nworkers * sizeof(shm_mq_handle*));

	/*
	 * Establish one message queue per worker in dynamic shared memory.
	 * These queues should be used to transmit tuple data.
	 */
	tuple_queue_space =
	   shm_toc_allocate(pcxt->toc, PARALLEL_TUPLE_QUEUE_SIZE * pcxt->nworkers);
	for (i = 0; i < pcxt->nworkers; ++i)
	{
		mq = shm_mq_create(tuple_queue_space + i * PARALLEL_TUPLE_QUEUE_SIZE,
						   (Size) PARALLEL_TUPLE_QUEUE_SIZE);
		
		shm_mq_set_receiver(mq, MyProc);

		/*
		 * Attach the queue before launching a worker, so that we'll automatically
		 * detach the queue if we error out.  (Otherwise, the worker might sit
		 * there trying to write the queue long after we've gone away.)
		 */
		(*responseqp)[i] = shm_mq_attach(mq, pcxt->seg, NULL);
	}
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLE_QUEUE, tuple_queue_space);

	/* Register backend workers. */
	/*LaunchParallelWorkers(pcxt);

	for (i = 0; i < pcxt->nworkers; ++i)
		shm_mq_set_handle((*responseqp)[i], pcxt->worker[i].bgwhandle);*/
}

/*
 * InitializeParallelWorkers
 *
 *	Sets up the required infrastructure for backend workers to
 *	perform execution and return results to the main backend.
 */
void
InitializeParallelWorkers(Plan *plan, EState *estate, Relation rel,
						  char **inst_options_space,
						  shm_mq_handle ***responseqp, ParallelContext **pcxtp,
						  ParallelHeapScanDesc *pscan, int nWorkers)
{
	bool		already_in_parallel_mode = IsInParallelMode();
	Size		params_size, pscan_size, plannedstmt_size;
	char	   *plannedstmt_str;
	PlannedStmt	*plannedstmt;
	ParallelContext *pcxt;

	if (!already_in_parallel_mode)
		EnterParallelMode();

	pcxt = CreateParallelContext(ParallelQueryMain, nWorkers);

	plannedstmt = create_worker_scan_plannedstmt((PartialSeqScan *)plan,
												 estate->es_range_table);
	plannedstmt_str = nodeToString(plannedstmt);

	EstimatePartialSeqScanSpace(pcxt, estate, plannedstmt_str,
								&plannedstmt_size, &pscan_size);
	EstimateParallelSupportInfoSpace(pcxt, estate->es_param_list_info,
									 estate->es_instrument, &params_size);
	EstimateResponseQueueSpace(pcxt);

	InitializeParallelDSM(pcxt);
	
	StorePartialSeqScan(pcxt, estate, rel, plannedstmt_str,
						pscan, plannedstmt_size, pscan_size);

	StoreParallelSupportInfo(pcxt, estate->es_param_list_info,
							 estate->es_instrument,
							 params_size, inst_options_space);
	StoreResponseQueue(pcxt, responseqp);

	/* Return results to caller. */
	*pcxtp = pcxt;
}

/*
 * GetParallelSupportInfo
 *
 * Look up based on keys in dynamic shared memory segment
 * and get the bind parameter's and instrumentation information
 * required to perform parallel operation.
 */
void
GetParallelSupportInfo(shm_toc *toc, ParamListInfo *params,
					   int *inst_options, char **instrument)
{
	char		*paramsdata;
	char		*inst_options_space;
	int			*instoptions;

	paramsdata = shm_toc_lookup(toc, PARALLEL_KEY_PARAMS);
	instoptions	= shm_toc_lookup(toc, PARALLEL_KEY_INST_OPTIONS);

	*params = RestoreBoundParams(paramsdata);

	*inst_options = *instoptions;
	if (inst_options)
	{
		inst_options_space = shm_toc_lookup(toc, PARALLEL_KEY_INST_INFO);
		*instrument = (inst_options_space +
			ParallelWorkerNumber * sizeof(Instrumentation));
	}
}

/*
 * GetPlannedStmt
 *
 * Look up based on keys in dynamic shared memory segment
 * and get the planned statement required to perform
 * parallel operation.
 */
void
GetPlannedStmt(shm_toc *toc, PlannedStmt **plannedstmt)
{
	char		*plannedstmtdata;

	plannedstmtdata = shm_toc_lookup(toc, PARALLEL_KEY_PLANNEDSTMT);

	*plannedstmt = (PlannedStmt *) stringToNode(plannedstmtdata);

	/* Fill in opfuncid values if missing */
	fix_opfuncids((Node*) (*plannedstmt)->planTree->qual);
	fix_opfuncids((Node*) (*plannedstmt)->planTree->targetlist);
}

/*
 * SetupResponseQueue
 *
 * Look up based on keys in dynamic shared memory segment
 * and get the tuple queue information for a particular worker,
 * attach to the queue and redirect all futher responses from
 * worker backend via that queue.
 */
void
SetupResponseQueue(dsm_segment *seg, shm_toc *toc, shm_mq **mq)
{
	char		*tuple_queue_space;
	shm_mq_handle *responseq;

	tuple_queue_space = shm_toc_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE);
	*mq = (shm_mq *) (tuple_queue_space +
		ParallelWorkerNumber * PARALLEL_TUPLE_QUEUE_SIZE);

	shm_mq_set_sender(*mq, MyProc);
	responseq = shm_mq_attach(*mq, seg, NULL);

	/* Redirect protocol messages to responseq. */
	pq_redirect_to_tuple_shm_mq(responseq);
}

/*
 * ParallelQueryMain
 *
 * Execute the operation to return the tuples or other information
 * to parallelism driving node.
 */
void
ParallelQueryMain(dsm_segment *seg, shm_toc *toc)
{
	shm_mq		*mq;
	PlannedStmt *plannedstmt;
	ParamListInfo params;
	int			inst_options;
	char		*instrument = NULL;
	ParallelStmt	*parallelstmt;

	while(1)
	{
	}

	SetupResponseQueue(seg, toc, &mq);

	GetPlannedStmt(toc, &plannedstmt);
	GetParallelSupportInfo(toc, &params, &inst_options, &instrument);

	parallelstmt = palloc(sizeof(ParallelStmt));

	parallelstmt->plannedstmt = plannedstmt;
	parallelstmt->params	= params;
	parallelstmt->inst_options = inst_options;
	parallelstmt->instrument = instrument;
	parallelstmt->toc = toc;

	/* Execute the worker command. */
	exec_parallel_stmt(parallelstmt);

	/*
	 * Once we are done with sending tuples, detach from
	 * shared memory message queue used to send tuples.
	 */
	shm_mq_detach(mq);
}
