/*-------------------------------------------------------------------------
 *
 * backendworker.c
 *	  Support routines for setting up backend workers.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
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
#include "executor/nodeParallelSeqscan.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
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


/* Table-of-contents constants for our dynamic shared memory segment. */
#define PARALLEL_KEY_SCANRELID		0
#define PARALLEL_KEY_TARGETLIST		1
#define PARALLEL_KEY_QUAL			2
#define	PARALLEL_KEY_RANGETBL		3
#define	PARALLEL_KEY_PARAMS			4
#define PARALLEL_KEY_BLOCKS			5
#define PARALLEL_KEY_INST_OPTIONS	6
#define PARALLEL_KEY_INST_INFO		7
#define PARALLEL_KEY_TUPLE_QUEUE	8
#define PARALLEL_KEY_OPERATION		9

static void ParallelQueryMain(dsm_segment *seg, shm_toc *toc);
static void RestoreAndExecuteParallelScan(dsm_segment *seg, shm_toc *toc);

/*
 * EstimateParallelQueryElemsSpace
 *
 * Estimate the amount of space required to record information of
 * query elements that need to be copied to parallel workers.
 */
void
EstimateParallelQueryElemsSpace(ParallelContext *pcxt,
								char *targetlist_str, char *qual_str,
								Size *targetlist_len, Size *qual_len)
{
	*targetlist_len = strlen(targetlist_str) + 1;
	shm_toc_estimate_chunk(&pcxt->estimator, targetlist_len);

	*qual_len = strlen(qual_str) + 1;
	shm_toc_estimate_chunk(&pcxt->estimator, qual_len);

	/* keys for parallel query elements. */
	shm_toc_estimate_keys(&pcxt->estimator, 2);
}

/*
 * StoreParallelQueryElems
 * 
 * Sets up target list and qualification required for parallel
 * execution.
 */
void
StoreParallelQueryElems(ParallelContext *pcxt,
						char *targetlist_str, char *qual_str,
						Size targetlist_len, Size qual_len)
{
	char	   *targetlistdata;
	char	   *qualdata;

	/* Store target list in dynamic shared memory. */
	targetlistdata = shm_toc_allocate(pcxt->toc, targetlist_len);
	memcpy(targetlistdata, targetlist_str, targetlist_len);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TARGETLIST, targetlistdata);

	/* Store qual list in dynamic shared memory. */
	qualdata = shm_toc_allocate(pcxt->toc, qual_len);
	memcpy(qualdata, qual_str, qual_len);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUAL, qualdata);
}

/*
 * EstimateParallelSupportInfoSpace
 *
 * Estimate the amount of space required to record information of
 * bind parameters and instrumentation information that need to be
 * retrieved from parallel workers.
 */
void
EstimateParallelSupportInfoSpace(ParallelContext *pcxt, ParamListInfo params,
								 int instOptions, Size *params_len)
{
	*params_len = EstimateBoundParametersSpace(params);
	shm_toc_estimate_chunk(&pcxt->estimator, *params_len);

	/* account for instrumentation options. */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(int));

	/*
	 * We expect each worker to populate the instrumentation structure
	 * allocated by master backend and then master backend will aggregate
	 * all the information, so account it for each worker.
	 */
	if (instOptions)
		shm_toc_estimate_chunk(&pcxt->estimator,
							   sizeof(Instrumentation) * pcxt->nworkers);

	/* keys for parallel support information. */
	shm_toc_estimate_keys(&pcxt->estimator, 2);
}

/*
 * StoreParallelSupportInfo
 * 
 * Sets up the bind parameters, instrumentation information
 * required for parallel execution.
 */
void
StoreParallelSupportInfo(ParallelContext *pcxt, ParamListInfo params,
						 int instOptions, int params_len,
						 char **inst_options_space)
{
	char	*paramsdata;
	int		*inst_options;

	/*
	 * Store bind parameter's list in dynamic shared memory.  This is
	 * used for parameters in prepared query.
	 */
	paramsdata = shm_toc_allocate(pcxt->toc, params_len);
	SerializeBoundParams(params, params_len, paramsdata);
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
 * EstimateParallelSeqScanSpace
 *
 * Estimate the amount of space required to record information of
 * scanrelId, rangetable and block range that need to be copied
 * to parallel workers.
 */
void
EstimateParallelSeqScanSpace(ParallelContext *pcxt, Index scanrelId,
							 char *rangetbl_str, BlockNumber numBlocksPerWorker,
							 Size *rangetbl_len)
{
	/* Estimate space for parallel seq. scan specific contents. */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(NodeTag));
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(scanrelId));

	*rangetbl_len = strlen(rangetbl_str) + 1;
	shm_toc_estimate_chunk(&pcxt->estimator, rangetbl_len);

	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(BlockNumber));

	/* keys for parallel support information. */
	shm_toc_estimate_keys(&pcxt->estimator, 4);
}

/*
 * StoreParallelSeqScan
 * 
 * Sets up the scanrelid, rangetable entries and block range
 * for parallel sequence scan.
 */
void
StoreParallelSeqScan(ParallelContext *pcxt, Index scanrelId,
					 char *rangetbl_str, Size rangetbl_len,
					 BlockNumber numBlocksPerWorker)
{
	Oid			*scanreliddata;
	char		*rangetbldata;
	BlockNumber	*num_blocks_per_worker;
	NodeTag		*nodetype;

	/* Store sequence scan Nodetag in dynamic shared memory. */
	nodetype = shm_toc_allocate(pcxt->toc, sizeof(NodeTag));
	*nodetype = T_SeqScan;
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_OPERATION, nodetype);

	/* Store scan relation id in dynamic shared memory. */
	scanreliddata = shm_toc_allocate(pcxt->toc, sizeof(Index));
	*scanreliddata = scanrelId;
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_SCANRELID, scanreliddata);

	/* Store range table list in dynamic shared memory. */
	rangetbldata = shm_toc_allocate(pcxt->toc, rangetbl_len);
	memcpy(rangetbldata, rangetbl_str, rangetbl_len);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_RANGETBL, rangetbldata);

	/* Store blocks to be scanned by each worker in dynamic shared memory. */
	num_blocks_per_worker = shm_toc_allocate(pcxt->toc, sizeof(BlockNumber));
	*num_blocks_per_worker = numBlocksPerWorker;
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BLOCKS, num_blocks_per_worker);
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
 * StoreResponseQueueAndStartWorkers
 * 
 * It sets up the response queues for backend workers to
 * return tuples to the main backend and start the workers.
 * This function must be called after setting up all the other
 * necessary parallel execution related information as it start
 * the workers after which we initialize or pass the parallel
 * state information.
 */
void
StoreResponseQueueAndStartWorkers(ParallelContext *pcxt,
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
	LaunchParallelWorkers(pcxt);

	for (i = 0; i < pcxt->nworkers; ++i)
		shm_mq_set_handle((*responseqp)[i], pcxt->worker[i].bgwhandle);
}

/*
 * InitializeParallelWorkers
 *
 *	Sets up the required infrastructure for backend workers to
 *	perform execution and return results to the main backend.
 */
void
InitializeParallelWorkers(Index scanrelId, List *targetList, List *qual,
						  List *rangeTable, ParamListInfo params, int instOptions,
						  char **inst_options_space, shm_mq_handle ***responseqp,
						  ParallelContext **pcxtp, BlockNumber numBlocksPerWorker,
						  int nWorkers)
{
	bool		already_in_parallel_mode = IsInParallelMode();
	Size		targetlist_len, qual_len, rangetbl_len, params_len;
	char	   *targetlist_str;
	char	   *qual_str;
	char	   *rangetbl_str;
	ParallelContext *pcxt;

	if (!already_in_parallel_mode)
		EnterParallelMode();

	pcxt = CreateParallelContext(ParallelQueryMain, nWorkers);

	/* Estimate space for parallel seq. scan specific contents. */
	targetlist_str = nodeToString(targetList);
	qual_str = nodeToString(qual);
	EstimateParallelQueryElemsSpace(pcxt, targetlist_str, qual_str,
									&targetlist_len, &qual_len);

	rangetbl_str = nodeToString(rangeTable);
	EstimateParallelSeqScanSpace(pcxt, scanrelId, rangetbl_str, numBlocksPerWorker,
								 &rangetbl_len);

	EstimateParallelSupportInfoSpace(pcxt, params, instOptions, &params_len);

	EstimateResponseQueueSpace(pcxt);

	InitializeParallelDSM(pcxt);

	StoreParallelQueryElems(pcxt, targetlist_str, qual_str,
							targetlist_len, qual_len);
	StoreParallelSeqScan(pcxt, scanrelId, rangetbl_str,
						 rangetbl_len, numBlocksPerWorker);
	StoreParallelSupportInfo(pcxt, params, instOptions,
							 params_len, inst_options_space);
	StoreResponseQueueAndStartWorkers(pcxt, responseqp);

	/* Return results to caller. */
	*pcxtp = pcxt;
}

/*
 * GetParallelQueryElems
 *
 * Look up based on keys in dynamic shared memory segment
 * and get the targetlist and qualification list required
 * to perform parallel operation.
 */
void
GetParallelQueryElems(shm_toc *toc, List **targetList, List **qual)
{
	char	    *targetlistdata;
	char		*qualdata;

	targetlistdata = shm_toc_lookup(toc, PARALLEL_KEY_TARGETLIST);
	qualdata = shm_toc_lookup(toc, PARALLEL_KEY_QUAL);

	*targetList = (List *) stringToNode(targetlistdata);
	*qual = (List *) stringToNode(qualdata);
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
 * GetParallelSupportInfo
 *
 * Look up based on keys in dynamic shared memory segment
 * and get the scanrelId, rangeTable and block scan range
 * required to perform parallel sequential scan.
 */
void
GetParallelSeqScanInfo(shm_toc *toc, Index *scanrelId,
					   List **rangeTableList,
					   BlockNumber *start_block,
					   BlockNumber *end_block)
{
	char		*rangetbldata;
	BlockNumber *num_blocks_per_worker;
	Index		*scanrel;

	scanrel = shm_toc_lookup(toc, PARALLEL_KEY_SCANRELID);
	rangetbldata = shm_toc_lookup(toc, PARALLEL_KEY_RANGETBL);
	num_blocks_per_worker = shm_toc_lookup(toc, PARALLEL_KEY_BLOCKS);

	*scanrelId = *scanrel;
	*rangeTableList = (List *) stringToNode(rangetbldata);

	*end_block = (ParallelWorkerNumber + 1) * (*num_blocks_per_worker);
	*start_block = *end_block - *num_blocks_per_worker;
}

/*
 * GetParallelSupportInfo
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
	NodeTag		*nodetype;

	nodetype = shm_toc_lookup(toc, PARALLEL_KEY_OPERATION);

	switch (*nodetype)
	{
		case T_SeqScan:
			RestoreAndExecuteParallelScan(seg, toc);
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) *nodetype);
			break;
	}
}

/*
 * RestoreAndExecuteParallelScan
 *
 * Lookup the parallel sequence scan related parameters
 * from dynamic shared memory segment and setup the
 * statement to execute the scan.
 */
void
RestoreAndExecuteParallelScan(dsm_segment *seg, shm_toc *toc)
{
	shm_mq		*mq;
	List		*targetList = NIL;
	List		*qual = NIL;
	List *rangeTableList = NIL;
	ParamListInfo params;
	int			inst_options;
	char		*instrument = NULL;
	Index		scanrelId;
	BlockNumber start_block;
	BlockNumber end_block;
	worker_stmt	*workerstmt;

	SetupResponseQueue(seg, toc, &mq);

	GetParallelQueryElems(toc, &targetList, &qual);
	GetParallelSeqScanInfo(toc, &scanrelId, &rangeTableList,
						   &start_block, &end_block);
	GetParallelSupportInfo(toc, &params, &inst_options, &instrument);

	workerstmt = palloc(sizeof(worker_stmt));

	workerstmt->scanrelId = scanrelId;
	workerstmt->targetList = targetList;
	workerstmt->qual = qual;
	workerstmt->rangetableList = rangeTableList;
	workerstmt->params	= params;
	workerstmt->startBlock = start_block;
	workerstmt->inst_options = inst_options;
	workerstmt->instrument = instrument;

	/*
	 * Last worker should scan all the remaining blocks.
	 *
	 * XXX - It is possible that expected number of workers
	 * won't get started, so to handle such cases master
	 * backend should scan remaining blocks.
	 */
	workerstmt->endBlock = end_block;

	/* Execute the worker command. */
	exec_parallel_scan(workerstmt);

	/*
	 * Once we are done with sending tuples, detach from
	 * shared memory message queue used to send tuples.
	 */
	shm_mq_detach(mq);
}
