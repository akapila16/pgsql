/*-------------------------------------------------------------------------
 *
 * parallelpath.c
 *	  Routines to determine which conditions are usable for scanning
 *	  a given relation, and create ParallelPaths accordingly.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/parallelpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/rel.h"


/*
 *	IsTargetListContainNonVars -
 *		Check if target list contain non-var entries.
 */
static bool
IsTargetListContainNonVars(List *targetlist)
{
	ListCell   *l;

	foreach(l, targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(l);

		if (!IsA(te, TargetEntry))
			continue;			/* probably should never happen */
		if (!IsA(te->expr, Var))
			return true;
	}
	return false;
}

/*
 *	check_simple_qual -
 *		Check if qual is made only of simple things we can
 *		hand out directly to backend worker for execution.
 *
 *		XXX - Currently we don't allow to push an expression
 *		if it contains volatile function, however eventually we
 *		need a mechanism (proisparallel) with which we can distinquish
 *		the functions that can be pushed for execution by parallel
 *		worker.
 */
static bool
check_simple_qual(Node *node)
{
	if (node == NULL)
		return TRUE;

	if (contain_volatile_functions(node))
		return FALSE;

	return TRUE;
}

/*
 * create_parallelscan_paths
 *	  Create paths corresponding to parallel scans of the given rel.
 *	  Currently we only support parallel sequential scan.
 *
 *	  Candidate paths are added to the rel's pathlist (using add_path).
 */
void
create_parallelscan_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int num_parallel_workers = 0;
	Oid			reloid;
	Relation	relation;

	/*
	 * parallel scan is possible only if user has set
	 * parallel_seqscan_degree to value greater than 0.
	 */
	if (parallel_seqscan_degree <= 0)
		return;

	/*
	 * parallel scan is not supported for joins.
	 */
	if (root->simple_rel_array_size > 2)
		return;

	/* parallel scan is supportted only for Select statements. */
	if (root->parse->commandType != CMD_SELECT)
		return;

	reloid = planner_rt_fetch(rel->relid, root)->relid;

	relation = heap_open(reloid, NoLock);

	/*
	 * Temporary relations can't be scanned by parallel workers as
	 * they are visible only to local sessions.
	 */
	if (RelationUsesLocalBuffers(relation))
	{
		heap_close(relation, NoLock);
		return;
	}

	heap_close(relation, NoLock);

	/*
	 * parallel scan is not supported for non-var target list.
	 *
	 * XXX - This is to keep the implementation simple, we can do this
	 * in future.  Here we are checking by passing root->parse->targetList
	 * instead of rel->reltargetlist because rel->targetlist always contains
	 * Vars (refer build_base_rel_tlists).
	 */
	if (IsTargetListContainNonVars(root->parse->targetList))
	   return;

	/*
	 * parallel scan is not supported for mutable functions
	 */
	if (!check_simple_qual((Node*) extract_actual_clauses(rel->baserestrictinfo, false)))
		return;

	/*
	 * There should be atleast one page to scan for each worker.
	 */
	if (parallel_seqscan_degree <= rel->pages)
		num_parallel_workers = parallel_seqscan_degree;
	else
		num_parallel_workers = rel->pages;

	add_path(rel, (Path *) create_parallelseqscan_path(root, rel,
													   num_parallel_workers));
}
