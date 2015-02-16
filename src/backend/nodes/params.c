/*-------------------------------------------------------------------------
 *
 * params.c
 *	  Support for finding the values associated with Param nodes.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/nodes/params.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/params.h"
#include "storage/shmem.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

/*
 * for each bind parameter, pass this structure followed by value
 * except for pass-by-value parameters.
 */
typedef struct SerializedParamExternData
{
	Datum		value;			/*pass-by-val are directly stored */
	Size		length;			/* length of parameter value */
	bool		isnull;			/* is it NULL? */
	uint16		pflags;			/* flag bits, see above */
	Oid			ptype;			/* parameter's datatype, or 0 */
} SerializedParamExternData;

/*
 * Copy a ParamListInfo structure.
 *
 * The result is allocated in CurrentMemoryContext.
 *
 * Note: the intent of this function is to make a static, self-contained
 * set of parameter values.  If dynamic parameter hooks are present, we
 * intentionally do not copy them into the result.  Rather, we forcibly
 * instantiate all available parameter values and copy the datum values.
 */
ParamListInfo
copyParamList(ParamListInfo from)
{
	ParamListInfo retval;
	Size		size;
	int			i;

	if (from == NULL || from->numParams <= 0)
		return NULL;

	/* sizeof(ParamListInfoData) includes the first array element */
	size = sizeof(ParamListInfoData) +
		(from->numParams - 1) * sizeof(ParamExternData);

	retval = (ParamListInfo) palloc(size);
	retval->paramFetch = NULL;
	retval->paramFetchArg = NULL;
	retval->parserSetup = NULL;
	retval->parserSetupArg = NULL;
	retval->numParams = from->numParams;

	for (i = 0; i < from->numParams; i++)
	{
		ParamExternData *oprm = &from->params[i];
		ParamExternData *nprm = &retval->params[i];
		int16		typLen;
		bool		typByVal;

		/* give hook a chance in case parameter is dynamic */
		if (!OidIsValid(oprm->ptype) && from->paramFetch != NULL)
			(*from->paramFetch) (from, i + 1);

		/* flat-copy the parameter info */
		*nprm = *oprm;

		/* need datumCopy in case it's a pass-by-reference datatype */
		if (nprm->isnull || !OidIsValid(nprm->ptype))
			continue;
		get_typlenbyval(nprm->ptype, &typLen, &typByVal);
		nprm->value = datumCopy(nprm->value, typByVal, typLen);
	}

	return retval;
}

/*
 * Estimate the amount of space required to serialize the bound
 * parameters.
 */
Size
EstimateBoundParametersSpace(ParamListInfo paramInfo)
{
	Size		size;
	int			i;

	/* Add space required for saving numParams */
	size = sizeof(int);

	if (paramInfo)
	{
		/* Add space required for saving the param data */
		for (i = 0; i < paramInfo->numParams; i++)
		{
			/*
			 * for each parameter, calculate the size of fixed part
			 * of parameter (SerializedParamExternData) and length of
			 * parameter value.
			 */
			ParamExternData *oprm;
			int16		typLen;
			bool		typByVal;
			Size		length;

			length = sizeof(SerializedParamExternData);

			oprm = &paramInfo->params[i];

			get_typlenbyval(oprm->ptype, &typLen, &typByVal);

			/*
			 * pass-by-value parameters are directly stored in
			 * SerializedParamExternData, so no need of additional
			 * space for them.
			 */
			if (!(typByVal || oprm->isnull))
			{
				length += datumGetSize(oprm->value, typByVal, typLen);
				size = add_size(size, length);

				/* Allow space for terminating zero-byte */
				size = add_size(size, 1);
			}
			else
				size = add_size(size, length);
		}
	}

	return size;
}

/*
 * Serialize the bind parameters into the memory, beginning at start_address.
 * maxsize should be at least as large as the value returned by
 * EstimateBoundParametersSpace.
 */
void
SerializeBoundParams(ParamListInfo paramInfo, Size maxsize, char *start_address)
{
	char	   *curptr;
	SerializedParamExternData *retval;
	int i;

	/*
	 * First, we store the number of bind parameters, if there is
	 * no bind parameter then no need to store any more information.
	 */
	if (paramInfo && paramInfo->numParams > 0)
		* (int *) start_address = paramInfo->numParams;
	else
	{
		* (int *) start_address = 0;
		return;
	}
	curptr = start_address + sizeof(int);


	for (i = 0; i < paramInfo->numParams; i++)
	{
		ParamExternData *oprm;
		int16		typLen;
		bool		typByVal;
		Size		datumlength, length;
		const char	*s;

		Assert (curptr <= start_address + maxsize);
		retval = (SerializedParamExternData*) curptr;
		oprm = &paramInfo->params[i];

		retval->isnull = oprm->isnull;
		retval->pflags = oprm->pflags;
		retval->ptype = oprm->ptype;
		retval->value = oprm->value;

		curptr = curptr + sizeof(SerializedParamExternData);

		if (retval->isnull)
			continue;

		get_typlenbyval(oprm->ptype, &typLen, &typByVal);

		if (!typByVal)
		{
			datumlength = datumGetSize(oprm->value, typByVal, typLen);
			s = (char *) DatumGetPointer(oprm->value);
			memcpy(curptr, s, datumlength);
			length = datumlength;
			curptr[length] = '\0';
			retval->length = length;
			curptr += length + 1;
		}
	}
}

/*
 * RestoreBoundParams
 *		Restore bind parameters from the specified address.
 *
 * The params are palloc'd in CurrentMemoryContext.
 */
ParamListInfo
RestoreBoundParams(char *start_address)
{
	ParamListInfo retval;
	Size		size;
	int			num_params,i;
	char	   *curptr;

	num_params = * (int *) start_address;

	if (num_params <= 0)
		return NULL;

	/* sizeof(ParamListInfoData) includes the first array element */
	size = sizeof(ParamListInfoData) +
		(num_params - 1) * sizeof(ParamExternData);
	retval = (ParamListInfo) palloc(size);
	retval->paramFetch = NULL;
	retval->paramFetchArg = NULL;
	retval->parserSetup = NULL;
	retval->parserSetupArg = NULL;
	retval->numParams = num_params;

	curptr = start_address + sizeof(int);

	for (i = 0; i < num_params; i++)
	{
		SerializedParamExternData *nprm;
		char	*s;
		int16		typLen;
		bool		typByVal;

		nprm = (SerializedParamExternData *) curptr;

		/* copy the parameter info */
		retval->params[i].isnull = nprm->isnull;
		retval->params[i].pflags = nprm->pflags;
		retval->params[i].ptype = nprm->ptype;
		retval->params[i].value = nprm->value;

		curptr = curptr + sizeof(SerializedParamExternData);

		if (nprm->isnull)
			continue;

		get_typlenbyval(nprm->ptype, &typLen, &typByVal);

		if (!typByVal)
		{
			s = palloc(nprm->length + 1);
			memcpy(s, curptr, nprm->length + 1);
			retval->params[i].value = CStringGetDatum(s);

			curptr += nprm->length + 1;
		}
	}

	return retval;
}
