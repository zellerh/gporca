//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CStatsPredNDV.cpp
//
//	@doc:
//		Implementation of statistics filter for NDV based equality predicate
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredNDV.h"
#include "naucrates/md/CMDIdGPDB.h"

#include "gpopt/base/CColRef.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredNDV::CStatisticsFilterPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredNDV::CStatsPredNDV
	(
	const CColRef *colref,
	ULONG num_distinct_values
	)
	:
	CStatsPred(gpos::ulong_max),
	m_num_distinct_values(num_distinct_values)
{
	GPOS_ASSERT(NULL != colref);

	m_colid = colref->Id();
}

// EOF

