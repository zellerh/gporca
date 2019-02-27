//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinHybridGreedyIk.cpp
//
//	@doc:
//		Implementation of n-ary join expansion using dynamic programming
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformExpandNAryJoinHybridGreedyIk.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CJoinOrderHybridGreedyIk.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinHybridGreedyIk::CXformExpandNAryJoinHybridGreedyIk
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinHybridGreedyIk::CXformExpandNAryJoinHybridGreedyIk
	(
	IMemoryPool *mp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(mp) CExpression
					(
					mp,
					GPOS_NEW(mp) CLogicalNAryJoin(mp),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternMultiLeaf(mp)),
					GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinHybridGreedyIk::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinHybridGreedyIk::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinHybridGreedyIk::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinHybridGreedyIk::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *mp = pxfctxt->Pmp();

	const ULONG arity = pexpr->Arity();
	GPOS_ASSERT(arity >= 3);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CExpression *pexprScalar = (*pexpr)[arity - 1];
	CExpressionArray *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	// create join order using dynamic programming
	CJoinOrderHybridGreedyIk jodp(mp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jodp.PexprExpand();

	const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->Size();
	for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
	{
		CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
		if (pexprJoinOrder != pexprResult)
		{
			CExpression *pexprTemp = CNormalizer::PexprNormalize(mp, pexprJoinOrder);
			pxfres->Add(pexprTemp);
		}
	}
}

// EOF
