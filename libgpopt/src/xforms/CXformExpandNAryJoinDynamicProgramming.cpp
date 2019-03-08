//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynamicProgramming.cpp
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
#include "gpopt/xforms/CXformExpandNAryJoinDynamicProgramming.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CJoinOrderDynamicProgramming.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDynamicProgramming::CXformExpandNAryJoinDynamicProgramming
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformExpandNAryJoinDynamicProgramming::CXformExpandNAryJoinDynamicProgramming
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
//		CXformExpandNAryJoinDynamicProgramming::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformExpandNAryJoinDynamicProgramming::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	return CXformUtils::ExfpExpandJoinOrder(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformExpandNAryJoinDynamicProgramming::Transform
//
//	@doc:
//		Actual transformation of n-ary join to cluster of inner joins using
//		dynamic programming
//
//---------------------------------------------------------------------------
void
CXformExpandNAryJoinDynamicProgramming::Transform
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

	// Make an expression array with all the FIXMEchilds (onewayjoin/child/leaf)
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	// Make an expression array with all the join conditions, one entry for
	// every conjunct (ANDed condition)
	CExpression *pexprScalar = (*pexpr)[arity - 1];
	CExpressionArray *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);

	// create join order using dynamic programming
	CJoinOrderDynamicProgramming jodp(mp, pdrgpexpr, pdrgpexprPreds);
	CExpression *pexprResult = jodp.PexprExpand();

	// Retrieve top K join orders from jodp and add as alternatives
	const ULONG UlTopKJoinOrders = jodp.PdrgpexprTopK()->Size();
	for (ULONG ul = 0; ul < UlTopKJoinOrders; ul++)
	{
		CExpression *pexprJoinOrder = (*jodp.PdrgpexprTopK())[ul];
		if (pexprJoinOrder != pexprResult)
		{
			pexprJoinOrder->AddRef();
			pxfres->Add(pexprJoinOrder);
		}
	}
}

// EOF
