//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynProg.cpp
//
//	@doc:
//		Implementation of dynamic programming-based join order generation
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/xforms/CJoinOrderDynProg.h"

#include "gpopt/exception.h"

using namespace gpopt;

#define GPOPT_DP_JOIN_ORDERING_TOPK	10

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynProg::SComponentPair::SComponentPair
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
	:
	m_pbsFst(pbsFst),
	m_pbsSnd(pbsSnd)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);
	GPOS_ASSERT(pbsFst->IsDisjoint(pbsSnd));
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDynProg::SComponentPair::HashValue
	(
	const SComponentPair *pcomppair
	)
{
	GPOS_ASSERT(NULL != pcomppair);

	return CombineHashes
			(
			pcomppair->m_pbsFst->HashValue(),
			pcomppair->m_pbsSnd->HashValue()
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDynProg::SComponentPair::Equals
	(
	const SComponentPair *pcomppairFst,
	const SComponentPair *pcomppairSnd
	)
{
	GPOS_ASSERT(NULL != pcomppairFst);
	GPOS_ASSERT(NULL != pcomppairSnd);

	return pcomppairFst->m_pbsFst->Equals(pcomppairSnd->m_pbsFst) &&
		pcomppairFst->m_pbsSnd->Equals(pcomppairSnd->m_pbsSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynProg::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::CJoinOrderDynProg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynProg::CJoinOrderDynProg
	(
	IMemoryPool *mp,
	CExpressionArray *pdrgpexprComponents,
	CExpressionArray *pdrgpexprConjuncts
	)
	:
	CJoinOrder(mp, pdrgpexprComponents, pdrgpexprConjuncts, false /* m_include_loj_childs */)
{
	m_phmcomplink = GPOS_NEW(mp) ComponentPairToExpressionMap(mp);
	m_phmbsexpr = GPOS_NEW(mp) BitSetToExpressionMap(mp);
	m_phmexprcost = GPOS_NEW(mp) ExpressionToCostMap(mp);
	m_pdrgpexprTopKOrders = GPOS_NEW(mp) CExpressionArray(mp);
	m_pexprDummy = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));

#ifdef GPOS_DEBUG
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		GPOS_ASSERT(NULL != m_rgpcomp[ul]->m_pexpr->Pstats() &&
				"stats were not derived on input component");
	}
#endif // GPOS_DEBUG
	
	m_orig_pbs = GPOS_NEW(m_mp) CBitSet(m_mp);
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		(void) m_orig_pbs->ExchangeSet(ul);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::~CJoinOrderDynProg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynProg::~CJoinOrderDynProg()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	m_phmcomplink->Release();
	m_phmbsexpr->Release();
	m_phmexprcost->Release();
	m_pdrgpexprTopKOrders->Release();
	m_pexprDummy->Release();
	m_orig_pbs->Release();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDynProg::AddJoinOrder
	(
	CExpression *pexprJoin,
	CDouble dCost
	)
{
	GPOS_ASSERT(NULL != pexprJoin);
	GPOS_ASSERT(NULL != m_pdrgpexprTopKOrders);

	// length of the array will not be more than 10
	INT ulResults = m_pdrgpexprTopKOrders->Size();
	INT iReplacePos = -1;
	BOOL fAddJoinOrder = false;
	if (ulResults < GPOPT_DP_JOIN_ORDERING_TOPK)
	{
		// we have less than K results, always add the given expression
		fAddJoinOrder = true;
	}
	else
	{
		CDouble dmaxCost = 0.0;
		// we have stored K expressions, evict worst expression
		for (INT ul = 0; ul < ulResults; ul++)
		{
			CExpression *pexpr = (*m_pdrgpexprTopKOrders)[ul];
			CDouble *pd = m_phmexprcost->Find(pexpr);
			GPOS_ASSERT(NULL != pd);

			if (dmaxCost < *pd && dCost < *pd)
			{
				// found a worse expression
				dmaxCost = *pd;
				fAddJoinOrder = true;
				iReplacePos = ul;
			}
		}
	}

	if (fAddJoinOrder)
	{
		pexprJoin->AddRef();
		if (iReplacePos > -1)
		{
			m_pdrgpexprTopKOrders->Replace((ULONG) iReplacePos, pexprJoin);
		}
		else
		{
			m_pdrgpexprTopKOrders->Append(pexprJoin);
		}

		InsertExpressionCost(pexprJoin, dCost, false /*fValidateInsert*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprLookup
	(
	CBitSet *pbs
	)
{
	// if set has size 1, return expression directly
	if (1 == pbs->Size())
	{
		CBitSetIter bsi(*pbs);
		(void) bsi.Advance();

		return m_rgpcomp[bsi.Bit()]->m_pexpr;
	}

	// otherwise, return expression by looking up DP table
	return m_phmbsexpr->Find(pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	if (!pbsFst->IsDisjoint(pbsSnd) || 0 == pbsFst->Size() || 0 == pbsSnd->Size())
	{
		// components must be non-empty and disjoint
		return NULL;
	}

	CExpression *pexprPred = NULL;
	SComponentPair *pcomppair = NULL;

	// lookup link map
	for (ULONG ul = 0; ul < 2; ul++)
	{
		pbsFst->AddRef();
		pbsSnd->AddRef();
		pcomppair = GPOS_NEW(m_mp) SComponentPair(pbsFst, pbsSnd);
		pexprPred = m_phmcomplink->Find(pcomppair);
		if (NULL != pexprPred)
		{
			pcomppair->Release();
			if (m_pexprDummy == pexprPred)
			{
				return NULL;
			}
			return pexprPred;
		}

		// try again after swapping sets
		if (0 == ul)
		{
			pcomppair->Release();
			std::swap(pbsFst, pbsSnd);
		}
	}

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		m_pexprDummy->AddRef();
		pexprPred = m_pexprDummy;
	}

	// store predicate in link map
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmcomplink->Insert(pcomppair, pexprPred);
	GPOS_ASSERT(fInserted);

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprJoin
//
//	@doc:
//		Join expressions in the given two sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprJoin
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	GPOS_ASSERT(NULL != pbsFst);
	GPOS_ASSERT(NULL != pbsSnd);

	CExpression *pexprFst = PexprLookup(pbsFst);
	GPOS_ASSERT(NULL != pexprFst);

	CExpression *pexprSnd = PexprLookup(pbsSnd);
	GPOS_ASSERT(NULL != pexprSnd);

	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	GPOS_ASSERT(NULL != pexprScalar);

	pexprFst->AddRef();
	pexprSnd->AddRef();
	pexprScalar->AddRef();

	return CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprFst, pexprSnd, pexprScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDynProg::DeriveStats
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy != pexpr && NULL == pexpr->Pstats())
	{
		CExpressionHandle exprhdl(m_mp);
		exprhdl.Attach(pexpr);
		exprhdl.DeriveStats(m_mp, m_mp, NULL /*prprel*/, NULL /*stats_ctxt*/);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDynProg::InsertExpressionCost
	(
	CExpression *pexpr,
	CDouble dCost,
	BOOL fValidateInsert // if true, insertion must succeed
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (m_pexprDummy == pexpr)
	{
		// ignore dummy expression
		return;
	}

	if (!fValidateInsert && NULL != m_phmexprcost->Find(pexpr))
	{
		// expression already exists in cost map
		return;
	}

	pexpr->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmexprcost->Insert(pexpr, GPOS_NEW(m_mp) CDouble(dCost));
	GPOS_ASSERT(fInserted);
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprJoin
//
//	@doc:
//		Join expressions in the given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprJoin
	(
	CBitSet *pbs
	)
{
	GPOS_ASSERT(2 == pbs->Size());

	CBitSetIter bsi(*pbs);
	(void) bsi.Advance();
	ULONG ulCompFst = bsi.Bit();
	(void) bsi.Advance();
	ULONG ulCompSnd = bsi.Bit();
	GPOS_ASSERT(!bsi.Advance());

	CBitSet *pbsFst = GPOS_NEW(m_mp) CBitSet(m_mp);
	(void) pbsFst->ExchangeSet(ulCompFst);
	CBitSet *pbsSnd = GPOS_NEW(m_mp) CBitSet(m_mp);
	(void) pbsSnd->ExchangeSet(ulCompSnd);
	CExpression *pexprScalar = PexprPred(pbsFst, pbsSnd);
	pbsFst->Release();
	pbsSnd->Release();

	if (NULL == pexprScalar)
	{
		return NULL;
	}

	CExpression *pexprLeft = m_rgpcomp[ulCompFst]->m_pexpr;
	CExpression *pexprRight = m_rgpcomp[ulCompSnd]->m_pexpr;
	pexprLeft->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprJoin =
		CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprLeft, pexprRight, pexprScalar);

	DeriveStats(pexprJoin);
	// store solution in DP table
	pbs->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif // GPOS_DEBUG
		m_phmbsexpr->Insert(pbs, pexprJoin);
	GPOS_ASSERT(fInserted);

	return pexprJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprBestJoinOrderDP
//
//	@doc:
//		Find the best join order of a given set of elements using dynamic
//		programming;
//		given a set of elements (e.g., {A, B, C}), we find all possible splits
//		of the set (e.g., {A}, {B, C}) where at least one edge connects the
//		two subsets resulting from the split,
//		for each split, we find the best join orders of left and right subsets
//		recursively,
//		the function finds the split with the least cost, and stores the join
//		of its two subsets as the best join order of the given set
//
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprBestJoinOrderDP
	(
	CBitSet *//pbs // set of elements to be joined
	)
{
//	GPOS_ASSERT(pbs);
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::GenerateSubsets
//
//	@doc:
//		Generate all subsets of given array of elements
//
//---------------------------------------------------------------------------
void
CJoinOrderDynProg::GenerateSubsets
	(
	IMemoryPool *mp,
	CBitSet *pbsCurrent,
	ULONG *pulElems,
	ULONG size,
	ULONG ulIndex,
	CBitSetArray *pdrgpbsSubsets
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(ulIndex <= size);
	GPOS_ASSERT(NULL != pbsCurrent);
	GPOS_ASSERT(NULL != pulElems);
	GPOS_ASSERT(NULL != pdrgpbsSubsets);

	if (ulIndex == size)
	{
		pdrgpbsSubsets->Append(pbsCurrent);
		return;
	}

	CBitSet *pbsCopy = GPOS_NEW(mp) CBitSet(mp, *pbsCurrent);
#ifdef GPOS_DEBUG
	BOOL fSet =
#endif // GPOS_DEBUG
		pbsCopy->ExchangeSet(pulElems[ulIndex]);
	GPOS_ASSERT(!fSet);

	GenerateSubsets(mp, pbsCopy, pulElems, size, ulIndex + 1, pdrgpbsSubsets);
	GenerateSubsets(mp, pbsCurrent, pulElems, size, ulIndex + 1, pdrgpbsSubsets);
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PdrgpbsSubsets
//
//	@doc:
//		 Driver of subset generation
//
//---------------------------------------------------------------------------
CBitSetArray *
CJoinOrderDynProg::PdrgpbsSubsets
	(
	IMemoryPool *mp,
	CBitSet *pbs
	)
{
	const ULONG size = pbs->Size();
	ULONG *pulElems = GPOS_NEW_ARRAY(mp, ULONG, size);
	ULONG ul = 0;
	CBitSetIter bsi(*pbs);
	while (bsi.Advance())
	{
		pulElems[ul++] = bsi.Bit();
	}

	CBitSet *pbsCurrent = GPOS_NEW(mp) CBitSet(mp);
	CBitSetArray *pdrgpbsSubsets = GPOS_NEW(mp) CBitSetArray(mp);
	GenerateSubsets(mp, pbsCurrent, pulElems, size, 0, pdrgpbsSubsets);
	GPOS_DELETE_ARRAY(pulElems);

	return pdrgpbsSubsets;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDynProg::DCost
	(
	CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	CDouble *pd = m_phmexprcost->Find(pexpr);
	if (NULL != pd)
	{
		// stop recursion if cost was already cashed
		return *pd;
	}

	CDouble dCost(0.0);
	const ULONG arity = pexpr->Arity();
	if (0 == arity)
	{
		// leaf operator, use its estimated number of rows as cost
		dCost = CDouble(pexpr->Pstats()->Rows());
	}
	else
	{
		// inner join operator, sum-up cost of its children
		DOUBLE rgdRows[2] = {0.0,  0.0};
		for (ULONG ul = 0; ul < arity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			// call function recursively to find child cost
			dCost = dCost + DCost(pexprChild);
			DeriveStats(pexprChild);
			rgdRows[ul] = pexprChild->Pstats()->Rows().Get();
		}

		// add inner join local cost
		dCost = dCost + (rgdRows[0] + rgdRows[1]);
	}

	return dCost;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PbsCovered
//
//	@doc:
//		Return a subset of the given set covered by one or more edges
//
//---------------------------------------------------------------------------
CBitSet *
CJoinOrderDynProg::PbsCovered
	(
	CBitSet *pbsInput
	)
{
	GPOS_ASSERT(NULL != pbsInput);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (pbsInput->ContainsAll(pedge->m_pbs))
		{
			pbs->Union(pedge->m_pbs);
		}
	}

	return pbs;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprCross
//
//	@doc:
//		Generate cross product for the given components
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprCross
	(
	CBitSet *pbs
	)
{
	GPOS_ASSERT(NULL != pbs);

	CExpression *pexpr = PexprLookup(pbs);
	if (NULL != pexpr)
	{
		// join order is already created
		return pexpr;
	}

	CBitSetIter bsi(*pbs);
	(void) bsi.Advance();
	CExpression *pexprComp = m_rgpcomp[bsi.Bit()]->m_pexpr;
	pexprComp->AddRef();
	CExpression *pexprCross = pexprComp;
	while (bsi.Advance())
	{
		pexprComp =  m_rgpcomp[bsi.Bit()]->m_pexpr;
		pexprComp->AddRef();
		pexprCross = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprComp, pexprCross, CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/));
	}

	pbs->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif // GPOS_DEBUG
			m_phmbsexpr->Insert(pbs, pexprCross);
		GPOS_ASSERT(fInserted);

	return pexprCross;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprJoinCoveredSubsetWithUncoveredSubset
//
//	@doc:
//		Join a covered subset with uncovered subset
//
//---------------------------------------------------------------------------


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprBestJoinOrder
//
//	@doc:
//		find best join order for a given set of elements;
//
//---------------------------------------------------------------------------
CJoinOrderDynProg::SComponentArray*
CJoinOrderDynProg::PexprBestJoinOrder
	(
	CBitSet *pbs
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	GPOS_ASSERT(NULL != pbs);

	CBitSetIter outerbitSetIter(*pbs);
	
	SComponentArray *compArray = GPOS_NEW(m_mp) SComponentArray(m_mp);

	CAutoTrace at(m_mp);
//	CDouble minCost(0.0);
//	SComponent *pTemp = NULL;
	while (outerbitSetIter.Advance())
	{
		ULONG comp1id = outerbitSetIter.Bit();
		CBitSetIter innerbitSetIter(*pbs);
		SComponent *pTemp = NULL;
		CDouble minCost(0.0);
		while (innerbitSetIter.Advance())
		{
			ULONG comp2id = innerbitSetIter.Bit();
			if (comp1id < comp2id)
			{
				SComponent *comp1 = m_rgpcomp[comp1id];
				SComponent *comp2 = m_rgpcomp[comp2id];
				CJoinOrder::SComponent *compTemp = Join(comp1,comp2);
				if (compTemp == NULL)
				{
					continue;
				}
				if(CUtils::FCrossJoin(compTemp->m_pexpr))
				{
					compTemp->Release();
					continue;
				}
				CDouble cost = DCost(compTemp->m_pexpr);
				if (cost < minCost || minCost == 0.0)
				{
					CRefCount::SafeRelease(pTemp);
					pTemp = compTemp;

				}
				else
				{
					compTemp->Release();
				}
				
			}
		}
		if (pTemp != NULL)
			compArray->Append(pTemp);
	}

//	compArray->Append(pTemp);
	return compArray;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprBuildPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
	// collect edges connecting the given sets
	CBitSet *pbsEdges = GPOS_NEW(m_mp) CBitSet(m_mp);
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsFst);
	pbs->Union(pbsSnd);

	for (ULONG ul = 0; ul < m_ulEdges; ul++)
	{
		SEdge *pedge = m_rgpedge[ul];
		if (
			pbs->ContainsAll(pedge->m_pbs) &&
			!pbsFst->IsDisjoint(pedge->m_pbs) &&
			!pbsSnd->IsDisjoint(pedge->m_pbs)
			)
		{
#ifdef GPOS_DEBUG
		BOOL fSet =
#endif // GPOS_DEBUG
			pbsEdges->ExchangeSet(ul);
			GPOS_ASSERT(!fSet);
		}
	}
	pbs->Release();

	CExpression *pexprPred = NULL;
	if (0 < pbsEdges->Size())
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		CBitSetIter bsi(*pbsEdges);
		while (bsi.Advance())
		{
			ULONG ul = bsi.Bit();
			SEdge *pedge = m_rgpedge[ul];
			pedge->m_pexpr->AddRef();
			pdrgpexpr->Append(pedge->m_pexpr);
		}

		pexprPred = CPredicateUtils::PexprConjunction(m_mp, pdrgpexpr);
	}

	pbsEdges->Release();
	return pexprPred;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynProg::PexprExpand()
{
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp);
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		(void) pbs->ExchangeSet(ul);
	}
	SComponentArrays *pexprLevelArrays = GPOS_NEW(m_mp) SComponentArrays(m_mp);
	SComponentArray *pexprResult = PexprBestJoinOrder(pbs);
	pexprLevelArrays->Append(pexprResult);
	for (ULONG ul = 2; ul < m_ulComps; ul++)
	{
		SComponentArray *pexprLevelResult = PexprJoinOrder((*pexprLevelArrays)[ul-2]);
		if (ul > 2)
		{
			for (ULONG k = 2; k < ul; k++)
			{
				ULONG idx1 = k;
				ULONG idx2 = ul+1 - idx1;
				SComponentArray *arr1 = (*pexprLevelArrays)[idx1-2];
				SComponentArray *arr2 = (*pexprLevelArrays)[idx2-2];
				CDouble minCost(0.0);
				SComponent *pTemp = NULL;
				for (ULONG arr1size = 0; arr1size< arr1->Size(); arr1size++)
				{
					SComponent *p1 = (*arr1)[arr1size];
					for (ULONG arr2size = 0; arr2size< arr2->Size(); arr2size++)
					{
						SComponent *p2 = (*arr2)[arr2size];
						if (!p1->m_pbs->IsDisjoint(p2->m_pbs))
						{
							continue;
						}
						
						SComponent *pcomb = Join(p1, p2);
						if (pcomb == NULL)
						{
							continue;
						}
						if (CUtils::FCrossJoin(pcomb->m_pexpr))
						{
							pcomb->Release();
							continue;
						}
						
						CDouble cost = DCost(pcomb->m_pexpr);
						if (cost < minCost || minCost == 0.0)
						{
							CRefCount::SafeRelease(pTemp);
							pTemp = pcomb;
							
						}
						else
						{
							pcomb->Release();
						}
						
					}

				}
				if (pTemp != NULL)
					pexprLevelResult->Append(pTemp);
			}
		}
		pexprLevelArrays->Append(pexprLevelResult);
	}
	
#ifdef GPOS_DEBUG
	GPOS_ASSERT(pexprResult != NULL);
#endif
	
	GPOS_ASSERT(pexprResult != NULL);

	CExpression *pexprFinalOne = NULL;
	SComponentArray *pexprFinalLevel = (*pexprLevelArrays)[m_ulComps-2];
	CAutoTrace at(m_mp);
	for (ULONG ul = 0; ul < pexprFinalLevel->Size(); ul++)
	{
		CExpression *pexpr = (*pexprFinalLevel)[ul]->m_pexpr;
		CDouble dCost = DCost(pexpr);
		CExpression *pexprNorm = CNormalizer::PexprNormalize(m_mp, pexpr);
//		ULONG hashValue = CExpression::HashValue(pexprNorm);
		at.Os() << "Hash Value: " << dCost << std::endl;
		pexprNorm->DbgPrint();
		pexprNorm->Release();
		AddJoinOrder(pexpr, dCost);
		if (ul == pexprFinalLevel->Size()-1 )
		{
			pexprFinalOne = pexpr;
			pexprFinalOne->AddRef();
		}
	}
	pbs->Release();
	pexprLevelArrays->Release();
//	pexprResult->Release();
	return pexprFinalOne;
}

CJoinOrderDynProg::SComponentArray *
CJoinOrderDynProg::PexprJoinOrder
	(
	SComponentArray *inputCompArray
	)
{

//	SComponent *pTemp = NULL;
//	CDouble minCost (0.0);
	ULONG comp_array_size = inputCompArray->Size();
	SComponentArray *compArray = GPOS_NEW(m_mp) SComponentArray(m_mp);
	for (ULONG ul = 0; ul < comp_array_size ; ul++)
	{
		SComponent *pcomp = (*inputCompArray)[ul];
		SComponent *pTemp = NULL;
		CDouble minCost(0.0);
		
		for (ULONG id = 0; id < m_ulComps; id++)
		{
			SComponent *base_comp = m_rgpcomp[id];
			if (pcomp->m_pbs->ContainsAll(base_comp->m_pbs))
			{
				continue;
			}
			
			SComponent *pCompTemp = Join(pcomp, base_comp);
			if (pCompTemp == NULL)
			{
				continue;
			}
			if (CUtils::FCrossJoin(pCompTemp->m_pexpr))
			{
				pCompTemp->Release();
				continue;
			}
			
			
			CDouble cost = DCost(pCompTemp->m_pexpr);
			if (cost < minCost || minCost == 0.0)
			{
				CRefCount::SafeRelease(pTemp);
				pTemp = pCompTemp;

			}
			else
			{
				pCompTemp->Release();
			}
			
			
		}
		compArray->Append(pTemp);
	}
//	MarkUsedEdges(pTemp);
//	compArray->Append(pTemp);
	return compArray;
}

CJoinOrder::SComponent *
CJoinOrderDynProg::Join
(
 SComponent *comp1,
 SComponent *comp2
)
{

	CExpression *pexprPred = PexprBuildPred(comp1->m_pbs, comp2->m_pbs);
	
	if (pexprPred == NULL)
		return NULL;

	CExpression *pexprLeft = comp1->m_pexpr;
	CExpression *pexprRight = comp2->m_pexpr;
	pexprLeft->AddRef();
	pexprRight->AddRef();
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprLeft, pexprRight, pexprPred);
	
	CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp, *(comp1->m_pbs));
	pbs->Union(comp2->m_pbs);
	
	CBitSet *edge_set = GPOS_NEW(m_mp) CBitSet(m_mp);
	edge_set->Union(comp1->m_edge_set);
	edge_set->Union(comp2->m_edge_set);
	SComponent *pcomp = GPOS_NEW(m_mp) SComponent(pexprJoin, pbs, edge_set);
	
	return pcomp;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynProg::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDynProg::OsPrint
	(
	IOstream &os
	)
	const
{
	return os;
}

// EOF
