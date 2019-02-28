//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynamicProg.cpp
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
#include "gpopt/xforms/CJoinOrderDynamicProg.h"

#include "gpopt/exception.h"

using namespace gpopt;

#define GPOPT_DP_JOIN_ORDERING_TOPK	10

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg::SComponentPair::SComponentPair
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
//		CJoinOrderDynamicProg::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDynamicProg::SComponentPair::HashValue
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
//		CJoinOrderDynamicProg::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDynamicProg::SComponentPair::Equals
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
//		CJoinOrderDynamicProg::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg::CJoinOrderDynamicProg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg::CJoinOrderDynamicProg
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
//		CJoinOrderDynamicProg::~CJoinOrderDynamicProg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg::~CJoinOrderDynamicProg()
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
//		CJoinOrderDynamicProg::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg::AddJoinOrder
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
//		CJoinOrderDynamicProg::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprLookup
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
//		CJoinOrderDynamicProg::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprPred
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
//		CJoinOrderDynamicProg::PexprJoin
//
//	@doc:
//		Join expressions in the given two sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprJoin
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
//		CJoinOrderDynamicProg::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg::DeriveStats
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
//		CJoinOrderDynamicProg::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg::InsertExpressionCost
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
//		CJoinOrderDynamicProg::PexprJoin
//
//	@doc:
//		Join expressions in the given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprJoin
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
//		CJoinOrderDynamicProg::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDynamicProg::DCost
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
        DeriveStats(pexpr);
        dCost = dCost + (rgdRows[0] + rgdRows[1]);
    }

	return dCost;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg::PbsCovered
//
//	@doc:
//		Return a subset of the given set covered by one or more edges
//
//---------------------------------------------------------------------------
CBitSet *
CJoinOrderDynamicProg::PbsCovered
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
//		CJoinOrderDynamicProg::PexprCross
//
//	@doc:
//		Generate cross product for the given components
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprCross
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
//		CJoinOrderDynamicProg::PexprJoinCoveredSubsetWithUncoveredSubset
//
//	@doc:
//		Join a covered subset with uncovered subset
//
//---------------------------------------------------------------------------


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg::PexprBuildPred
	(
	CBitSet *pbsFst,
	CBitSet *pbsSnd
	)
{
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
			pexprPred->AddRef();
			return pexprPred;
		}
		
		// try again after swapping sets
		if (0 == ul)
		{
			pcomppair->Release();
			std::swap(pbsFst, pbsSnd);
		}
	}

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
	if (pexprPred == NULL)
	{
		pexprPred = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
	}
	pexprPred->AddRef();
	m_phmcomplink->Insert(pcomppair, pexprPred);
	return pexprPred;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg::PexprExpand
//
//	@doc:
//		Create join order
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg::PexprExpand()
{
	SComponentArrays *join_comp_all_levels = GPOS_NEW(m_mp) SComponentArrays(m_mp);
	SComponentArray *base_comps = GPOS_NEW(m_mp) SComponentArray(m_mp);
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		SComponent *base_comp = m_rgpcomp[ul];
		base_comp->AddRef();
		base_comps->Append(base_comp);
	}
	join_comp_all_levels->Append(base_comps);

	for (ULONG current_join_level = 1; current_join_level < m_ulComps; current_join_level++)
	{
		ULONG prev_level = current_join_level - 1;
		BOOL avoid_duplicates = prev_level == 0 ? true: false;
		SComponentArray *comps_array = (*join_comp_all_levels)[prev_level];
		SComponentArray *current_lvl_join_comps = GetJoinCompArray(comps_array, base_comps, avoid_duplicates);
        if (current_join_level > 2)
        {
            for (ULONG k = 2; k < current_join_level; k++)
            {
                ULONG comps_idx = k - 1;
                ULONG other_comps_idx = current_join_level - comps_idx - 1;
                if (comps_idx > other_comps_idx)
                    break;
                SComponentArray *comps = (*join_comp_all_levels)[comps_idx];
                SComponentArray *other_comps = (*join_comp_all_levels)[other_comps_idx];
                SComponentArray *bushy_join_comps = GetJoinCompArray(comps, other_comps, comps_idx == other_comps_idx);
                for (ULONG ul = 0; ul < bushy_join_comps->Size(); ul++)
                {
                    SComponent *bushy_join_comp = (*bushy_join_comps)[ul];
                    bushy_join_comp->AddRef();
                    current_lvl_join_comps->Append(bushy_join_comp);
                }
                bushy_join_comps->Release();
            }
        }
		join_comp_all_levels->Append(current_lvl_join_comps);
	}

	SComponentArray *pexprFinalLevel = (*join_comp_all_levels)[m_ulComps-1];
	for (ULONG ul = 0; ul < pexprFinalLevel->Size(); ul++)
	{
		CExpression *pexpr = (*pexprFinalLevel)[ul]->m_pexpr;
		CDouble dCost = DCost(pexpr);
		AddJoinOrder(pexpr, dCost);
	}
	join_comp_all_levels->Release();
}

CJoinOrderDynamicProg::SComponentArray *
CJoinOrderDynamicProg::GetJoinCompArray
	(
	SComponentArray *prev_lvl_comps,
	SComponentArray *other_comps,
	BOOL same_level
	)
{
	ULONG comps_size = prev_lvl_comps->Size();
	ULONG other_comps_size = other_comps->Size();
	SComponentArray *result_comps = GPOS_NEW(m_mp) SComponentArray(m_mp);
	for (ULONG ul = 0; ul < comps_size ; ul++)
	{
		SComponent *comp = (*prev_lvl_comps)[ul];
		SComponent *best_join_comp = NULL;
		CDouble min_cost(0.0);
		SComponent *best_cross_join_comp = NULL;
		CDouble min_cross_cost(0.0);
		ULONG offset = 0;
		if (same_level)
			offset = ul+1;
		
		for (ULONG id = offset; id < other_comps_size; id++)
		{
			SComponent *other_comp = (*other_comps)[id];
			if (!comp->m_pbs->IsDisjoint(other_comp->m_pbs))
			{
				continue;
			}
			
			SComponent *join_comp = JoinComp(comp, other_comp);
			CDouble cost = DCost(join_comp->m_pexpr);
			BOOL is_cross_join = CUtils::FCrossJoin(join_comp->m_pexpr);
			if (!is_cross_join && (cost < min_cost || min_cost == 0.0))
			{
				CRefCount::SafeRelease(best_join_comp);
				best_join_comp = join_comp;
				min_cost = cost;
			}
			else if (is_cross_join && (cost < min_cross_cost || min_cross_cost == 0.0))
			{
				CRefCount::SafeRelease(best_cross_join_comp);
				best_cross_join_comp = join_comp;
				min_cross_cost = cost;
			}
			else
			{
				join_comp->Release();
			}
		}
		if (NULL != best_join_comp)
		{
            InsertExpressionCost(best_join_comp->m_pexpr, min_cost, false);
			best_join_comp->AddRef();
			result_comps->Append(best_join_comp);
		}
		else if (NULL != best_cross_join_comp)
		{
            InsertExpressionCost(best_cross_join_comp->m_pexpr, min_cross_cost, false);
			best_cross_join_comp->AddRef();
			result_comps->Append(best_cross_join_comp);
		}
		CRefCount::SafeRelease(best_cross_join_comp);
		CRefCount::SafeRelease(best_join_comp);
	}
	return result_comps;
}

CJoinOrder::SComponent *
CJoinOrderDynamicProg::JoinComp
(
 SComponent *comp1,
 SComponent *comp2
)
{
	CExpression *pexprPred = PexprBuildPred(comp1->m_pbs, comp2->m_pbs);
    CAutoTrace at(m_mp);

	CExpression *pexprLeft = comp1->m_pexpr;
	CExpression *pexprRight = comp2->m_pexpr;
	pexprLeft->AddRef();
	pexprRight->AddRef();
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprLeft, pexprRight, pexprPred);
    CDouble dcost = DCost(pexprJoin);
    at.Os() << "Combination: {" << *(comp1->m_pbs) << "--" << *(comp2->m_pbs) << "}" << "Cost: " << dcost.Get() << std::endl;
    at.Os() << "Left Expr:" << (*pexprLeft) << std::endl;
    at.Os() << "Right Expr:" << (*pexprRight) << std::endl;

	
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
//		CJoinOrderDynamicProg::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDynamicProg::OsPrint
	(
	IOstream &os
	)
	const
{
	return os;
}

// EOF
