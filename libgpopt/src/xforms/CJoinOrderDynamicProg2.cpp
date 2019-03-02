//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynamicProg2.cpp
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
#include "gpopt/xforms/CJoinOrderDynamicProg2.h"

#include "gpopt/exception.h"

using namespace gpopt;

#define GPOPT_DP_JOIN_ORDERING_TOPK	20

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDP::SComponentPair::SComponentPair
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg2::SComponentPair::SComponentPair
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
//		CJoinOrderDynamicProg2::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDynamicProg2::SComponentPair::HashValue
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
//		CJoinOrderDynamicProg2::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDynamicProg2::SComponentPair::Equals
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
//		CJoinOrderDynamicProg2::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg2::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg2::CJoinOrderDynamicProg2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg2::CJoinOrderDynamicProg2
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
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg2::~CJoinOrderDynamicProg2
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProg2::~CJoinOrderDynamicProg2()
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
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg2::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg2::AddJoinOrder
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
//		CJoinOrderDynamicProg2::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg2::PexprLookup
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
//		CJoinOrderDynamicProg2::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg2::PexprPred
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
//		CJoinOrderDynamicProg2::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg2::DeriveStats
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
//		CJoinOrderDynamicProg2::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProg2::InsertExpressionCost
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
//		CJoinOrderDynamicProg2::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDynamicProg2::DCost
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
		dCost = (dCost + (rgdRows[0] + rgdRows[1]));
	}

	return dCost;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg2::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProg2::PexprBuildPred
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

CExpression *
CJoinOrderDynamicProg2::JoinComp
(
 CBitSet *pbsFirst,
 CBitSet *pbsSecond,
 BOOL allow_cross_joins
 )
{
//
	CExpression *pexprScalar = PexprPred(pbsFirst, pbsSecond);
	
	if (NULL == pexprScalar)
	{
		if (!allow_cross_joins)
			return NULL;
		pexprScalar = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
	}
	else
	{
		pexprScalar->AddRef();
	}

	CExpression *pexprLeft = PexprLookup(pbsFirst);
	CExpression *pexprRight = PexprLookup(pbsSecond);
	
	pexprLeft->AddRef();
	pexprRight->AddRef();
	
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, pexprLeft, pexprRight, pexprScalar);

	return pexprJoin;
}


void
CJoinOrderDynamicProg2::AddExprAlternativeToBitSetMap
	(
	CBitSet *pbs,
	CExpression *expr,
	BitSetToExpressionArrayMap *bitsetToExprArray
	)
{
	GPOS_ASSERT(NULL != pbs);
	GPOS_ASSERT(NULL != expr);
	
	expr->AddRef();
	CExpressionArray *existing_exprs = bitsetToExprArray->Find(pbs);
	if (existing_exprs)
	{
		existing_exprs->Append(expr);
	}
	else
	{
		CExpressionArray *exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		exprs->Append(expr);
		pbs->AddRef();
		bitsetToExprArray->Insert(pbs, exprs);
	}
}

CJoinOrderDynamicProg2::BitSetToExpressionArrayMap *
CJoinOrderDynamicProg2::SearchJoinOrder
	(
	 CBitSetArray *pbsFirst,
	 CBitSetArray *pbsSecond,
	 BOOL same_level,
	 BOOL allow_cross_joins
 )
{
	GPOS_ASSERT(pbsFirst);
	GPOS_ASSERT(pbsSecond);
	
	ULONG pbsFirstSize = pbsFirst->Size();
	ULONG pbsSecondSize = pbsSecond->Size();
	BitSetToExpressionArrayMap *bitsetToExprArray = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);

//	CAutoTrace at(m_mp);
	for (ULONG ul = 0; ul < pbsFirstSize; ul++)
	{
		CBitSet *pbsOuter = (*pbsFirst)[ul];

		CBitSet *pbsBest = NULL;
		CExpression *best_expr = NULL;
		CDouble minCost (0.0);
		CBitSet *pbsResult = GPOS_NEW(m_mp) CBitSet(m_mp, *pbsOuter);

		ULONG offset = 0;
		if (same_level)
			offset = ul+1;
	
		for (ULONG id = offset; id < pbsSecondSize; id++)
		{
			CBitSet *pbsInner = (*pbsSecond)[id];
			if (!pbsOuter->IsDisjoint(pbsInner))
			{
				continue;
			}
//			at.Os() << "Combination: " << *pbsOuter << ": " << *pbsInner << std::endl;
			CExpression *result_expr = JoinComp(pbsOuter, pbsInner, allow_cross_joins);
			if (result_expr == NULL && !allow_cross_joins)
				continue;
			
			CDouble dCost = DCost(result_expr);
			if (dCost < minCost || minCost == 0.0)
			{
				pbsInner->AddRef();
				CRefCount::SafeRelease(pbsBest);
				pbsBest = pbsInner;
				CRefCount::SafeRelease(best_expr);
				best_expr= result_expr;
				minCost = dCost;
			}
			else
			{
				result_expr->Release();
			}
		}
		
		if (NULL != pbsBest)
		{
			pbsResult->Union(pbsBest);
			
			AddExprAlternativeToBitSetMap(pbsResult, best_expr, bitsetToExprArray);
			InsertExpressionCost(best_expr, minCost, false);
			
			best_expr->Release();
		}
		pbsResult->Release();
		
		CRefCount::SafeRelease(pbsBest);

	}
	return bitsetToExprArray;
}

void
CJoinOrderDynamicProg2::AddExprArrayAlternativesToMap
(
	BitSetToExpressionArrayMap *final_map,
 	BitSetToExpressionArrayMap *mapToAdd
)
{
	if (NULL == mapToAdd)
		return;

	BitSetToExpressionArrayMapIter iter2(mapToAdd);
	while (iter2.Advance())
	{
		const CBitSet *pbs = iter2.Key();
		CExpressionArray *expr_array = final_map->Find(pbs);
		const CExpressionArray *exprs = iter2.Value();
		if (NULL != expr_array)
		{
			for (ULONG ul = 0; ul < exprs->Size(); ul++)
			{
				CExpression *pexpr = (*exprs)[ul];
				pexpr->AddRef();
				expr_array->Append(pexpr);
			}
		}
		else
		{
			CBitSet *pbsNew = GPOS_NEW(m_mp) CBitSet(m_mp, *pbs);
			const CExpressionArray *expr_array = iter2.Value();
			CExpressionArray *result_array = GPOS_NEW(m_mp) CExpressionArray(m_mp);
			for (ULONG ul = 0; ul < expr_array->Size(); ul++)
			{
				CExpression *pexpr = (*expr_array)[ul];
				pexpr->AddRef();
				result_array->Append(pexpr);
			}
			final_map->Insert(pbsNew, result_array);
		}
	}
}

CJoinOrderDynamicProg2::BitSetToExpressionArrayMap *
CJoinOrderDynamicProg2::MergeAlternatives
	(
	 BitSetToExpressionArrayMap *map_a,
	 BitSetToExpressionArrayMap *map_b
	 )
{
	BitSetToExpressionArrayMap *result = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);
	AddExprArrayAlternativesToMap(result, map_a);
	AddExprArrayAlternativesToMap(result, map_b);
	return result;
}

void
CJoinOrderDynamicProg2::AddExprFromMap
	(
	BitSetToExpressionArrayMap *bit_expr_map
	)
{
	BitSetToExpressionArrayMapIter iter(bit_expr_map);

	while (iter.Advance())
	{
		const CExpressionArray *childs = iter.Value();
		for (ULONG ul = 0; ul < childs->Size(); ul++)
		{
			CExpression *pexpr = (*childs)[ul];
			CDouble dCost = DCost(pexpr);
			AddJoinOrder(pexpr, dCost);
		}
	}
}

CBitSetArray *
CJoinOrderDynamicProg2::GetThisLevelArray
	(
	BitSetToExpressionMap *cheapest_map
	)
{
	BitSetToExpressionMapIter iter(cheapest_map);
	CBitSetArray *array = GPOS_NEW(m_mp) CBitSetArray(m_mp);
	while (iter.Advance())
	{
		const CBitSet *pbs = iter.Key();
		CAutoTrace at(m_mp);
		
		CBitSet *pbsNew = GPOS_NEW(m_mp) CBitSet(m_mp, *pbs);
		
		const CExpression *best_expr = iter.Value();
		COperator *popLogical = best_expr->Pop();
		CExpressionArray *childs = best_expr->PdrgPexpr();
		childs->AddRef();
		popLogical->AddRef();
		CExpression *new_expr = GPOS_NEW(m_mp) CExpression(m_mp, popLogical, childs);
		
		array->Append(pbsNew);
		pbsNew->AddRef();
		m_phmbsexpr->Insert(pbsNew, new_expr);
	}
	return array;
}

CJoinOrderDynamicProg2::BitSetToExpressionArrayMap *
CJoinOrderDynamicProg2::GetBushyMaps
	(
	ULONG level,
	CBitSetArrays *join_levels
	)
{
	BitSetToExpressionArrayMap *bushy_maps = NULL;
	if (level > 2)
	{
		for (ULONG k = 2; k < level; k++)
		{
			ULONG comps_idx = k - 1;
			ULONG other_comps_idx = level - comps_idx - 1;
			if (comps_idx > other_comps_idx)
				break;
			CBitSetArray *comps = (*join_levels)[comps_idx];
			CBitSetArray *other_comps = (*join_levels)[other_comps_idx];
			BitSetToExpressionArrayMap *bushy_join_comps = SearchJoinOrder(comps, other_comps, comps_idx == other_comps_idx);
			BitSetToExpressionArrayMap *interim_map = bushy_maps;
			bushy_maps = MergeAlternatives(bushy_join_comps, interim_map);
			CRefCount::SafeRelease(interim_map);
			bushy_join_comps->Release();
		}
	}
	return bushy_maps;
}

CExpression *
CJoinOrderDynamicProg2::PexprExpand()
{
	CBitSetArray *base_comps = GPOS_NEW(m_mp) CBitSetArray(m_mp);
	CBitSetArrays *join_levels = GPOS_NEW(m_mp) CBitSetArrays(m_mp);
	for (ULONG ul = 0; ul < m_ulComps; ul++)
	{
		CBitSet *pbs = GPOS_NEW(m_mp) CBitSet(m_mp);
		pbs->ExchangeSet(ul);
		base_comps->Append(pbs);
	}
	join_levels->Append(base_comps);

	for (ULONG level = 1; level < m_ulComps; level++)
	{
		ULONG previous_level = level - 1;
		CBitSetArray *prev_lev_comps = (*join_levels)[previous_level];
		BOOL base_level = previous_level == 0 ? true: false;
		BitSetToExpressionArrayMap *bit_exprarray_map = SearchJoinOrder(prev_lev_comps, base_comps, base_level);
		BitSetToExpressionArrayMap *bushy_maps = GetBushyMaps(level, join_levels);
		if (bit_exprarray_map->Size() == 0 && bushy_maps->Size() == 0)
		{
			bit_exprarray_map->Release();
			bit_exprarray_map = SearchJoinOrder(prev_lev_comps, base_comps, base_level, true /* allow_cross_joins */);
		}

		BitSetToExpressionArrayMap *all_maps = MergeAlternatives(bit_exprarray_map, bushy_maps);
		if (level == m_ulComps - 1)
		{
			AddExprFromMap(all_maps);
		}
		else
		{
			BitSetToExpressionMap *cheapest_map = GetCheapest(all_maps);
			CBitSetArray *this_level_array = GetThisLevelArray(cheapest_map);
			cheapest_map->Release();
			join_levels->Append(this_level_array);
		}
		all_maps->Release();
		CRefCount::SafeRelease(bushy_maps);
		bit_exprarray_map->Release();
	}
	
	join_levels->Release();
	return NULL;
}



CJoinOrderDynamicProg2::BitSetToExpressionMap*
CJoinOrderDynamicProg2::GetCheapest
	(
	BitSetToExpressionArrayMap *bit_exprarray_map
	)
{
	BitSetToExpressionMap *cheapest_map = GPOS_NEW(m_mp) BitSetToExpressionMap(m_mp);
	BitSetToExpressionArrayMapIter iter(bit_exprarray_map);
	CAutoTrace at(m_mp);
	while (iter.Advance())
	{
		const CBitSet *pbs = iter.Key();
		const CExpressionArray *exprs = iter.Value();
		CDouble minCost(0.0);
		CExpression *best_expr = NULL;
		for (ULONG ul = 0; ul < exprs->Size(); ul++)
		{
			CExpression *expr = (*exprs)[ul];
			CDouble dCost = DCost(expr);
			if (minCost == 0.0 || dCost < minCost)
			{
				best_expr = expr;
				minCost = dCost;
			}
		}
		CBitSet *pbsNew = GPOS_NEW(m_mp) CBitSet(m_mp, *pbs);
		best_expr->AddRef();
		cheapest_map->Insert(pbsNew, best_expr);
	}
	return cheapest_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProg2::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDynamicProg2::OsPrint
	(
	IOstream &os
	)
	const
{
	return os;
}

// EOF
