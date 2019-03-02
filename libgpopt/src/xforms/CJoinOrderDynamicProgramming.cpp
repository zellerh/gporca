//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynamicProgramming.cpp
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
#include "gpopt/xforms/CJoinOrderDynamicProgramming.h"

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
CJoinOrderDynamicProgramming::SComponentPair::SComponentPair
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
//		CJoinOrderDynamicProgramming::SComponentPair::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CJoinOrderDynamicProgramming::SComponentPair::HashValue
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
//		CJoinOrderDynamicProgramming::SComponentPair::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CJoinOrderDynamicProgramming::SComponentPair::Equals
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
//		CJoinOrderDynamicProgramming::SComponentPair::~SComponentPair
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProgramming::SComponentPair::~SComponentPair()
{
	m_pbsFst->Release();
	m_pbsSnd->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProgramming::CJoinOrderDynamicProgramming
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProgramming::CJoinOrderDynamicProgramming
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
//		CJoinOrderDynamicProgramming::~CJoinOrderDynamicProgramming
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDynamicProgramming::~CJoinOrderDynamicProgramming()
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
//		CJoinOrderDynamicProgramming::AddJoinOrder
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProgramming::AddJoinOrder
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
//		CJoinOrderDynamicProgramming::PexprLookup
//
//	@doc:
//		Lookup best join order for given set
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProgramming::PexprLookup
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
//		CJoinOrderDynamicProgramming::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProgramming::PexprPred
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
//		CJoinOrderDynamicProgramming::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProgramming::DeriveStats
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
//		CJoinOrderDynamicProgramming::InsertExpressionCost
//
//	@doc:
//		Add expression to cost map
//
//---------------------------------------------------------------------------
void
CJoinOrderDynamicProgramming::InsertExpressionCost
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
//		CJoinOrderDynamicProgramming::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		cost of a join expression is the summation of the costs of its
//		children plus its local cost;
//		cost of a leaf expression is the estimated number of rows
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDynamicProgramming::DCost
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
//		CJoinOrderDynamicProgramming::PexprBuildPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDynamicProgramming::PexprBuildPred
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
CJoinOrderDynamicProgramming::GetJoinExpr
	(
	CBitSet *left_child,
	CBitSet *right_child,
	BOOL allow_cross_joins
	)
{
	CExpression *scalar_expr = PexprPred(left_child, right_child);

	if (NULL == scalar_expr)
	{
		if (!allow_cross_joins)
			return NULL;

		scalar_expr = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
	}
	else
	{
		scalar_expr->AddRef();
	}

	CExpression *left_expr = PexprLookup(left_child);
	CExpression *right_expr = PexprLookup(right_child);

	left_expr->AddRef();
	right_expr->AddRef();

	CExpression *join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, left_expr, right_expr, scalar_expr);

	return join_expr;
}


void
CJoinOrderDynamicProgramming::AddJoinExprAlternativeForBitSet
	(
	CBitSet *join_bitset,
	CExpression *join_expr,
	BitSetToExpressionArrayMap *map
	)
{
	GPOS_ASSERT(NULL != join_bitset);
	GPOS_ASSERT(NULL != join_expr);

	join_expr->AddRef();
	CExpressionArray *existing_join_exprs = map->Find(join_bitset);
	if (NULL != existing_join_exprs)
	{
		existing_join_exprs->Append(join_expr);
	}
	else
	{
		CExpressionArray *exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		exprs->Append(join_expr);
		join_bitset->AddRef();
		map->Insert(join_bitset, exprs);
	}
}

CJoinOrderDynamicProgramming::BitSetToExpressionArrayMap *
CJoinOrderDynamicProgramming::SearchJoinOrders
	(
	CBitSetArray *join_pair_bitsets,
	CBitSetArray *other_join_pair_bitsets,
	BOOL same_level_join_pairs,
	BOOL allow_cross_joins
	)
{
	GPOS_ASSERT(join_pairs_bitsets);
	GPOS_ASSERT(other_join_pairs_bitsets);

	ULONG join_pairs_size = join_pair_bitsets->Size();
	ULONG other_join_pairs_size = other_join_pair_bitsets->Size();
	BitSetToExpressionArrayMap *join_pairs_map = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);

	for (ULONG join_pair_id = 0; join_pair_id < join_pairs_size; join_pair_id++)
	{
		CBitSet *left_bitset = (*join_pair_bitsets)[join_pair_id];

		CBitSet *best_right_bitset = NULL;
		CExpression *best_join_expr = NULL;
		CDouble min_join_cost (0.0);
		CBitSet *best_join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		ULONG other_pair_start_id = 0;
		if (same_level_join_pairs)
			other_pair_start_id = join_pair_id + 1;

		for (ULONG other_pair_id = other_pair_start_id; other_pair_id < other_join_pairs_size; other_pair_id++)
		{
			CBitSet *right_bitset = (*other_join_pair_bitsets)[other_pair_id];
			if (!left_bitset->IsDisjoint(right_bitset))
			{
				continue;
			}

			CExpression *join_expr = GetJoinExpr(left_bitset, right_bitset, allow_cross_joins);
			if (join_expr == NULL && !allow_cross_joins)
				continue;

			CDouble join_cost = DCost(join_expr);
			if (join_cost < min_join_cost || min_join_cost == 0.0)
			{
				right_bitset->AddRef();
				CRefCount::SafeRelease(best_right_bitset);
				best_right_bitset = right_bitset;
				CRefCount::SafeRelease(best_join_expr);
				best_join_expr = join_expr;
				min_join_cost = join_cost;
			}
			else
			{
				join_expr->Release();
			}
		}
		
		if (NULL != best_right_bitset)
		{
			best_join_bitset->Union(best_right_bitset);
			
			AddJoinExprAlternativeForBitSet(best_join_bitset, best_join_expr, join_pairs_map);
			InsertExpressionCost(best_join_expr, min_join_cost, false);
			
			best_join_expr->Release();
		}
		best_join_bitset->Release();
		
		CRefCount::SafeRelease(best_right_bitset);

	}
	return join_pairs_map;
}

void
CJoinOrderDynamicProgramming::AddJoinExprsForBitSet
	(
	BitSetToExpressionArrayMap *result_map,
 	BitSetToExpressionArrayMap *candidate_map
	)
{
	if (NULL == candidate_map)
		return;

	BitSetToExpressionArrayMapIter iter(candidate_map);
	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		CExpressionArray *existing_join_exprs = result_map->Find(join_bitset);
		const CExpressionArray *candidate_join_exprs = iter.Value();
		if (NULL == existing_join_exprs)
		{
			CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
			const CExpressionArray *candidate_join_exprs = iter.Value();
			CExpressionArray *join_exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
			for (ULONG ul = 0; ul < candidate_join_exprs->Size(); ul++)
			{
				CExpression *join_expr = (*candidate_join_exprs)[ul];
				join_expr->AddRef();
				join_exprs->Append(join_expr);
			}
			result_map->Insert(join_bitset_entry, join_exprs);
		}
		else
		{
			for (ULONG id = 0; id < candidate_join_exprs->Size(); id++)
			{
				CExpression *join_expr = (*candidate_join_exprs)[id];
				join_expr->AddRef();
				existing_join_exprs->Append(join_expr);
			}
		}
	}
}

CJoinOrderDynamicProgramming::BitSetToExpressionArrayMap *
CJoinOrderDynamicProgramming::MergeJoinExprsForBitSet
	(
	BitSetToExpressionArrayMap *map,
	BitSetToExpressionArrayMap *other_map
	)
{
	BitSetToExpressionArrayMap *result_map = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);
	AddJoinExprsForBitSet(result_map, map);
	AddJoinExprsForBitSet(result_map, other_map);
	return result_map;
}

void
CJoinOrderDynamicProgramming::AddJoinExprFromMap
	(
	BitSetToExpressionArrayMap *bitset_joinexpr_map
	)
{
	BitSetToExpressionArrayMapIter iter(bitset_joinexpr_map);

	while (iter.Advance())
	{
		const CExpressionArray *join_exprs = iter.Value();
		for (ULONG id = 0; id < join_exprs->Size(); id++)
		{
			CExpression *join_expr = (*join_exprs)[id];
			CDouble join_cost = DCost(join_expr);
			AddJoinOrder(join_expr, join_cost);
		}
	}
}

CBitSetArray *
CJoinOrderDynamicProgramming::GetJoinExprBitSets
	(
	BitSetToExpressionMap *join_expr_map
	)
{
	BitSetToExpressionMapIter iter(join_expr_map);
	CBitSetArray *join_bitsets = GPOS_NEW(m_mp) CBitSetArray(m_mp);
	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);

		const CExpression *join_expr = iter.Value();
		
		COperator *join_op = join_expr->Pop();
		CExpressionArray *join_child_exprs = join_expr->PdrgPexpr();
		join_child_exprs->AddRef();
		join_op->AddRef();
		CExpression *join_expr_entry = GPOS_NEW(m_mp) CExpression(m_mp, join_op, join_child_exprs);
		
		join_bitsets->Append(join_bitset_entry);
		join_bitset_entry->AddRef();
		m_phmbsexpr->Insert(join_bitset_entry, join_expr_entry);
	}
	return join_bitsets;
}

CJoinOrderDynamicProgramming::BitSetToExpressionArrayMap *
CJoinOrderDynamicProgramming::SearchBushyJoinOrders
	(
	ULONG current_level,
	CBitSetArrays *join_levels
	)
{
	BitSetToExpressionArrayMap *final_bushy_join_exprs_map = NULL;
	if (current_level > 2)
	{
		for (ULONG k = 2; k < current_level; k++)
		{
			ULONG join_level = k - 1;
			ULONG other_join_level = current_level - join_level - 1;
			if (join_level > other_join_level)
				break;
			CBitSetArray *join_bitsets = (*join_levels)[join_level];
			CBitSetArray *other_join_bitsets = (*join_levels)[other_join_level];
			BitSetToExpressionArrayMap *bitset_bushy_join_exprs_map = SearchJoinOrders(join_bitsets, other_join_bitsets, join_level == other_join_level);
			BitSetToExpressionArrayMap *interim_map = final_bushy_join_exprs_map;
			final_bushy_join_exprs_map = MergeJoinExprsForBitSet(bitset_bushy_join_exprs_map, interim_map);
			CRefCount::SafeRelease(interim_map);
			bitset_bushy_join_exprs_map->Release();
		}
	}
	return final_bushy_join_exprs_map;
}

CExpression *
CJoinOrderDynamicProgramming::PexprExpand()
{
	CBitSetArray *base_relations_bitsets = GPOS_NEW(m_mp) CBitSetArray(m_mp);
	CBitSetArrays *join_level_bitsets = GPOS_NEW(m_mp) CBitSetArrays(m_mp);
	for (ULONG relation_id = 0; relation_id < m_ulComps; relation_id++)
	{
		CBitSet *base_relation_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		base_relation_bitset->ExchangeSet(relation_id);
		base_relations_bitsets->Append(base_relation_bitset);
	}
	join_level_bitsets->Append(base_relations_bitsets);

	for (ULONG current_join_level = 1; current_join_level < m_ulComps; current_join_level++)
	{
		ULONG previous_level = current_join_level - 1;
		CBitSetArray *prev_lev_comps = (*join_level_bitsets)[previous_level];
		BOOL base_relations_level = previous_level == 0 ? true: false;
		BitSetToExpressionArrayMap *bitset_join_exprs_map = SearchJoinOrders(prev_lev_comps, base_relations_bitsets, base_relations_level);
		BitSetToExpressionArrayMap *bitset_bushy_join_exprs_map = SearchBushyJoinOrders(current_join_level, join_level_bitsets);
		if (bitset_join_exprs_map->Size() == 0 && NULL == bitset_bushy_join_exprs_map)
		{
			bitset_join_exprs_map->Release();
			bitset_join_exprs_map = SearchJoinOrders(prev_lev_comps, base_relations_bitsets, base_relations_level, true /* allow_cross_joins */);
		}

		BitSetToExpressionArrayMap *all_join_exprs_map = MergeJoinExprsForBitSet(bitset_join_exprs_map, bitset_bushy_join_exprs_map);
		if (current_join_level == m_ulComps - 1)
		{
			AddJoinExprFromMap(all_join_exprs_map);
		}
		else
		{
			BitSetToExpressionMap *cheapest_bitset_join_expr_map = GetCheapestJoinExprForBitSet(all_join_exprs_map);
			CBitSetArray *join_expr_bitsets = GetJoinExprBitSets(cheapest_bitset_join_expr_map);
			cheapest_bitset_join_expr_map->Release();
			join_level_bitsets->Append(join_expr_bitsets);
		}
		all_join_exprs_map->Release();
		CRefCount::SafeRelease(bitset_bushy_join_exprs_map);
		bitset_join_exprs_map->Release();
	}
	
	join_level_bitsets->Release();
	return NULL;
}



CJoinOrderDynamicProgramming::BitSetToExpressionMap *
CJoinOrderDynamicProgramming::GetCheapestJoinExprForBitSet
	(
	BitSetToExpressionArrayMap *bitset_exprs_map
	)
{
	BitSetToExpressionMap *cheapest_join_map = GPOS_NEW(m_mp) BitSetToExpressionMap(m_mp);
	BitSetToExpressionArrayMapIter iter(bitset_exprs_map);
	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		const CExpressionArray *join_exprs = iter.Value();
		CDouble min_join_cost(0.0);
		CExpression *best_join_expr = NULL;
		for (ULONG id = 0; id < join_exprs->Size(); id++)
		{
			CExpression *join_expr = (*join_exprs)[id];
			CDouble join_cost = DCost(join_expr);
			if (min_join_cost == 0.0 || join_cost < min_join_cost)
			{
				best_join_expr = join_expr;
				min_join_cost = join_cost;
			}
		}
		CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
		best_join_expr->AddRef();
		cheapest_join_map->Insert(join_bitset_entry, best_join_expr);
	}
	return cheapest_join_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDynamicProgramming::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDynamicProgramming::OsPrint
	(
	IOstream &os
	)
	const
{
	return os;
}

// EOF
