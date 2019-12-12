//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.cpp
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
#include "gpopt/operators/CScalarNAryJoinPredList.h"
#include "gpopt/xforms/CJoinOrderDPv2.h"

#include "gpopt/exception.h"

using namespace gpopt;

// Communitivity will generate an additional 5 alternatives. This value should be 1/2
// of GPOPT_DP_JOIN_ORDERING_TOPK to generate equivalent alternatives as the DP xform
#define GPOPT_DP_JOIN_ORDERING_TOPK	5
#define GPOPT_DP_STOP_BUSHY_TREES_AT_LEVEL 12
#define GPOPT_DP_BUSHY_TREE_BASE 2.0
#define GPOPT_DP_START_GREEDY_AT_LEVEL 14
#define GPOPT_DP_GREEDY_BASE 2.0

CJoinOrderDPv2::KHeap::KHeap(CMemoryPool *mp, CJoinOrderDPv2 *join_order, ULONG k)
:
m_join_order(join_order),
m_topk(NULL),
m_mp(mp),
m_k(k),
m_size(0),
m_highest_cost(0)
{
	m_bitSetExprArrayMap = GPOS_NEW(m_mp) BitSetToExpressionArrayMap(m_mp);
}


CJoinOrderDPv2::KHeap::~KHeap()
{
	m_bitSetExprArrayMap->Release();
	CRefCount::SafeRelease(m_topk);
}


void CJoinOrderDPv2::KHeap::BuildTopK()
{
	m_topk = GPOS_NEW(m_mp) ComponentInfoArray(m_mp);
	BitSetToExpressionArrayMapIter iter(m_bitSetExprArrayMap);

	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.Key();
		const CExpressionArray *join_exprs = iter.Value();

		for (ULONG id = 0; id < join_exprs->Size(); id++)
		{
			CExpression *join_expr = (*join_exprs)[id];
			CDouble join_cost = m_join_order->DCost(join_expr);

			CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
			join_expr->AddRef();

			SComponentInfo *component_info = GPOS_NEW(m_mp) SComponentInfo(join_bitset_entry, join_expr, join_cost);

			m_topk->Append(component_info);
			if (join_cost > m_highest_cost)
			{
				m_highest_cost = join_cost;
			}
		}
	}
}


ULONG CJoinOrderDPv2::KHeap::EvictMostExpensiveEntry()
{
	ULONG result = m_topk->Size();
	CDouble secondHighest = 0;

	for (ULONG ul = 0; ul < m_topk->Size(); ul++)
	{
		if (m_highest_cost <= (*m_topk)[ul]->cost && result == m_topk->Size())
		{
			result = ul;
		}
		else if ((*m_topk)[ul]->cost > secondHighest)
		{
			secondHighest = (*m_topk)[ul]->cost;
		}
	}

	m_highest_cost = secondHighest;
	SComponentInfo *ci = (*m_topk)[result];
	ci->best_expr->Release();
	ci->component->Release();
	ci->best_expr = NULL;
	ci->component = NULL;

	return result;
}


CExpressionArray *CJoinOrderDPv2::KHeap::ArrayForBitset(const CBitSet *bit_set)
{
	CExpressionArray *exprArray =  m_bitSetExprArrayMap->Find(bit_set);
	return exprArray;
}


BOOL CJoinOrderDPv2::KHeap::Insert(CBitSet *join_bitset, CExpression *join_expr)
{
	GPOS_ASSERT(NULL != join_bitset);
	GPOS_ASSERT(NULL != join_expr);

	m_size++;
	if (m_size > m_k)
	{
		if (NULL == m_topk)
		{
			BuildTopK();
		}

		CDouble join_cost = m_join_order->DCost(join_expr);

		if (join_cost >= m_highest_cost)
		{
			// the new expression is too expensive, it is not in the top K
			return false;
		}

		// evict the most expensive entry and replace it with the new one, join_expr
		ULONG evicted_index = EvictMostExpensiveEntry();

		GPOS_ASSERT(evicted_index < m_topk->Size());

		join_bitset->AddRef();
		join_expr->AddRef();
		(*m_topk)[evicted_index]->best_expr = join_expr;
		(*m_topk)[evicted_index]->component = join_bitset;
		(*m_topk)[evicted_index]->cost = join_cost;

	}

	// insert the new expression into the BitSetToExpressionArrayMap
	join_expr->AddRef();
	CExpressionArray *existing_join_exprs = m_bitSetExprArrayMap->Find(join_bitset);
	if (NULL != existing_join_exprs)
	{
		existing_join_exprs->Append(join_expr);
	}
	else
	{
		CExpressionArray *exprs = GPOS_NEW(m_mp) CExpressionArray(m_mp);
		exprs->Append(join_expr);
		join_bitset->AddRef();
		BOOL success = m_bitSetExprArrayMap->Insert(join_bitset, exprs);
		if (!success)
			GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedPred);
	}

	return true;
}


CJoinOrderDPv2::KHeapIterator::KHeapIterator(KHeap *kHeap)
:
m_kheap(kHeap),
m_iter(kHeap->BSExpressionArrayMap()),
m_entry_in_expression_array(-1),
m_entry_in_topk_array(-1)
{
}


BOOL CJoinOrderDPv2::KHeapIterator::Advance()
{
	if (m_kheap->HasTopK())
	{
		m_entry_in_topk_array++;
		return m_entry_in_topk_array < m_kheap->m_topk->Size();
	}

	m_entry_in_expression_array++ ;

	if (0 == m_entry_in_expression_array)
	{
		return m_iter.Advance();
	}

	if (m_entry_in_expression_array >= m_iter.Value()->Size())
	{
		m_entry_in_expression_array = 0;
		return m_iter.Advance();
	}

	return true;
}


const CBitSet *CJoinOrderDPv2::KHeapIterator::BitSet()
{
	if (m_kheap->HasTopK())
	{
		return (*(m_kheap->m_topk))[m_entry_in_topk_array]->component;
	}

	return m_iter.Key();
}


CExpression *CJoinOrderDPv2::KHeapIterator::Expression()
{
	if (m_kheap->HasTopK())
	{
		return (*(m_kheap->m_topk))[m_entry_in_topk_array]->best_expr;
	}

	return (*(m_iter.Value()))[m_entry_in_expression_array];
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::CJoinOrderDPv2
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::CJoinOrderDPv2
	(
	CMemoryPool *mp,
	CExpressionArray *pdrgpexprComponents,
	CExpressionArray *innerJoinConjuncts,
	CExpressionArray *onPredConjuncts,
	ULongPtrArray *childPredIndexes
	)
	:
	CJoinOrder(mp, pdrgpexprComponents, innerJoinConjuncts, onPredConjuncts, childPredIndexes),
	m_expression_to_edge_map(NULL),
	m_on_pred_conjuncts(onPredConjuncts),
	m_child_pred_indexes(childPredIndexes),
	m_non_inner_join_dependencies(NULL),
	m_top_k_expressions(NULL),
	m_k_heap_iterator(NULL)
{
	m_join_levels = GPOS_NEW(mp) ComponentInfoArrayLevels(mp);
	// put a NULL entry at index 0, because there are no 0-way joins
	m_join_levels->Append(GPOS_NEW(mp) ComponentInfoArray(mp));

	m_pexprDummy = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp));
	m_mp = mp;
	if (0 < m_on_pred_conjuncts->Size())
	{
		// we have non-inner joins, add dependency info
		ULONG numNonInnerJoins = m_on_pred_conjuncts->Size();

		m_non_inner_join_dependencies = GPOS_NEW(mp) CBitSetArray(mp, numNonInnerJoins);
		for (ULONG ul=0; ul<numNonInnerJoins; ul++)
		{
			m_non_inner_join_dependencies->Append(GPOS_NEW(mp) CBitSet(mp));
		}

		// compute dependencies of the NIJ right children
		// (those components must appear on the left of the NIJ)
		// Note: NIJ = Non-inner join, e.g. LOJ
		for (ULONG en = 0; en < m_ulEdges; en++)
		{
			SEdge *pedge = m_rgpedge[en];

			if (0 < pedge->m_loj_num)
			{
				// edge represents a non-inner join pred
				ULONG logicalChildNum = FindLogicalChildByNijId(pedge->m_loj_num);
				CBitSet * nijBitSet = (*m_non_inner_join_dependencies)[pedge->m_loj_num-1];

				GPOS_ASSERT(0 < logicalChildNum);
				nijBitSet->Union(pedge->m_pbs);
				// clear the bit representing the right side of the NIJ, we only
				// want to track the components needed on the left side
				nijBitSet->ExchangeClear(logicalChildNum);
			}
		}
		PopulateExpressionToEdgeMapIfNeeded();
	}

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
//		CJoinOrderDPv2::~CJoinOrderDPv2
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::~CJoinOrderDPv2()
{
#ifdef GPOS_DEBUG
	// in optimized build, we flush-down memory pools without leak checking,
	// we can save time in optimized build by skipping all de-allocations here,
	// we still have all de-llocations enabled in debug-build to detect any possible leaks
	CRefCount::SafeRelease(m_top_k_expressions);
	m_pexprDummy->Release();
	m_join_levels->Release();
	m_on_pred_conjuncts->Release();
	CRefCount::SafeRelease(m_child_pred_indexes);
	CRefCount::SafeRelease(m_non_inner_join_dependencies);
	CRefCount::SafeRelease(m_expression_to_edge_map);
	GPOS_DELETE(m_k_heap_iterator);
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinOrderToTopK
//
//	@doc:
//		Add given join order to top k join orders
//
//---------------------------------------------------------------------------


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PexprPred
//
//	@doc:
//		Extract predicate joining the two given sets or NULL for cross joins
//		or overlapping or empty sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprPred
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

	// could not find link in the map, construct it from edge set
	pexprPred = PexprBuildInnerJoinPred(pbsFst, pbsSnd);
	if (NULL == pexprPred)
	{
		pexprPred = m_pexprDummy;
	}

	if (m_pexprDummy != pexprPred)
	{
		return pexprPred;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::DeriveStats
//
//	@doc:
//		Derive stats on given expression
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::DeriveStats
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
//		CJoinOrderDPv2::DCost
//
//	@doc:
//		Primitive costing of join expressions;
//		Cost of a join expression is the "internal data flow" of the join
//		tree, the sum of all the rows flowing from the leaf nodes up to
//		the root. This does not include the number of result rows of joins,
//		for a simple reason: To compare two equivalent joins, we would have
//		to derive the stats twice, which would be expensive and run the risk
//		that we get different rowcounts.
//		NOTE: We could consider the width of the rows as well, if we had
//		a reliable way of determining the actual width.
//
//---------------------------------------------------------------------------
CDouble
CJoinOrderDPv2::DCost
	(
	CExpression *pexpr
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

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
			// NOTE: This will count cost of leaves twice
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
//		CJoinOrderDPv2::PexprBuildInnerJoinPred
//
//	@doc:
//		Build predicate connecting the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprBuildInnerJoinPred
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
			// edge represents an inner join pred
			0 == pedge->m_loj_num &&
			// all columns referenced in the edge pred are provided
			pbs->ContainsAll(pedge->m_pbs) &&
			// the edge represents a true join predicate between the two components
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
//		CJoinOrderDPv2::GetJoinExpr
//
//	@doc:
//		Build a CExpression joining the two given sets
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::GetJoinExpr
	(
	SComponentInfo *left_child,
	SComponentInfo *right_child
	)
{
	CExpression *scalar_expr = NULL;
	CBitSet *required_on_left = NULL;
	BOOL isNIJ = IsRightChildOfNIJ(right_child, &scalar_expr, &required_on_left);

	if (NULL == scalar_expr)
	{
		scalar_expr = PexprPred(left_child->component, right_child->component);
	}
	else
	{
		// check whether scalar_expr can be computed from left_child and right_child,
		// otherwise this is not a valid join
		GPOS_ASSERT(NULL != required_on_left);
		if (!left_child->component->ContainsAll(required_on_left))
		{
			// the left child does not produce all the values needed in the ON
			// predicate, so this is not a valid join
			return NULL;
		}
		scalar_expr->AddRef();
	}

	if (NULL == scalar_expr)
	{
		scalar_expr = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
	}

	CExpression *left_expr = left_child->best_expr;
	CExpression *right_expr = right_child->best_expr;
	CExpression *join_expr = NULL;

	left_expr->AddRef();
	right_expr->AddRef();

	if (isNIJ)
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalLeftOuterJoin>(m_mp, left_expr, right_expr, scalar_expr);
	}
	else
	{
		join_expr = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(m_mp, left_expr, right_expr, scalar_expr);
	}

	return join_expr;
}


//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinExprAlternativeForBitSet
//
//	@doc:
//		Add the given expression to BitSetToExpressionArrayMap map
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddJoinExprAlternativeForBitSet
	(
	CBitSet *join_bitset,
	CExpression *join_expr,
	KHeap *map
	)
{
	GPOS_ASSERT(NULL != join_bitset);
	GPOS_ASSERT(NULL != join_expr);

	map->Insert(join_bitset, join_expr);
}

BOOL
CJoinOrderDPv2::PopulateExpressionToEdgeMapIfNeeded()
{
	if (0 == m_child_pred_indexes->Size())
	{
		return false;
	}

	BOOL populate = false;
	// make a bitset b with all the LOJ right children
	CBitSet *loj_right_children = GPOS_NEW(m_mp) CBitSet(m_mp);

	for (ULONG c=0; c<m_child_pred_indexes->Size(); c++)
	{
		if (0 < *((*m_child_pred_indexes)[c]))
		{
			loj_right_children->ExchangeSet(c);
		}
	}

	for (ULONG en1 = 0; en1 < m_ulEdges; en1++)
	{
		SEdge *pedge = m_rgpedge[en1];

		if (pedge->m_loj_num == 0)
		{
			// check whether it refers to any LOJ right child (whether its bitset overlaps with b)
			if (!loj_right_children->IsDisjoint(pedge->m_pbs))
			{
				populate = true;
				break;
			}
		}
	}

	if (populate)
	{
		m_expression_to_edge_map = GPOS_NEW(m_mp) ExpressionToEdgeMap(m_mp);

		for (ULONG en2 = 0; en2 < m_ulEdges; en2++)
		{
			SEdge *pedge = m_rgpedge[en2];

			pedge->AddRef();
			pedge->m_pexpr->AddRef();
			m_expression_to_edge_map->Insert(pedge->m_pexpr, pedge);
		}
	}

	loj_right_children->Release();

	return populate;
}

// add a select node with any remaining edges (predicates) that have
// not been incorporated in the join tree
CExpression *
CJoinOrderDPv2::AddSelectNodeForRemainingEdges(CExpression *join_expr)
{
	if (NULL == m_expression_to_edge_map)
	{
		return join_expr;
	}

	CExpressionArray *exprArray = GPOS_NEW(m_mp) CExpressionArray(m_mp);
	RecursivelyMarkEdgesAsUsed(join_expr);

	// find any unused edges and add them to a select
	for (ULONG en = 0; en < m_ulEdges; en++)
	{
		SEdge *pedge = m_rgpedge[en];

		if (pedge->m_fUsed)
		{
			// mark the edge as unused for the next alternative, where
			// we will have to repeat this check
			pedge->m_fUsed = false;
		}
		else
		{
			// found an unused edge, this one will need to go into
			// a select node on top of the join
			pedge->m_pexpr->AddRef();
			exprArray->Append(pedge->m_pexpr);
		}
	}

	if (0 < exprArray->Size())
	{
		CExpression *conj = CPredicateUtils::PexprConjunction(m_mp, exprArray);

		join_expr->AddRef();

		return GPOS_NEW(m_mp) CExpression(m_mp, GPOS_NEW(m_mp) CLogicalSelect(m_mp), join_expr, conj);
	}

	exprArray->Release();

	return join_expr;
}


void CJoinOrderDPv2::RecursivelyMarkEdgesAsUsed(CExpression *expr)
{
	if (expr->Pop()->FLogical())
	{
		for (ULONG ul=0; ul< expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
	}
	else
	{
		GPOS_ASSERT(expr->Pop()->FScalar());
		const SEdge *edge = m_expression_to_edge_map->Find(expr);
		if (NULL != edge)
		{
			// we found the edge belonging to this expression, terminate the recursion
			const_cast<SEdge *>(edge)->m_fUsed = true;
			return;
		}

		// we should not reach the leaves of the tree without finding an edge
		GPOS_ASSERT(0 < expr->Arity());

		// this is not an edge, it is probably an AND of multiple edges
		for (ULONG ul = 0; ul < expr->Arity(); ul++)
		{
			RecursivelyMarkEdgesAsUsed((*expr)[ul]);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchJoinOrders
//
//	@doc:
//		Enumerate all the possible joins between two lists of components
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::KHeap *
CJoinOrderDPv2::SearchJoinOrders
	(
	ComponentInfoArray *join_pair_components,
	ComponentInfoArray *other_join_pair_components,
	ULONG topK
	)
{
	GPOS_ASSERT(join_pair_components);
	GPOS_ASSERT(other_join_pair_components);

	ULONG join_pairs_size = join_pair_components->Size();
	ULONG other_join_pairs_size = other_join_pair_components->Size();
	KHeap *join_pairs_map = GPOS_NEW(m_mp) KHeap(m_mp, this, topK);

	for (ULONG join_pair_id = 0; join_pair_id < join_pairs_size; join_pair_id++)
	{
		SComponentInfo *left_component_info = (*join_pair_components)[join_pair_id];
		CBitSet *left_bitset = left_component_info->component;

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		ULONG other_pair_start_id = 0;
		if (join_pair_components == other_join_pair_components)
			other_pair_start_id = join_pair_id + 1;

		for (ULONG other_pair_id = other_pair_start_id; other_pair_id < other_join_pairs_size; other_pair_id++)
		{
			CBitSet *join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);
			SComponentInfo *right_component_info = (*other_join_pair_components)[other_pair_id];
			CBitSet *right_bitset = right_component_info->component;
			if (!left_bitset->IsDisjoint(right_bitset))
			{
				join_bitset->Release();
				continue;
			}

			CExpression *join_expr = GetJoinExpr(left_component_info, right_component_info);

			if (NULL != join_expr)
			{
				join_bitset->Union(right_bitset);
				AddJoinExprAlternativeForBitSet(join_bitset, join_expr, join_pairs_map);
				join_expr->Release();
			}
			join_bitset->Release();
		}
	}
	return join_pairs_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddExprs
//
//	@doc:
//		Add the given expressions to a CExpressionArray
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddExprs
	(
	const CExpressionArray *candidate_join_exprs,
	CExpressionArray *result_join_exprs
	)
{
	for (ULONG ul = 0; ul < candidate_join_exprs->Size(); ul++)
	{
		CExpression *join_expr = (*candidate_join_exprs)[ul];
		join_expr->AddRef();
		result_join_exprs->Append(join_expr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::AddJoinExprsForBitSet
//
//	@doc:
//		Add candidate expressions to BitSetToExpressionArrayMap result_map if
//      not already present, merge expression arrays otherwise
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::AddJoinExprsForBitSet
	(
	KHeap *result_map,
	KHeap *candidate_map
	)
{
	if (NULL == candidate_map)
		return;

	KHeapIterator iter(candidate_map);
	while (iter.Advance())
	{
		CBitSet *newBitSet = GPOS_NEW(m_mp) CBitSet(m_mp, *iter.BitSet());
		result_map->Insert(newBitSet, iter.Expression());
		newBitSet->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::MergeJoinExprsForBitSet
//
//	@doc:
//		Create a new map that contains the union of two provided maps
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::KHeap *
CJoinOrderDPv2::MergeJoinExprsForBitSet
	(
	KHeap *map,
	KHeap *other_map,
	ULONG topK
	)
{
	KHeap *result_map = GPOS_NEW(m_mp) KHeap(m_mp, this, topK);
	AddJoinExprsForBitSet(result_map, map);
	AddJoinExprsForBitSet(result_map, other_map);
	return result_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::SearchBushyJoinOrders
//
//	@doc:
//		Generate all bushy join trees of level current_level,
//		given an array of an array of bit sets (components), arranged by level
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::KHeap *
CJoinOrderDPv2::SearchBushyJoinOrders
	(
	ULONG current_level
	)
{
	KHeap *final_bushy_join_exprs_map = NULL;
	ULONG topK = ULONG (floor(pow(GPOPT_DP_BUSHY_TREE_BASE, (GPOPT_DP_STOP_BUSHY_TREES_AT_LEVEL - (int) current_level))));

	if (0 == topK)
	{
		// we've exceeded the number of joins for which we generate bushy trees
		return final_bushy_join_exprs_map;
	}

	// join trees of level 3 and below are never bushy
	if (current_level > 3)
	{
		// try all joins of bitsets of level x and y, where
		// x + y = current_level and x > 1 and y > 1
		for (ULONG k = 3; k <= current_level; k++)
		{
			ULONG join_level = k - 1;
			ULONG other_join_level = current_level - join_level;
			if (join_level > other_join_level)
				// we've already considered the commutated join
				break;
			ComponentInfoArray *join_component_infos = (*m_join_levels)[join_level];
			ComponentInfoArray *other_join_component_infos = (*m_join_levels)[other_join_level];
			KHeap *bitset_bushy_join_exprs_map = SearchJoinOrders
														(
														 join_component_infos,
														 other_join_component_infos,
														 topK
														);
			KHeap *interim_map = final_bushy_join_exprs_map;
			final_bushy_join_exprs_map = MergeJoinExprsForBitSet(bitset_bushy_join_exprs_map, interim_map, topK);
			CRefCount::SafeRelease(interim_map);
			bitset_bushy_join_exprs_map->Release();
		}
	}
	return final_bushy_join_exprs_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PExprExpand
//
//	@doc:
//		Main driver for join order enumeration, called by xform
//
//---------------------------------------------------------------------------
CExpression *
CJoinOrderDPv2::PexprExpand()
{
	// We go from full enumeration of all linear and bushy trees to
	// a greedy algorithm for larger joins, by limiting the number of
	// alternatives we generate to a fixed k (keep only the top k).
	//
	// This k is a function of the form k = x ** (l - n) where
	//   k is the number of alternatives to keep
	//   x is a floating point constant that indicates how steep the
	//     reduction is as the number of joins grows
	//   l is the limit at which we want to have reduced k
	//     to 0 (for bushy trees) and to 1 (for expressions per level)
	//   n is the join level (number of non-join vertices joined)
	//
	// We have two separate formulas, one for the number of bushy joins
	// and another for the number of expressions on a join level.
	//
	// Note that we use a floor() function to compute the number of bushy
	// joins, which means that we allow zero bushy joins once we reach
	// the limit l, while we use a ceil() function for the number of expressions,
	// so that we will always allow at least one expression per join level.
	// One expression per join level is a greedy algorithm.
	//
	// Here is what this looks like as a graph:
	//
	//          ^
	//          |           +            *            + number of bushy
	//          |                                       trees considered (top k)
	//   top k  |
	//          |            +            *           * number of expressions
	//          |                                       considered per join level
	//          |              +            *           (top k)
	//          |               +            *
	//        2 |                +             *
	//        1 |                  +              *********************
	//        0 +--------------------+--------------------------------------->
	//            1 2 3              l1           l2         # of joins in query
	//
	ComponentInfoArray *non_join_vertex_component_infos = GPOS_NEW(m_mp) ComponentInfoArray(m_mp);
	// put the "non join vertices", the nodes of the join tree that
	// are not joins themselves, at the first level
	for (ULONG relation_id = 0; relation_id < m_ulComps; relation_id++)
	{
		CBitSet *non_join_vertex_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		non_join_vertex_bitset->ExchangeSet(relation_id);
		CExpression *pexpr_relation = m_rgpcomp[relation_id]->m_pexpr;
		pexpr_relation->AddRef();
		SComponentInfo *non_join_component_info = GPOS_NEW(m_mp) SComponentInfo(non_join_vertex_bitset,
																				pexpr_relation,
																				0.0);
		non_join_vertex_component_infos->Append(non_join_component_info);
	}

	m_join_levels->Append(non_join_vertex_component_infos);

	for (ULONG current_join_level = 2; current_join_level <= m_ulComps; current_join_level++)
	{
		ULONG topK = ULONG (ceil(pow(GPOPT_DP_GREEDY_BASE, (GPOPT_DP_START_GREEDY_AT_LEVEL - (int) current_join_level))));
		ULONG previous_level = current_join_level - 1;
		ComponentInfoArray *prev_lev_comps = (*m_join_levels)[previous_level];
		// build linear "current_join_level" joins, with a "previous_level"-way join on one
		// side and a non-join vertex on the other side
		KHeap *bitset_join_exprs_map = SearchJoinOrders(prev_lev_comps, non_join_vertex_component_infos, topK);
		// build bushy trees - joins between two other joins
		KHeap *bitset_bushy_join_exprs_map = SearchBushyJoinOrders(current_join_level);

		// A set of different components/bit sets, each with a set of equivalent expressions,
		// for current_join_level
		KHeap *all_join_exprs_map = MergeJoinExprsForBitSet(bitset_join_exprs_map, bitset_bushy_join_exprs_map, topK);
		if (current_join_level == m_ulComps)
		{
			m_top_k_expressions = all_join_exprs_map;
		}
		else
		{
			// levels below the top level: Keep the best expression for each bit set
			ComponentInfoArray *cheapest_bitset_join_expr_map = GetCheapestJoinExprForBitSet(all_join_exprs_map);
			m_join_levels->Append(cheapest_bitset_join_expr_map);
			all_join_exprs_map->Release();
		}
		CRefCount::SafeRelease(bitset_bushy_join_exprs_map);
		bitset_join_exprs_map->Release();
	}
	return NULL;
}

CExpression*
CJoinOrderDPv2::GetNextOfTopK()
{
	if (NULL == m_top_k_expressions)
	{
		return NULL;
	}

	if (NULL == m_k_heap_iterator)
	{
		m_k_heap_iterator = GPOS_NEW(m_mp) KHeapIterator(m_top_k_expressions);
	}

	if (!m_k_heap_iterator->Advance())
	{
		return NULL;
	}

	CExpression *joinExpr = m_k_heap_iterator->Expression();
	CExpression *joinOrSelect = AddSelectNodeForRemainingEdges(joinExpr);

	return joinOrSelect;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::GetCheapestJoinExprForBitSet
//
//	@doc:
//		Convert a BitSetToExpressionArrayMap to a BitSetToExpressionMap
//		by selecting the cheapest expression from each array
//
//---------------------------------------------------------------------------
CJoinOrderDPv2::ComponentInfoArray *
CJoinOrderDPv2::GetCheapestJoinExprForBitSet
	(
	KHeap *bitset_exprs_map
	)
{
	ComponentInfoArray *cheapest_join_array = GPOS_NEW(m_mp) ComponentInfoArray(m_mp);
	KHeapIterator iter(bitset_exprs_map);

	while (iter.Advance())
	{
		const CBitSet *join_bitset = iter.BitSet();
		CExpression *expr = iter.Expression();
		const CExpressionArray *join_exprs = bitset_exprs_map->ArrayForBitset(join_bitset);
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

		if (best_join_expr == expr)
		{
			CBitSet *join_bitset_entry = GPOS_NEW(m_mp) CBitSet(m_mp, *join_bitset);
			best_join_expr->AddRef();

			SComponentInfo *component_info = GPOS_NEW(m_mp) SComponentInfo(join_bitset_entry, best_join_expr, min_join_cost);

			cheapest_join_array->Append(component_info);
		}

	}
	return cheapest_join_array;
}

BOOL
CJoinOrderDPv2::IsRightChildOfNIJ
	(SComponentInfo *component,
	 CExpression **onPredToUse,
	 CBitSet **requiredBitsOnLeft
	)
{
	*onPredToUse = NULL;
	*requiredBitsOnLeft = NULL;

	if (1 != component->component->Size() || 0 == m_on_pred_conjuncts->Size())
	{
		// this is not a non-join vertex component (and only those can be right
		// children of NIJs), or the entire NAry join doesn't contain any NIJs
		return false;
	}

	// get the child predicate index for the non-join vertex component represented
	// by this component
	CBitSetIter iter(*component->component);

	// there is only one bit set for this component
	iter.Advance();

	ULONG childPredIndex = *(*m_child_pred_indexes)[iter.Bit()];

	if (GPOPT_ZERO_INNER_JOIN_PRED_INDEX != childPredIndex)
	{
		// this non-join vertex component is the right child of an
		// NIJ, return the ON predicate to use and also return TRUE
		*onPredToUse = (*m_on_pred_conjuncts)[childPredIndex-1];
		// also return the required minimal component on the left side of the join
		*requiredBitsOnLeft = (*m_non_inner_join_dependencies)[childPredIndex-1];
		return true;
	}

	// this is a non-join vertex component that is not the right child of an NIJ
	return false;
}

ULONG
CJoinOrderDPv2::FindLogicalChildByNijId(ULONG nij_num)
{
	GPOS_ASSERT(NULL != m_child_pred_indexes);

	for (ULONG c=0; c<m_child_pred_indexes->Size(); c++)
	{
		if (*(*m_child_pred_indexes)[c] == nij_num)
		{
			return c;
		}
	}

	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::OsPrint
//
//	@doc:
//		Print created join order
//
//---------------------------------------------------------------------------
IOstream &
CJoinOrderDPv2::OsPrint
	(
	IOstream &os
	)
	const
{
	ULONG num_levels = m_join_levels->Size();
	ULONG num_bitsets = 0;

	for (ULONG lev=0; lev<num_levels; lev++)
	{
		ComponentInfoArray *bitsets_this_level = (*m_join_levels)[lev];
		ULONG num_bitsets_this_level = bitsets_this_level->Size();

		os << "CJoinOrderDPv2 - Level: " << lev << std::endl;

		for (ULONG c=0; c<num_bitsets_this_level; c++)
		{
			SComponentInfo *ci = (*bitsets_this_level)[c];
			num_bitsets++;
			os << "   Bitset: ";
			ci->component->OsPrint(os);
			os << std::endl;
			os << "   Expression: ";
			if (NULL == ci->best_expr)
			{
				os << "NULL" << std::endl;
			}
			else
			{
				ci->best_expr->OsPrint(os);
			}
			os << "   Cost: ";
			ci->cost.OsPrint(os);
			os << std::endl;
		}
	}

	os << "CJoinOrderDPv2 - total number of bitsets: " << num_bitsets << std::endl;

	return os;
}

// EOF
