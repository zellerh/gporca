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
#include "gpopt/optimizer/COptimizerConfig.h"
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
/*
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
		// walk through the top k entries, no need to look at the bitset to expression array map
		m_entry_in_topk_array++;
		return m_entry_in_topk_array < m_kheap->m_topk->Size();
	}

	// otherwise, create an iterator of the KHeap, using the BitSetToExpressionArrayMapIter
	m_entry_in_expression_array++ ;

	if (0 == m_entry_in_expression_array)
	{
		// first time we reach here, do an initial advance to the next bitset
		return m_iter.Advance();
	}

	if (m_entry_in_expression_array >= m_iter.Value()->Size())
	{
		// the current bitset of m_iter is exhausted, continue with the first
		// entry in the CExpressionArray of the next bitset (if it exists)
		m_entry_in_expression_array = 0;
		return m_iter.Advance();
	}

	// we advanced to the next entry in the current expression array
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
}*/


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
	CExpressionArray *pdrgpexprAtoms,
	CExpressionArray *innerJoinConjuncts,
	CExpressionArray *onPredConjuncts,
	ULongPtrArray *childPredIndexes
	)
	:
	CJoinOrder(mp, pdrgpexprAtoms, innerJoinConjuncts, onPredConjuncts, childPredIndexes),
	m_expression_to_edge_map(NULL),
	m_on_pred_conjuncts(onPredConjuncts),
	m_child_pred_indexes(childPredIndexes),
	m_non_inner_join_dependencies(NULL),
	m_top_k_index(0)
{
	m_join_levels = GPOS_NEW(mp) DPv2Levels(mp, m_ulComps+1);
	// populate levels array with n+1 levels for an n-way join
	// level 0 remains unused, so index i corresponds to level i,
	// making it easier for humans to read the code
	for (ULONG l=0; l<= m_ulComps; l++)
	{
		m_join_levels->Append(GPOS_NEW(mp) GroupInfoArray(mp));
	}

	m_top_k_group_limits = GPOS_NEW_ARRAY(mp, ULONG, m_ulComps+1);

	for (ULONG k=0; k<=m_ulComps; k++)
	{
		// 0 means no limit, this is the default
		m_top_k_group_limits[k] = 0;
	}

	m_bitset_to_group_info_map = GPOS_NEW(mp) BitSetToGroupInfoMap(mp);

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
	CRefCount::SafeRelease(m_non_inner_join_dependencies);
	CRefCount::SafeRelease(m_child_pred_indexes);
	m_bitset_to_group_info_map->Release();
	CRefCount::SafeRelease(m_expression_to_edge_map);
	GPOS_DELETE_ARRAY(m_top_k_group_limits);
	m_join_levels->Release();
	m_on_pred_conjuncts->Release();
#endif // GPOS_DEBUG
}


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

	return pexprPred;
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
	 SGroupInfo *group,
	 const SGroupInfo *leftChildGroup,
	 const SGroupInfo *rightChildGroup
	)
{
	CDouble dCost(group->m_expr_for_stats->Pstats()->Rows());

	if (NULL != leftChildGroup)
	{
		GPOS_ASSERT(NULL != rightChildGroup);
		dCost = dCost + leftChildGroup->m_expr_info->m_cost;
		dCost = dCost + rightChildGroup->m_expr_info->m_cost;
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
	SGroupInfo *left_child,
	SGroupInfo *right_child
	)
{
	CExpression *scalar_expr = NULL;
	CBitSet *required_on_left = NULL;
	BOOL isNIJ = IsRightChildOfNIJ(right_child, &scalar_expr, &required_on_left);

	if (!isNIJ)
	{
		GPOS_ASSERT(NULL == scalar_expr);
		scalar_expr = PexprPred(left_child->m_atoms, right_child->m_atoms);
	}
	else
	{
		// check whether scalar_expr can be computed from left_child and right_child,
		// otherwise this is not a valid join
		GPOS_ASSERT(NULL != scalar_expr && NULL != required_on_left);
		if (!left_child->m_atoms->ContainsAll(required_on_left))
		{
			// the left child does not produce all the values needed in the ON
			// predicate, so this is not a valid join
			return NULL;
		}
		scalar_expr->AddRef();
	}

	if (NULL == scalar_expr)
	{
		// this is a cross product

		if (right_child->IsAnAtom())
		{
			// generate a TRUE boolean expression as the join predicate of the cross product
			scalar_expr = CPredicateUtils::PexprConjunction(m_mp, NULL /*pdrgpexpr*/);
		}
		else
		{
			// we don't do bushy cross products, any mandatory or optional cross products
			// are linear trees
			return NULL;
		}
	}

	CExpression *left_expr = left_child->m_expr_info->m_best_expr;
	CExpression *right_expr = right_child->m_expr_info->m_best_expr;
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
/*void
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
}*/

BOOL
CJoinOrderDPv2::PopulateExpressionToEdgeMapIfNeeded()
{
	// In some cases we may not place all of the predicates in the NAry join in
	// the resulting tree of binary joins. If that situation is a possibility,
	// we'll create a map from expressions to edges, so that we can find any
	// unused edges to be placed in a select node on top of the join.
	//
	// Example:
	// select * from foo left join bar on foo.a=bar.a where coalesce(bar.b, 0) < 10;
	if (0 == m_child_pred_indexes->Size())
	{
		// all inner joins, all predicates will be placed
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
			// check whether this inner join (WHERE) predicate refers to any LOJ right child
			// (whether its bitset overlaps with b)
			// or whether we see any local predicates (this should be uncommon)
			if (!loj_right_children->IsDisjoint(pedge->m_pbs) || 1 == pedge->m_pbs->Size())
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
	// this method doesn't consume a ref count from the input parameter,
	// but it uses the input for the output in all cases
	join_expr->AddRef();

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

		return GPOS_NEW(m_mp) CExpression(m_mp, GPOS_NEW(m_mp) CLogicalSelect(m_mp), join_expr, conj);
	}

	exprArray->Release();

	return join_expr;
}


void CJoinOrderDPv2::RecursivelyMarkEdgesAsUsed(CExpression *expr)
{
	GPOS_CHECK_STACK_SIZE;

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
void
CJoinOrderDPv2::SearchJoinOrders
	(
	 ULONG left_level,
	 ULONG right_level
	)
{
	GPOS_ASSERT(left_level > 0 &&
				right_level > 0 &&
				left_level + right_level <= m_ulComps);

	GroupInfoArray *left_group_info_array = (*m_join_levels)[left_level];
	GroupInfoArray *right_group_info_array = (*m_join_levels)[right_level];

	ULONG left_size = left_group_info_array->Size();
	ULONG right_size = right_group_info_array->Size();

	for (ULONG left_ix=0; left_ix<left_size; left_ix++)
	{
		SGroupInfo *left_group_info = (*left_group_info_array)[left_ix];
		CBitSet *left_bitset = left_group_info->m_atoms;
		ULONG right_ix = 0;

		// if pairs from the same level, start from the next
		// entry to avoid duplicate join combinations
		// i.e a join b and b join a, just try one
		// commutativity will take care of the other
		if (left_level == right_level)
		{
			right_ix = left_ix + 1;
		}

		for (; right_ix<right_size; right_ix++)
		{
			SGroupInfo *right_group_info = (*right_group_info_array)[right_ix];
			CBitSet *right_bitset = right_group_info->m_atoms;

			if (!left_bitset->IsDisjoint(right_bitset))
			{
				// not a valid join, left and right tables must not overlap
				continue;
			}

			CExpression *join_expr = GetJoinExpr(left_group_info, right_group_info);

			if (NULL != join_expr)
			{
				// we have a valid join
				CBitSet *join_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, *left_bitset);

				// TODO: Reduce non-mandatory cross products

				join_bitset->Union(right_bitset);

				SGroupInfo *group_info = m_bitset_to_group_info_map->Find(join_bitset);

				if (NULL != group_info)
				{
					// we are dealing with a group that already has an existing expression in it
					CDouble newCost = DCost(group_info, left_group_info, right_group_info);

					if (newCost < group_info->m_expr_info->m_cost)
					{
						// this new expression is better than the one currently in the group,
						// so release the current expression and replace it with the new one
						SExpressionInfo *expr_info = group_info->m_expr_info;

						expr_info->m_best_expr->Release();
						expr_info->m_best_expr = join_expr;
						expr_info->m_left_child_group = left_group_info;
						expr_info->m_right_child_group = right_group_info;
						expr_info->m_cost = newCost;
					}
					else
					{
						join_expr->Release();
					}
					join_bitset->Release();
				}
				else
				{
					// this is a new group, insert it if we have not yet exceeded the maximum number of groups for this level
					group_info = GPOS_NEW(m_mp) SGroupInfo(join_bitset,
														   GPOS_NEW(m_mp) SExpressionInfo(join_expr,
																						  left_group_info,
																						  right_group_info));
					AddGroupInfo(group_info);
				}
			}
		}
	}
}


void
CJoinOrderDPv2::AddGroupInfo(SGroupInfo *groupInfo)
{
	ULONG join_level = groupInfo->m_atoms->Size();
	// find the join level at which to insert this group
	GroupInfoArray *join_level_array = (*m_join_levels)[join_level];

	// TODO: Better way to limit the number of groups at this level to the top k
	if (m_top_k_group_limits[join_level] == 0 ||
		m_top_k_group_limits[join_level] >= join_level_array->Size())
	{
		// derive stats and cost
		groupInfo->m_expr_for_stats = groupInfo->m_expr_info->m_best_expr;
		groupInfo->m_expr_for_stats->AddRef();
		DeriveStats(groupInfo->m_expr_for_stats);
		groupInfo->m_expr_info->m_cost = DCost
					(
					 groupInfo,
					 groupInfo->m_expr_info->m_left_child_group,
					 groupInfo->m_expr_info->m_right_child_group
					);

		// now do the actual insert
		join_level_array->Append(groupInfo);

		if (1 < join_level)
		{
			// also insert into the bitset to group map
			groupInfo->m_atoms->AddRef();
			groupInfo->AddRef();
			m_bitset_to_group_info_map->Insert(groupInfo->m_atoms, groupInfo);
		}
	}
	else
	{
		groupInfo->Release();
	}
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
void
CJoinOrderDPv2::SearchBushyJoinOrders
	(
	 ULONG current_level
	)
{
	// try all joins of bitsets of level x and y, where
	// x + y = current_level and x > 1 and y > 1
	// note that join trees of level 3 and below are never bushy,
	// so this loop only executes at current_level >= 4
	for (ULONG left_level = 2; left_level < current_level-1; left_level++)
	{
		if (LevelIsFull(current_level))
		{
			// we've exceeded the number of joins for which we generate bushy trees
			return;
		}

		ULONG right_level = current_level - left_level;
		if (left_level > right_level)
			// we've already considered the commuted join
			break;
		SearchJoinOrders(left_level, right_level);
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CJoinOrderDPv2::PExprExpand
//
//	@doc:
//		Main driver for join order enumeration, called by xform
//
//---------------------------------------------------------------------------
void
CJoinOrderDPv2::PexprExpand()
{
	// put the "atoms", the nodes of the join tree that
	// are not joins themselves, at the first level
	for (ULONG atom_id = 0; atom_id < m_ulComps; atom_id++)
	{
		CBitSet *atom_bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		atom_bitset->ExchangeSet(atom_id);
		CExpression *pexpr_atom = m_rgpcomp[atom_id]->m_pexpr;
		pexpr_atom->AddRef();
		SGroupInfo *atom_info = GPOS_NEW(m_mp) SGroupInfo(atom_bitset,
														  GPOS_NEW(m_mp) SExpressionInfo(pexpr_atom,
																						 NULL,
																						 NULL));
		AddGroupInfo(atom_info);
	}

	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	const CHint *phint = optimizer_config->GetHint();
	ULONG join_order_exhaustive_limit = phint->UlJoinOrderDPLimit();

	// for larger joins, compute the limit for the number of groups at each level, this
	// follows the number of groups for the largest join for which we do exhaustive search
	if (join_order_exhaustive_limit < m_ulComps)
	{
		for (ULONG k=2; k<=m_ulComps; k++)
		{
			if (join_order_exhaustive_limit < k)
			{
				m_top_k_group_limits[k] = NChooseK(join_order_exhaustive_limit, k);
			}
			else
			{
				m_top_k_group_limits[k] = 1;
			}
		}
	}

	// build n-ary joins from the bottom up, starting with 2-way, 3-way up to m_ulComps-way
	for (ULONG current_join_level = 2; current_join_level <= m_ulComps; current_join_level++)
	{
		ULONG previous_level = current_join_level - 1;

		// build linear "current_join_level" joins, with a "previous_level"-way join on one
		// side and an atom on the other side
		SearchJoinOrders(previous_level, 1);

		// build bushy trees - joins between two other joins
		SearchBushyJoinOrders(current_join_level);
	}
}

CExpression*
CJoinOrderDPv2::GetNextOfTopK()
{
	// TODO: Return more than just one expression
	GroupInfoArray *top_level = (*m_join_levels)[m_ulComps];

	if (top_level->Size() <= m_top_k_index)
	{
		return NULL;
	}

	CExpression *join_result = (*top_level)[m_top_k_index++]->m_expr_info->m_best_expr;

	return AddSelectNodeForRemainingEdges(join_result);

	/*
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

	return joinOrSelect;*/
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
/*CJoinOrderDPv2::ComponentInfoArray *
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
		// TODO: Find a more efficient way to compute the cheapest expression
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
}*/

BOOL
CJoinOrderDPv2::IsRightChildOfNIJ
	(SGroupInfo *groupInfo,
	 CExpression **onPredToUse,
	 CBitSet **requiredBitsOnLeft
	)
{
	*onPredToUse = NULL;
	*requiredBitsOnLeft = NULL;

	if (1 != groupInfo->m_atoms->Size() || 0 == m_on_pred_conjuncts->Size())
	{
		// this is not a non-join vertex component (and only those can be right
		// children of NIJs), or the entire NAry join doesn't contain any NIJs
		return false;
	}

	// get the child predicate index for the non-join vertex component represented
	// by this component
	CBitSetIter iter(*groupInfo->m_atoms);

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

ULONG CJoinOrderDPv2::NChooseK(ULONG n, ULONG k)
{
	ULLONG numerator = 1;
	ULLONG denominator = 1;

	for (ULONG i=1; i<=k; i++)
	{
		numerator *= n+1-i;
		denominator *= i;
	}

	return (ULONG) (numerator / denominator);
}

BOOL
CJoinOrderDPv2::LevelIsFull(ULONG level)
{
	return (*m_join_levels)[level]->Size() >= m_top_k_group_limits[level];
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
		GroupInfoArray *bitsets_this_level = (*m_join_levels)[lev];
		ULONG num_bitsets_this_level = bitsets_this_level->Size();

		os << "CJoinOrderDPv2 - Level: " << lev << std::endl;

		for (ULONG c=0; c<num_bitsets_this_level; c++)
		{
			SGroupInfo *ci = (*bitsets_this_level)[c];
			num_bitsets++;
			os << "   Bitset: ";
			ci->m_atoms->OsPrint(os);
			os << std::endl;
			os << "   Expression: ";
			if (NULL == ci->m_expr_info)
			{
				os << "NULL" << std::endl;
			}
			else
			{
				ci->m_expr_info->m_best_expr->OsPrint(os);
			}
			os << "   Cost: ";
			ci->m_expr_info->m_cost.OsPrint(os);
			os << std::endl;
		}
	}

	os << "CJoinOrderDPv2 - total number of bitsets: " << num_bitsets << std::endl;

	return os;
}

// EOF
