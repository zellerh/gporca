//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDPv2_H
#define GPOPT_CJoinOrderDPv2_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CBitSet.h"
#include "gpos/io/IOstream.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/xforms/CJoinOrder.h"
#include "gpopt/operators/CExpression.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderDPv2
	//
	//	@doc:
	//		Helper class for creating join orders using dynamic programming
	//
	//---------------------------------------------------------------------------
	class CJoinOrderDPv2 : public CJoinOrder
	{

		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SComponentPair
			//
			//	@doc:
			//		Struct containing a component, its best expression, and cost
			//
			//---------------------------------------------------------------------------
			struct SComponentInfo : public CRefCount
				{
					CBitSet *component;
					CExpression *best_expr;
					CDouble cost;

					SComponentInfo() : component(NULL),
									   best_expr(NULL),
									   cost(0.0)
					{
					}

					SComponentInfo(CBitSet *component,
								   CExpression *best_expr,
								   CDouble cost
								   ) : component(component),
									   best_expr(best_expr),
									   cost(cost)
					{
					}

					~SComponentInfo()
					{
						CRefCount::SafeRelease(component);
						CRefCount::SafeRelease(best_expr);
					}

				};

			// hashing function
			static
			ULONG UlHashBitSet
				(
				const CBitSet *pbs
				)
			{
				GPOS_ASSERT(NULL != pbs);

				return pbs->HashValue();
			}

			// equality function
			static
			BOOL FEqualBitSet
				(
				const CBitSet *pbsFst,
				const CBitSet *pbsSnd
				)
			{
				GPOS_ASSERT(NULL != pbsFst);
				GPOS_ASSERT(NULL != pbsSnd);

				return pbsFst->Equals(pbsSnd);
			}


			// hash map from bit set to expression array
			typedef CHashMap<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMap;

			// hash map iter from bit set to expression array
			typedef CHashMapIter<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMapIter;

			typedef CHashMap<CExpression, SEdge, CExpression::HashValue, CUtils::Equals,
			CleanupRelease<CExpression>, CleanupRelease<SEdge> > ExpressionToEdgeMap;

			// dynamic array of SComponentInfos
			typedef CDynamicPtrArray<SComponentInfo, CleanupRelease<SComponentInfo> > ComponentInfoArray;

			// dynamic array of componentInfoArray, where each index represents the level
			typedef CDynamicPtrArray<ComponentInfoArray, CleanupRelease> ComponentInfoArrayLevels;

			class KHeapIterator;

			class KHeap : public CRefCount
			{
				friend class KHeapIterator;
			private:

				CJoinOrderDPv2 *m_join_order;
				BitSetToExpressionArrayMap *m_bitSetExprArrayMap;
				ComponentInfoArray *m_topk;
				CMemoryPool *m_mp;
				ULONG m_k;
				ULONG m_size;
				CDouble m_highest_cost;

				void BuildTopK();
				ULONG EvictMostExpensiveEntry();

			public:

				KHeap(CMemoryPool *mp, CJoinOrderDPv2 *join_order, ULONG k);
				~KHeap();
				BOOL Insert(CBitSet *join_bitset, CExpression *join_expr);
				CExpressionArray *ArrayForBitset(const CBitSet *bit_set);
				BitSetToExpressionArrayMap *BSExpressionArrayMap() { return m_bitSetExprArrayMap; }
				BOOL HasTopK() { return NULL != m_topk; }
			};

			class KHeapIterator
			{
			private:
				KHeap *m_kheap;
				BitSetToExpressionArrayMapIter m_iter;
				LINT m_entry_in_expression_array;
				LINT m_entry_in_topk_array;

			public:
				KHeapIterator(KHeap *kHeap);
				BOOL Advance();
				const CBitSet *BitSet();
				CExpression *Expression();
			};

			// list of components, organized by level, main data structure for dynamic programming
			ComponentInfoArrayLevels *m_join_levels;

			ExpressionToEdgeMap *m_expression_to_edge_map;

			// ON predicates for NIJs (non-inner joins, e.g. LOJs)
			// currently NIJs are LOJs only, this may change in the future
			// if/when we add semijoins, anti-semijoins and relatives
			CExpressionArray *m_on_pred_conjuncts;

			// association between logical children and inner join/ON preds
			// (which of the logical children are right children of NIJs and what ON predicates are they using)
			ULongPtrArray *m_child_pred_indexes;

			// for each non-inner join (entry in m_on_pred_conjuncts), the required components on the left
			CBitSetArray *m_non_inner_join_dependencies;

			// top K elements at the top level
			KHeap *m_top_k_expressions;
			KHeapIterator *m_k_heap_iterator;

			// dummy expression to used for non-joinable components
			CExpression *m_pexprDummy;

			CMemoryPool *m_mp;

			// build expression linking given components
			CExpression *PexprBuildInnerJoinPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// extract predicate joining the two given sets
			CExpression *PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// compute cost of given join expression
			CDouble DCost(CExpression *pexpr);

			// derive stats on given expression
			virtual
			void DeriveStats(CExpression *pexpr);

			// if we need to keep track of used edges, make a map that
			// speeds up this usage check
			BOOL PopulateExpressionToEdgeMapIfNeeded();

			// add a select node with any remaining edges (predicates) that have
			// not been incorporated in the join tree
			CExpression *AddSelectNodeForRemainingEdges(CExpression *join_expr);

			// mark all the edges used in a join tree
			void RecursivelyMarkEdgesAsUsed(CExpression *expr);

			// enumerate all possible joins between the components in join_pair_bitsets on the
			// left side and those in other_join_pair_bitsets on the right
			KHeap *SearchJoinOrders(ComponentInfoArray *join_pair_bitsets, ComponentInfoArray *other_join_pair_bitsets, ULONG topK);

			// reduce a list of expressions per component down to the cheapest expression per component
			ComponentInfoArray *GetCheapestJoinExprForBitSet(KHeap *bit_exprarray_map);

			void AddJoinExprAlternativeForBitSet(CBitSet *join_bitset, CExpression *join_expr, KHeap *map);

			// create a CLogicalJoin and a CExpression to join two components
			CExpression *GetJoinExpr(SComponentInfo *left_child, SComponentInfo *right_child);

			KHeap *MergeJoinExprsForBitSet(KHeap *map, KHeap *other_map, ULONG topK);

			void AddJoinExprsForBitSet(KHeap *result_map, KHeap *candidate_map);

			// enumerate bushy joins (joins where both children are also joins) of level "current_level"
			KHeap *SearchBushyJoinOrders(ULONG current_level);

			void AddExprs(const CExpressionArray *candidate_join_exprs, CExpressionArray *result_join_exprs);

			ULONG FindLogicalChildByNijId(ULONG nij_num);


		public:

			// ctor
			CJoinOrderDPv2
				(
				CMemoryPool *mp,
				CExpressionArray *pdrgpexprComponents,
				CExpressionArray *innerJoinConjuncts,
				CExpressionArray *onPredConjuncts,
				ULongPtrArray *childPredIndexes
				);

			// dtor
			virtual
			~CJoinOrderDPv2();

			// main handler
			virtual
			CExpression *PexprExpand();

			CExpression *GetNextOfTopK();

			// check for NIJs
			BOOL
			IsRightChildOfNIJ
				(SComponentInfo *component,
				 CExpression **onPredToUse,
				 CBitSet **requiredBitsOnLeft
				);

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderDPv2

}

#endif // !GPOPT_CJoinOrderDPv2_H

// EOF
