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

			// forward declaration, circular reference
			struct SGroupInfo;

			// description of an expression in the DP environment,
			// left and right child of join expressions point to
			// other groups, similar to a CGroupExpression
			struct SExpressionInfo : public CRefCount
			{
				// best expression (so far) for this group
				CExpression *m_best_expr;

				// left/right child group info (group for left/right child of m_best_expr)
				const SGroupInfo *m_left_child_group;
				const SGroupInfo *m_right_child_group;

				// in the future, we may add properties relevant to the cost here,
				// like distribution key, partition selectors

				// cost of the best expression (so far)
				CDouble m_cost;

				SExpressionInfo(
								CExpression *expr,
								const SGroupInfo *left_child_group_info,
								const SGroupInfo *right_child_group_info
							   ) : m_best_expr(expr),
								   m_left_child_group(left_child_group_info),
								   m_right_child_group(right_child_group_info),
								   m_cost(0.0)
				{
				}

				~SExpressionInfo()
				{
					m_best_expr->Release();
				}

			};

			//---------------------------------------------------------------------------
			//	@struct:
			//		SGroupInfo
			//
			//	@doc:
			//		Struct containing a bitset, representing a group, its best expression, and cost
			//
			//---------------------------------------------------------------------------
			struct SGroupInfo : public CRefCount
				{
					CBitSet *m_atoms;
					CExpression *m_expr_for_stats;
					SExpressionInfo *m_expr_info;
					// future: have a list or map of SExpressionInfos
					// each with a different property

					SGroupInfo(CBitSet *atoms,
							   SExpressionInfo *first_expr_info
							  ) : m_atoms(atoms),
								  m_expr_for_stats(NULL),
								  m_expr_info(first_expr_info)
					{
					}

					~SGroupInfo()
					{
						m_atoms->Release();
						CRefCount::SafeRelease(m_expr_for_stats);
						m_expr_info->Release();
					}

					BOOL IsAnAtom() { return 1 == m_atoms->Size(); }

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

			typedef CHashMap<CExpression, SEdge, CExpression::HashValue, CUtils::Equals,
			CleanupRelease<CExpression>, CleanupRelease<SEdge> > ExpressionToEdgeMap;

			// dynamic array of SGroupInfos
			typedef CHashMap<CBitSet, SGroupInfo, UlHashBitSet, FEqualBitSet, CleanupRelease<CBitSet>, CleanupRelease<SGroupInfo> > BitSetToGroupInfoMap;

			// iterator over group infos in a level
			typedef CHashMapIter<CBitSet, SGroupInfo, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<SGroupInfo> > BitSetToGroupInfoMapIter;

			// dynamic array of SGroupInfo, where each index represents an alternative group of a given level k
			typedef CDynamicPtrArray<SGroupInfo, CleanupRelease<SGroupInfo> > GroupInfoArray;

			// dynamic array of GroupInfoArrays, where each index represents the level
			typedef CDynamicPtrArray<GroupInfoArray, CleanupRelease<GroupInfoArray> > DPv2Levels;
/*
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
*/
			// an array of an array of groups, organized by level at the first array dimension,
			// main data structure for dynamic programming
			DPv2Levels *m_join_levels;

			// limits for the number of bitsets (groups) per level
			ULONG *m_top_k_group_limits;

			// map to find the associated edge in the join graph from a join predicate
			ExpressionToEdgeMap *m_expression_to_edge_map;

			// map to check whether a DPv2 group already exists
			BitSetToGroupInfoMap *m_bitset_to_group_info_map;

			// ON predicates for NIJs (non-inner joins, e.g. LOJs)
			// currently NIJs are LOJs only, this may change in the future
			// if/when we add semijoins, anti-semijoins and relatives
			CExpressionArray *m_on_pred_conjuncts;

			// association between logical children and inner join/ON preds
			// (which of the logical children are right children of NIJs and what ON predicates are they using)
			ULongPtrArray *m_child_pred_indexes;

			// for each non-inner join (entry in m_on_pred_conjuncts), the required atoms on the left
			CBitSetArray *m_non_inner_join_dependencies;

			// top K elements at the top level
//			KHeap *m_top_k_expressions;
//			KHeapIterator *m_k_heap_iterator;
			ULONG m_top_k_index;

			CMemoryPool *m_mp;

			// build expression linking given groups
			CExpression *PexprBuildInnerJoinPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// compute cost of a join expression in a group
			CDouble DCost(SGroupInfo *group, const SGroupInfo *leftChildGroup, const SGroupInfo *rightChildGroup);

			// if we need to keep track of used edges, make a map that
			// speeds up this usage check
			BOOL PopulateExpressionToEdgeMapIfNeeded();

			// add a select node with any remaining edges (predicates) that have
			// not been incorporated in the join tree
			CExpression *AddSelectNodeForRemainingEdges(CExpression *join_expr);

			// mark all the edges used in a join tree
			void RecursivelyMarkEdgesAsUsed(CExpression *expr);

			// enumerate all possible joins between left_level-way joins on the left side
			// and right_level-way joins on the right side, resulting in left_level + right_level-way joins
			void SearchJoinOrders(ULONG left_level, ULONG right_level);

			// reduce a list of expressions per component down to the cheapest expression per component
			//ComponentInfoArray *GetCheapestJoinExprForBitSet(KHeap *bit_exprarray_map);

			// create a CLogicalJoin and a CExpression to join two groups
			CExpression *GetJoinExpr(SGroupInfo *left_child, SGroupInfo *right_child);

			// enumerate bushy joins (joins where both children are also joins) of level "current_level"
			void SearchBushyJoinOrders(ULONG current_level);

			void AddExprs(const CExpressionArray *candidate_join_exprs, CExpressionArray *result_join_exprs);
			void AddGroupInfo(SGroupInfo *groupInfo);
			// TODO: void ReplaceGroupInfo(SGroupInfo *newGroupInfo, SGroupInfo *oldGroupInfo);

			ULONG FindLogicalChildByNijId(ULONG nij_num);
			static
			ULONG NChooseK(ULONG n, ULONG k);
			BOOL LevelIsFull(ULONG level);


		public:

			// ctor
			CJoinOrderDPv2
				(
				CMemoryPool *mp,
				CExpressionArray *pdrgpexprAtoms,
				CExpressionArray *innerJoinConjuncts,
				CExpressionArray *onPredConjuncts,
				ULongPtrArray *childPredIndexes
				);

			// dtor
			virtual
			~CJoinOrderDPv2();

			// main handler
			virtual
			void PexprExpand();

			CExpression *GetNextOfTopK();

			// check for NIJs
			BOOL
			IsRightChildOfNIJ
				(SGroupInfo *groupInfo,
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
