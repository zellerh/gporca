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
	//		Some terminology:
	//
	//		NIJ:	Non-inner join. This is sometimes used instead of left join,
	//				since we anticipate to extend this code to semi-joins and other
	//				types of joins.
	//		Atom:	A child of the NAry join. This could be a table or some
	//				other operator like a groupby or a full outer join.
	//		Group:	A set of atoms (called a "component" in DPv1). Once we generate
	//				a result expression, each of these sets will be associated
	//				with a CGroup in MEMO.
	//---------------------------------------------------------------------------
	class CJoinOrderDPv2 : public CJoinOrder
	{

		private:

			// a heap keeping the k lowest-cost objects in an array of class A
			// A is a CDynamicPtrArray
			// E is the entry type of the array and it has a method CDouble DCost()
			// See https://en.wikipedia.org/wiki/Binary_heap for details
		    template<class A, class E>
			class KHeap : public CRefCount
			{
			private:

				CJoinOrderDPv2 *m_join_order;
				A *m_topk;
				CMemoryPool *m_mp;
				ULONG m_k;
				BOOL m_is_heapified;
				ULONG m_num_returned;

				// the parent index is (ix-1)/2, except for 0
				ULONG parent(ULONG ix) { return (0 < ix ? (ix-1)/2 : m_topk->Size()); }

				// children are at indexes 2*ix + 1 and 2*ix + 2
				ULONG left_child(ULONG ix)  { return 2*ix + 1; }
				ULONG right_child(ULONG ix) { return 2*ix + 2; }

				// does the parent/child exist?
				BOOL exists(ULONG ix) { return ix < m_topk->Size(); }
				// cost of an entry (this class implements a Min-Heap)
				CDouble cost(ULONG ix) { return (*m_topk)[ix]->DCost(); }

				// push node ix in the tree down into its child tree as much as needed
				void HeapifyDown(ULONG ix)
				{
					ULONG left_child_ix = left_child(ix);
					ULONG right_child_ix = right_child(ix);
					ULONG min_element_ix = ix;

					if (exists(left_child_ix) && cost(left_child_ix) < cost(ix))
						// left child is better than parent, it becomes the new candidate
						min_element_ix = left_child_ix;

					if (exists(right_child_ix) && cost(right_child_ix) < cost(min_element_ix))
						// right child is better than min(parent, left child)
						min_element_ix = right_child_ix;

					if (min_element_ix != ix)
					{
						// make the lowest of { parent, left child, right child } the new root
						m_topk->Swap(ix, min_element_ix);
						HeapifyDown(min_element_ix);
					}
				}

				// pull node ix in the tree up as much as needed
				void HeapifyUp(ULONG ix)
				{
					ULONG parent_ix = parent(ix);

					if (!exists(parent_ix))
						return;

					if (cost(ix) < cost(parent_ix))
					{
						m_topk->Swap(ix, parent_ix);
						HeapifyUp(parent_ix);
					}
				}

				// convert the array into a heap, heapify-down all interior nodes of the tree, bottom-up
				void Heapify()
				{
					// the parent of the last node is the last node in the tree that is a parent
					ULONG start_ix = parent(m_topk->Size()-1);

					// now work our way up to the root, calling HeapifyDown
					for (ULONG ix=start_ix; exists(ix); ix--)
						HeapifyDown(ix);

					m_is_heapified = true;
				}

			public:

				KHeap(CMemoryPool *mp, CJoinOrderDPv2 *join_order, ULONG k)
				:
				m_join_order(join_order),
				m_mp(mp),
				m_k(k),
				m_is_heapified(false),
				m_num_returned(0)
				{
					m_topk = GPOS_NEW(m_mp) A(m_mp);
				}

				~KHeap()
				{
					m_topk->Release();
				}

				void Insert(E *elem)
				{
					GPOS_ASSERT(NULL != elem);
					// since the cost may change as we find more expressions in the group,
					// we just append to the array now and heapify at the end
					GPOS_ASSERT(!m_is_heapified);
					m_topk->Append(elem);

					// this is dead code at the moment, but other users might want to
					// heapify and then insert additional items
					if (m_is_heapified)
					{
						HeapifyUp(m_topk->Size()-1);
					}
				}

				E *RemoveBestElement()
				{
					if (0 == m_topk->Size() || m_k <= m_num_returned)
					{
						return NULL;
					}

					m_num_returned++;

					if (!m_is_heapified)
						Heapify();

					// we want to remove and return the root of the tree, which is the best element

					// first, swap the root with the last element in the array
					m_topk->Swap(0, m_topk->Size()-1);

					// now remove the new last element, which is the real root
					E * result = m_topk->RemoveLast();

					// then push the new first element down to the correct place
					HeapifyDown(0);

					return result;
				}

				ULONG Size()
				{
					return m_topk->Size();
				}

				BOOL IsLimitExceeded()
				{
					return m_topk->Size() + m_num_returned > m_k;
				}

				void Clear()
				{
					m_topk->Clear();
					m_is_heapified = false;
					m_num_returned = 0;
				}

			};

			// forward declaration, circular reference
			struct SGroupInfo;

			// join order properties, these can be added if an expression satisfies multiple such properties
			// consider these as constants, not as a true enum
			enum JoinOrderPropType
			{
				EJoinOrderNone     = 0,
				EJoinOrderMincard  = 1,
				EJoinOrderStats    = 2
			};

			struct SExpressionProperties
			{
				ULONG m_join_order;

				SExpressionProperties(ULONG join_order_properties) :
						m_join_order(join_order_properties)
				{}
			};

			// description of an expression in the DP environment,
			// left and right child of join expressions point to
			// other groups, similar to a CGroupExpression
			struct SExpressionInfo : public CRefCount
			{
				// the expression
				CExpression *m_expr;

				// left/right child group info (group for left/right child of m_best_expr),
				// we do not keep a refcount for these
				SGroupInfo *m_left_child_group;
				SGroupInfo *m_right_child_group;

				SExpressionProperties m_properties;

				// in the future, we may add properties relevant to the cost here,
				// like distribution key, partition selectors

				// cost of the expression
				CDouble m_cost;

				SExpressionInfo(
								CExpression *expr,
								SGroupInfo *left_child_group_info,
								SGroupInfo *right_child_group_info,
								SExpressionProperties &properties
							   ) : m_expr(expr),
								   m_left_child_group(left_child_group_info),
								   m_right_child_group(right_child_group_info),
								   m_properties(properties),
								   m_cost(0.0)
				{
				}

				~SExpressionInfo()
				{
					m_expr->Release();
				}

				CDouble DCost() { return m_cost; }

			};

			typedef CDynamicPtrArray<SExpressionInfo, CleanupRelease<SExpressionInfo> > SExpressionInfoArray;

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
					// the set of atoms, this uniquely identifies the group
					CBitSet *m_atoms;
					// infos of the best (lowest cost) expressions (so far, if at the current level)
					// for each interesting property
					SExpressionInfoArray *m_best_expr_info_array;
					CDouble m_cardinality;

					SGroupInfo(CMemoryPool *mp,
							   CBitSet *atoms
							  ) : m_atoms(atoms),
								  m_cardinality(0.0)
					{
						m_best_expr_info_array = GPOS_NEW(mp) SExpressionInfoArray(mp);
					}

					~SGroupInfo()
					{
						m_atoms->Release();
						m_best_expr_info_array->Release();
					}

					BOOL IsAnAtom() { return 1 == m_atoms->Size(); }
				};

			// dynamic array of SGroupInfo, where each index represents an alternative group of a given level k
			typedef CDynamicPtrArray<SGroupInfo, CleanupRelease<SGroupInfo> > SGroupInfoArray;

			// info for a join level, the set of all groups representing <m_level>-way joins
			struct SLevelInfo : public CRefCount
				{
					ULONG m_level;
					SGroupInfoArray *m_groups;
					KHeap<SGroupInfoArray, SGroupInfo> *m_top_k_groups;

					SLevelInfo(ULONG level, SGroupInfoArray *groups) :
					m_level(level),
					m_groups(groups),
					m_top_k_groups(NULL)
					{}

					~SLevelInfo()
					{
						m_groups->Release();
						CRefCount::SafeRelease(m_top_k_groups);
					}

					BOOL IsLimited() { return NULL != m_top_k_groups; }
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

			// dynamic array of SLevelInfos, where each index represents the level
			typedef CDynamicPtrArray<SLevelInfo, CleanupRelease<SLevelInfo> > DPv2Levels;

			// an array of an array of groups, organized by level at the first array dimension,
			// main data structure for dynamic programming
			DPv2Levels *m_join_levels;

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

			// top K expressions at the top level
			KHeap<SExpressionInfoArray, SExpressionInfo> *m_top_k_expressions;

			CMemoryPool *m_mp;

			SLevelInfo *Level(ULONG l) { return (*m_join_levels)[l]; }

			// build expression linking given groups
			CExpression *PexprBuildInnerJoinPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// compute cost of a join expression in a group
			CDouble ComputeCost(SExpressionInfo *expr_info,
								SExpressionInfo *left_child_expr_info,
								SExpressionInfo *right_child_expr_info,
								SGroupInfo *group_info
							   );

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

			virtual
			void DeriveStats(CExpression *pexpr);

			// create a CLogicalJoin and a CExpression to join two groups
			SExpressionInfo *GetJoinExpr(
										 SGroupInfo *left_child,
										 SGroupInfo *right_child,
										 SExpressionProperties &requiredProperties
										);

			// does "prop" provide all the properties of "other_prop" plus maybe more?
			BOOL IsASupersetOfProperties(SExpressionProperties &prop, SExpressionProperties &other_prop);

			// is one of the properties a subset of the other or are they disjoint?
			BOOL ArePropertiesDisjoint(SExpressionProperties &prop, SExpressionProperties &other_prop);

			// get best expression in a group for a given set of properties
			SExpressionInfo *GetBestExprForProperties(SGroupInfo *group_info, SExpressionProperties &props);

			// add a new expression to a group, unless there already is an existing expression that dominates it
			void AddExprToGroupIfNecessary(SGroupInfo *group_info, SExpressionInfo *new_expr_info);

			// enumerate bushy joins (joins where both children are also joins) of level "current_level"
			void SearchBushyJoinOrders(ULONG current_level);

			void AddExprs(const CExpressionArray *candidate_join_exprs, CExpressionArray *result_join_exprs);
			SGroupInfo *AddGroupInfo(SLevelInfo *levelInfo, CBitSet *atoms, SExpressionInfo *expr_for_stats);
			void FinalizeLevel(ULONG level);

			SGroupInfoArray *GetGroupsForLevel(ULONG level) const
			{ return (*m_join_levels)[level]->m_groups; }

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

#ifdef GPOS_DEBUG
			void
			DbgPrint();
#endif

	}; // class CJoinOrderDPv2

}

#endif // !GPOPT_CJoinOrderDPv2_H

// EOF
