//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynamicProg2.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDynamicProg2_H
#define GPOPT_CJoinOrderDynamicProg2_H

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
	//		CJoinOrderDynamicProg2
	//
	//	@doc:
	//		Helper class for creating join orders using dynamic programming
	//
	//---------------------------------------------------------------------------
	class CJoinOrderDynamicProg2 : public CJoinOrder
	{

		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SComponentPair
			//
			//	@doc:
			//		Struct to capture a pair of components
			//
			//---------------------------------------------------------------------------
			struct SComponentPair : public CRefCount
			{
				// first component
				CBitSet *m_pbsFst;

				// second component
				CBitSet *m_pbsSnd;

				// ctor
				SComponentPair(CBitSet *pbsFst, CBitSet *pbsSnd);

				// dtor
				~SComponentPair();

				// hashing function
				static
				ULONG HashValue(const SComponentPair *pcomppair);

				// equality function
				static
				BOOL Equals(const SComponentPair *pcomppairFst, const SComponentPair *pcomppairSnd);
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

			// hash map from component to best join order
			typedef CHashMap<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
				CleanupRelease<CBitSet>, CleanupRelease<CExpression> > BitSetToExpressionMap;
		
			// hash map from component to best join order
			typedef CHashMapIter<CBitSet, CExpression, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpression> > BitSetToExpressionMapIter;
		
			// hash map from component to best join order
			typedef CHashMap<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMap;

		// hash map from component to best join order
		typedef CHashMapIter<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
		CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMapIter;
			// hash map from component pair to connecting edges
			typedef CHashMap<SComponentPair, CExpression, SComponentPair::HashValue, SComponentPair::Equals,
				CleanupRelease<SComponentPair>, CleanupRelease<CExpression> > ComponentPairToExpressionMap;

			// hash map from expression to cost of best join order
			typedef CHashMap<CExpression, CDouble, CExpression::HashValue, CUtils::Equals,
				CleanupRelease<CExpression>, CleanupDelete<CDouble> > ExpressionToCostMap;
		
			// dynamic array of bitsets
			typedef CDynamicPtrArray<CBitSetArray, CleanupRelease> CBitSetArrays;

			// lookup table for links
			ComponentPairToExpressionMap *m_phmcomplink;

			// dynamic programming table
			BitSetToExpressionMap *m_phmbsexpr;
		
			BitSetToExpressionArrayMap *m_bitsetToExprArray;

			// map of expressions to its cost
			ExpressionToCostMap *m_phmexprcost;

			// array of top-k join expression
			CExpressionArray *m_pdrgpexprTopKOrders;

			// dummy expression to used for non-joinable components
			CExpression *m_pexprDummy;

			// build expression linking given components
			CExpression *PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// lookup best join order for given set
			CExpression *PexprLookup(CBitSet *pbs);

			// extract predicate joining the two given sets
			CExpression *PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// join expressions in the given two sets
			CExpression *PexprJoin(CBitSet *pbsFst, CBitSet *pbsSnd);

			// join expressions in the given set
			CExpression *PexprJoin(CBitSet *pbs);

			// return a subset of the given set covered by one or more edges
			CBitSet *PbsCovered(CBitSet *pbsInput);

			// add given join order to best results
			void AddJoinOrder(CExpression *pexprJoin, CDouble dCost);

			// compute cost of given join expression
			CDouble DCost(CExpression *pexpr);

			// derive stats on given expression
			virtual
			void DeriveStats(CExpression *pexpr);

			// add expression to cost map
			void InsertExpressionCost(CExpression *pexpr, CDouble dCost, BOOL fValidateInsert);
		
			BitSetToExpressionArrayMap *SearchJoinOrder(CBitSetArray *pbsFirst, CBitSetArray *pbsSecond, BOOL same_level);
		
		BitSetToExpressionMap*
		GetCheapest
		(
		 BitSetToExpressionArrayMap *bit_exprarray_map
		 );
		
		void
		AddExprAlternativeToBitSetMap
		(
		 CBitSet *pbs,
		 CExpression *expr,
		 BitSetToExpressionArrayMap *bitsetToExprArray
		 );
		
		BitSetToExpressionArrayMap *
		MergeAlternatives
		(
		 BitSetToExpressionMap *map1,
		 BitSetToExpressionMap *map2
		 );
		
		void
		InsertExpressionCost
		(
		 CExpression *pexpr,
		 CDouble dCost,
		 BOOL fValidateInsert, // if true, insertion must succeed
		 ExpressionToCostMap *phmexprcost
		 );
		
		
		CExpression *
		JoinComp
		(
		 CBitSet *pbsFirst,
		 CBitSet *pbsSecond
		 );
		
		void
		AddExprFromMap
		(
		 BitSetToExpressionArrayMap *bit_expr_map
		 );
		
		BitSetToExpressionArrayMap *
		MergeAlternatives
		(
		 BitSetToExpressionArrayMap *map_a,
		 BitSetToExpressionArrayMap *map_b
		 );
		
		CBitSetArray *
		GetThisLevelArray
		(
		 BitSetToExpressionMap *cheapset_map
		 );
		
		void
		AddExprArrayAlternativesToMap
		(
		 BitSetToExpressionArrayMap *final_map,
		 BitSetToExpressionArrayMap *mapToAdd
		 );
		
		BitSetToExpressionArrayMap *
		GetBushyMaps
		(
		 ULONG level,
		 CBitSetArrays *join_levels
		 );

		public:

			// ctor
			CJoinOrderDynamicProg2
				(
				IMemoryPool *mp,
				CExpressionArray *pdrgpexprComponents,
				CExpressionArray *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderDynamicProg2();

			// main handler
			virtual
			CExpression *PexprExpand();

			// best join orders
			CExpressionArray *PdrgpexprTopK() const
			{
				return m_pdrgpexprTopKOrders;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderDynamicProg2

}

#endif // !GPOPT_CJoinOrderDynamicProg2_H

// EOF
