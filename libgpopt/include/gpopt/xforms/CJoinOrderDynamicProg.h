//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CJoinOrderDynamicProg.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDynamicProg_H
#define GPOPT_CJoinOrderDynamicProg_H

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
	//		CJoinOrderDynamicProg
	//
	//	@doc:
	//		Helper class for creating join orders using dynamic programming
	//
	//---------------------------------------------------------------------------
	class CJoinOrderDynamicProg : public CJoinOrder
	{

		typedef CDynamicPtrArray<CJoinOrder::SComponent, CleanupRelease > SComponentArray;
		typedef CDynamicPtrArray<SComponentArray, CleanupRelease > SComponentArrays;
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

			// hash map from component pair to connecting edges
			typedef CHashMap<SComponentPair, CExpression, SComponentPair::HashValue, SComponentPair::Equals,
				CleanupRelease<SComponentPair>, CleanupRelease<CExpression> > ComponentPairToExpressionMap;

			// hash map from expression to cost of best join order
			typedef CHashMap<CExpression, CDouble, CExpression::HashValue, CUtils::Equals,
				CleanupRelease<CExpression>, CleanupDelete<CDouble> > ExpressionToCostMap;

			// lookup table for links
			ComponentPairToExpressionMap *m_phmcomplink;

			// dynamic programming table
			BitSetToExpressionMap *m_phmbsexpr;

			// map of expressions to its cost
			ExpressionToCostMap *m_phmexprcost;

			// array of top-k join expression
			CExpressionArray *m_pdrgpexprTopKOrders;

			// dummy expression to used for non-joinable components
			CExpression *m_pexprDummy;
		
			CBitSet *m_orig_pbs;

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

			// generate cross product for the given components
			CExpression *PexprCross(CBitSet *pbs);
		
			SComponentArray *
			GetJoinCompArray(SComponentArray *compArray, SComponentArray *compArray2, BOOL same_level);
		
			SComponent *JoinComp(SComponent *comp1, SComponent *comp2);

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

		public:

			// ctor
			CJoinOrderDynamicProg
				(
				IMemoryPool *mp,
				CExpressionArray *pdrgpexprComponents,
				CExpressionArray *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderDynamicProg();

			// main handler
			virtual
			void PexprExpand();

			// best join orders
			CExpressionArray *PdrgpexprTopK() const
			{
				return m_pdrgpexprTopKOrders;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderDynamicProg

}

#endif // !GPOPT_CJoinOrderDynamicProg_H

// EOF
