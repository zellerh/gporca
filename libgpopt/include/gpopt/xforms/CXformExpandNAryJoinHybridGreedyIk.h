//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinHybridGreedyIk.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinHybridGreedyIk_H
#define GPOPT_CXformExpandNAryJoinHybridGreedyIk_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinHybridGreedyIk
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinHybridGreedyIk : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinHybridGreedyIk(const CXformExpandNAryJoinHybridGreedyIk &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinHybridGreedyIk(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinHybridGreedyIk()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinHybridGreedyIk;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinHybridGreedyIk";
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// do stats need to be computed before applying xform?
			virtual
			BOOL FNeedsStats() const
			{
				return true;
			}

			// actual transform
			void Transform
					(
					CXformContext *pxfctxt,
					CXformResult *pxfres,
					CExpression *pexpr
					) const;

	}; // class CXformExpandNAryJoinHybridGreedyIk

}


#endif // !GPOPT_CXformExpandNAryJoinHybridGreedyIk_H

// EOF
