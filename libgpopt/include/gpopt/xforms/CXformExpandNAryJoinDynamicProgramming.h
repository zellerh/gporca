//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynamicProgramming.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDynamicProgramming_H
#define GPOPT_CXformExpandNAryJoinDynamicProgramming_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinDynamicProgramming
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinDynamicProgramming : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinDynamicProgramming(const CXformExpandNAryJoinDynamicProgramming &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinDynamicProgramming(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinDynamicProgramming()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinDynamicProgramming;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinDynamicProgramming";
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

	}; // class CXformExpandNAryJoinDynamicProgramming

}


#endif // !GPOPT_CXformExpandNAryJoinDynamicProgramming_H

// EOF
