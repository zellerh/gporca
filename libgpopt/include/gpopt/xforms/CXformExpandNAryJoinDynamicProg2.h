//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynamicProg2.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDynamicProg2_H
#define GPOPT_CXformExpandNAryJoinDynamicProg2_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinDynamicProg2
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinDynamicProg2 : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinDynamicProg2(const CXformExpandNAryJoinDynamicProg2 &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinDynamicProg2(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinDynamicProg2()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinDynamicProg2;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinDynamicProg2";
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

	}; // class CXformExpandNAryJoinDynamicProg2

}


#endif // !GPOPT_CXformExpandNAryJoinDynamicProg2_H

// EOF
