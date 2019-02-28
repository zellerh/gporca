//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynamicProg.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDynamicProg_H
#define GPOPT_CXformExpandNAryJoinDynamicProg_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinDynamicProg
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinDynamicProg : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinDynamicProg(const CXformExpandNAryJoinDynamicProg &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinDynamicProg(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinDynamicProg()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinDynamicProg;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinDynamicProg";
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

	}; // class CXformExpandNAryJoinDynamicProg

}


#endif // !GPOPT_CXformExpandNAryJoinDynamicProg_H

// EOF
