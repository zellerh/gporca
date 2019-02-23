//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformExpandNAryJoinDynProg.h
//
//	@doc:
//		Expand n-ary join into series of binary joins using dynamic
//		programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformExpandNAryJoinDynProg_H
#define GPOPT_CXformExpandNAryJoinDynProg_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformExpandNAryJoinDynProg
	//
	//	@doc:
	//		Expand n-ary join into series of binary joins using dynamic
	//		programming
	//
	//---------------------------------------------------------------------------
	class CXformExpandNAryJoinDynProg : public CXformExploration
	{

		private:

			// private copy ctor
			CXformExpandNAryJoinDynProg(const CXformExpandNAryJoinDynProg &);

		public:

			// ctor
			explicit
			CXformExpandNAryJoinDynProg(IMemoryPool *mp);

			// dtor
			virtual
			~CXformExpandNAryJoinDynProg()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfExpandNAryJoinDynProg;
			}

			// return a string for xform name
			virtual
			const CHAR *SzId() const
			{
				return "CXformExpandNAryJoinDynProg";
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

	}; // class CXformExpandNAryJoinDynProg

}


#endif // !GPOPT_CXformExpandNAryJoinDynProg_H

// EOF
