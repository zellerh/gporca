//---------------------------------------------------------------------------

//  Greenplum Database
//  Copyright (C) 2018 Pivotal Inc.
//
//  @filename:
//      CXformEagerAgg.h
//
//  @doc:
//      Eagerly push aggregates below join when there is no primary/foreign keys
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformEagerAgg_H
#define GPOPT_CXformEagerAgg_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//  @class:
	//      CXformEagerAgg
	//
	//  @doc:
	//      Eagerly push aggregates below join when there is no primary/foreign keys
	//
	//---------------------------------------------------------------------------
	class CXformEagerAgg : public CXformExploration
	{

		private:

			// private copy ctor
			CXformEagerAgg(const CXformEagerAgg &);


		public:

			// ctor
			explicit
			CXformEagerAgg(IMemoryPool *mp);

			// ctor
			explicit
			CXformEagerAgg(CExpression *pexprPattern);

			// dtor
			virtual
			~CXformEagerAgg()
			{}

			// ident accessors
			virtual
			EXformId Exfid() const
			{
				return ExfEagerAgg;
			}

			virtual
			const CHAR *SzId() const
			{
				return "CXformEagerAgg";
			}

			// compatibility function for eager aggregation
			virtual
			BOOL FCompatible(CXform::EXformId exfid)
			{
				return (CXform::ExfEagerAgg != exfid) &&
							(CXform::ExfSplitGbAgg != exfid) &&
 							(CXform::ExfSplitDQA != exfid);
			}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &exprhdl) const;

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const;

			// check if transform can be applied
			BOOL CanApplyTransform(CExpression *pexprAgg, CColRefSet *push_down_gb_crs) const;

			// is this aggregate supported for push down?
			BOOL CanPushAggBelowJoin(CExpression *scalar_agg_func_expr) const;

			// generate project lists for the lower and upper aggregates
			// from the original aggregate
			void PopulateLowerUpperProjectList
			(
			 IMemoryPool *mp,               // memory pool
			 CExpression *orig_proj_list,   // project list of the original aggregate
			 CExpression **lower_proj_list, // project list of the new lower aggregate
			 CExpression **upper_proj_list  // project list of the new upper aggregate
			) const;

			// return true if xform should be applied only once
			virtual
			BOOL IsApplyOnce(){
				return true;
			 };
	}; // class CXformEagerAgg

}

#endif // !GPOPT_CXformEagerAgg_H

// EOF
