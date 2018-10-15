//---------------------------------------------------------------------------i
//  Greenplum Database
//  Copyright (C) 2018 Pivotal Inc.
//
//  @filename:
//      CXformEagerAgg.cpp
//
//  @doc:
//      Implementation for eagerly pushing aggregates below join
//          (with no foreign key restriction on the join condition)
//---------------------------------------------------------------------------
#include "gpos/base.h"

#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/CMDScalarOpGPDB.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CXformEagerAgg.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/base/CColRefSetIter.h"

using namespace gpopt;
using namespace gpmd;

// ctor
CXformEagerAgg::CXformEagerAgg
	(
	IMemoryPool *mp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(mp) CExpression
			(
			mp,
			GPOS_NEW(mp) CLogicalGbAgg(mp),
			GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalInnerJoin(mp),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // join outer child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)), // join inner child
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // join predicate
				),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // scalar project list
			)
		)
{}

// ctor
CXformEagerAgg::CXformEagerAgg
	(
	CExpression *pexprPattern
	)
	:
	CXformExploration(pexprPattern)
{}

// compute xform promise for a given expression handle;
// we only push down global aggregates
CXform::EXformPromise
CXformEagerAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popGbAgg->FGlobal())
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}

// actual transformation
void
CXformEagerAgg::Transform
	(
	CXformContext *pxf_ctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxf_ctxt);
	GPOS_ASSERT(FPromising(pxf_ctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *mp = pxf_ctxt->Pmp();
    /*
         1. check if aggregate columns is part of single child col ref
    */
    // Get col ref set of projection list (columns used in aggregation)
    CExpression *join_expr = (*pexpr)[0];
    CExpression *proj_list_expr = (*pexpr)[1];
    CExpression *outer_child_expr = (*join_expr)[0];
    CExpression *inner_child_expr = (*join_expr)[1];
    CExpression *scalar_expr = (*join_expr)[2];

    /*   2. get grouping columns and join predicate columns into a new set  */
    CLogicalGbAgg *gb_agg_op = CLogicalGbAgg::PopConvert(pexpr->Pop());
    CColRefSet *grouping_crs = gb_agg_op->PcrsLocalUsed();
    CColRefSet *push_down_gb_crs = GPOS_NEW(mp) CColRefSet
                                                (
                                                 mp,
                                                 *(CDrvdPropScalar::GetDrvdScalarProps(
                                                     scalar_expr->PdpDerive())->PcrsUsed())
                                                );
    push_down_gb_crs->Union(grouping_crs);
    if (!FApplicable(pexpr, push_down_gb_crs))
    {
        push_down_gb_crs->Release();
        return;
    }
	/* 3. only keep columns from push down child in new grouping col set */
	CColRefSet *outer_child_crs =  CDrvdPropRelational::GetRelationalProperties(
														outer_child_expr->PdpDerive())->PcrsOutput();
	push_down_gb_crs->Intersection(outer_child_crs);

	/* 4. Create new project lists for the two new Gb aggregates */
	CExpression *lower_expr_proj_list = NULL;
	CExpression *upper_expr_proj_list = NULL;
	(void) PopulateLowerUpperProjectList
			(
			 mp,
			 proj_list_expr,
			 &lower_expr_proj_list,
			 &upper_expr_proj_list
			 );

	/* 5. Create lower agg, join, and upper agg expressions */

	// lower agg expression
	outer_child_expr->AddRef();
	CColRefArray *push_down_gb_cra = push_down_gb_crs->Pdrgpcr(mp);
	CExpression *lower_agg_expr = GPOS_NEW(mp) CExpression
												(
												 mp,
												 GPOS_NEW(mp) CLogicalGbAgg
												 (
												  mp,
												  push_down_gb_cra,
												  COperator::EgbaggtypeGlobal
												 ),
												 outer_child_expr,
												 lower_expr_proj_list
												 );

	// join expression
	COperator *join_operator = join_expr->Pop();
	join_operator->AddRef();
	inner_child_expr->AddRef();
	scalar_expr->AddRef();
	CExpression *new_join_expr = GPOS_NEW(mp) CExpression
											  (
											   mp,
											   join_operator,
											   lower_agg_expr,
											   inner_child_expr,
											   scalar_expr
											   );

	// upper agg expression
	CColRefArray *grouping_cra = grouping_crs->Pdrgpcr(mp);
	CExpression *upper_agg_expr = GPOS_NEW(mp) CExpression
												(
												 mp,
												 GPOS_NEW(mp) CLogicalGbAgg
													 (
													  mp,
													  grouping_cra,
													  COperator::EgbaggtypeGlobal
													  ),
												 new_join_expr,
												 upper_expr_proj_list
												 );

	push_down_gb_crs->Release();
	pxfres->Add(upper_agg_expr);
}

BOOL CXformEagerAgg::IsAggSupported
(
	CExpression *scalar_agg_func_expr
)
const
{
    CScalarAggFunc *scalar_agg_func = CScalarAggFunc::PopConvert(scalar_agg_func_expr->Pop());
    COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
    CMDAccessor *md_accessor = poctxt->Pmda();
    IMDId *agg_mdid = scalar_agg_func->MDId();  // oid of the original aggregate function

    if (scalar_agg_func_expr->Arity() != 1)
    {
        // currently count() is not supported since it's not possible to
        // distinguish it from a dummy aggregate with no child expression
        return false;
    }

    CExpression *agg_child_expr = (*scalar_agg_func_expr)[0];
    IMDId *agg_child_mdid = CScalar::PopConvert(agg_child_expr->Pop())->MdidType();
    const IMDType *agg_child_type = md_accessor->RetrieveType(agg_child_mdid);

    if (! (agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggMin))) &&
        ! (agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggMax))))
    {
        return false;
    }

    // currently not supporting DQA
    if (scalar_agg_func->IsDistinct())
    {
        return false;
    }
    return true;
}

// Check if the transform can be applied
//   Eager agg is currently applied only if following is true:
//     - Inner join of two relations
//     - Single aggregate (MIN or MAX)
//     - Aggregate is not a DQA
//     - Single expression input in the agg
//     - Input expression only part of outer child
BOOL
CXformEagerAgg::FApplicable
(
 CExpression *pexpr,
 CColRefSet *push_down_gb_crs
)
const
{
	CExpression *join_expr = (*pexpr)[0];
	CExpression *proj_list_expr = (*pexpr)[1];
	CExpression *outer_child_expr = (*join_expr)[0];

    // currently only supporting aggregate column references from outer child //
    CColRefSet *outer_child_crs = CDrvdPropRelational::GetRelationalProperties(
                                   outer_child_expr->PdpDerive())->PcrsOutput();
    CColRefSet *proj_list_crs = CDrvdPropScalar::GetDrvdScalarProps(
                                proj_list_expr->PdpDerive())->PcrsUsed();
    if (!outer_child_crs->ContainsAll(proj_list_crs))
    {
        return false;
    }
    BOOL isSupported = false;
	ULONG arity = proj_list_expr->Arity();
	if (arity == 0)
    {
        return false;
    }
    for (ULONG ul = 0; ul < arity; ul++)
    {
        // currently only supporting single-input aggregates //
		// scalar aggregate function is the lone child of the project element

        // currently only supporting MIN/MAX aggregate function //
		CExpression *scalar_agg_proj_expr = (*proj_list_expr)[ul];
        BOOL is_agg_supported = IsAggSupported((*scalar_agg_proj_expr)[0]);
        if (is_agg_supported)
        {
            isSupported = true;
        } else
        {
            // An unsupported agg can only be applied on the grouping columns
            // of the aggregate being pushed down
            CColRefSet *scalar_agg_crs = CDrvdPropScalar::GetDrvdScalarProps(
                                        scalar_agg_proj_expr->PdpDerive())->PcrsUsed();
            if (!push_down_gb_crs->ContainsAll(scalar_agg_crs)){
                return false;
            }
        }
    }
	return isSupported;
}

// populate the lower and upper aggregate's project list after
// splitting and pushing down an aggregate.
void
CXformEagerAgg::PopulateLowerUpperProjectList
(
 IMemoryPool *mp, // memory pool
 CExpression *orig_proj_list,       // project list of the original global aggregate
 CExpression **lower_proj_list, // project list of the new lower aggregate
 CExpression **upper_proj_list // project list of the new upper aggregate
 ) const
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// building an array of project elements for the new lower and upper aggregates
	CExpressionArray *lower_proj_elem_array = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *upper_proj_elem_array = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = orig_proj_list->Arity();

	// loop over each project element
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *orig_proj_elem_expr = (*orig_proj_list)[ul];
		CScalarProjectElement *orig_proj_elem =
		CScalarProjectElement::PopConvert(orig_proj_elem_expr->Pop());

		CExpression *orig_agg_expr = (*orig_proj_elem_expr)[0];
        if (IsAggSupported(orig_agg_expr))
        {
        	CScalarAggFunc *orig_agg_func = CScalarAggFunc::PopConvert(orig_agg_expr->Pop());
            IMDId *orig_agg_mdid = orig_agg_func->MDId();

            /*  1. create new aggregate function for the lower aggregate operator   */
            orig_agg_mdid->AddRef();
            CScalarAggFunc *lower_agg_func  = CUtils::PopAggFunc
                (
                 mp,
                 orig_agg_mdid,
                 GPOS_NEW(mp) CWStringConst(mp, orig_agg_func->PstrAggFunc()->GetBuffer()),
                 orig_agg_func->IsDistinct(),
                 EaggfuncstageGlobal, /* fGlobal */
                 false /* fSplit */
                 );
            // add the arguments for the lower aggregate function, which is
            // going to be the same as the original aggregate function
            CExpressionArray *orig_agg_arg_array = orig_agg_expr->PdrgPexpr();
            orig_agg_arg_array->AddRef();
            CExpression *lower_agg_expr = GPOS_NEW(mp) CExpression
                                                        (
                                                        mp,
                                                        lower_agg_func,
                                                        orig_agg_arg_array
                                                        );

            /* 2. create new aggregate function for the upper aggregate operator */
            // determine the return type of the lower aggregate function
            IMDId *lower_agg_ret_mdid = NULL;
            if (orig_agg_func->FHasAmbiguousReturnType())
            {
                // Agg has an ambiguous return type, use the resolved type instead
                lower_agg_ret_mdid = orig_agg_func->MdidType();
            }
            else
            {
                lower_agg_ret_mdid = md_accessor->RetrieveAgg(orig_agg_mdid)->GetResultTypeMdid();
            }

            const IMDType *lower_agg_ret_type = md_accessor->RetrieveType(lower_agg_ret_mdid);
            // create a column reference for the new created aggregate function
            CColRef *lower_cr = col_factory->PcrCreate(lower_agg_ret_type, default_type_modifier);
            // create new project element for the aggregate function
            CExpression *lower_proj_elem_expr = CUtils::PexprScalarProjectElement
                                                   (
                                                    mp,
                                                    lower_cr,
                                                    lower_agg_expr
                                                    );
            lower_proj_elem_array->Append(lower_proj_elem_expr);

            // create a new operator
            orig_agg_mdid->AddRef();
            CScalarAggFunc *upper_agg_func = CUtils::PopAggFunc
                (
                 mp,
                 orig_agg_mdid,
                 GPOS_NEW(mp) CWStringConst(mp, orig_agg_func->PstrAggFunc()->GetBuffer()),
                 false /* is_distinct */,
                 EaggfuncstageGlobal, /* fGlobal */
                 true /* fSplit */
                 );

            // populate the argument list for the upper aggregate function
            CExpressionArray *upper_agg_arg_array = GPOS_NEW(mp) CExpressionArray(mp);
            upper_agg_arg_array->Append(CUtils::PexprScalarIdent(mp, lower_cr));
            CExpression *upper_agg_expr = GPOS_NEW(mp) CExpression
                (
                 mp,
                 upper_agg_func,
                 upper_agg_arg_array
                 );

            // determine column reference for the new project element
            CExpression *upper_proj_elem_expr = CUtils::PexprScalarProjectElement
                (
                 mp,
                 orig_proj_elem->Pcr(),
                 upper_agg_expr
                 );
            upper_proj_elem_array->Append(upper_proj_elem_expr);
        }
        else
        {
            // If unsupported, add the original project element as-is in the
            // upper project list.
            orig_proj_elem_expr->AddRef();
            upper_proj_elem_array->Append(orig_proj_elem_expr);
        }
	}  // end of loop over each project element

	/* 3. create new project lists */
	*lower_proj_list = GPOS_NEW(mp) CExpression
		(
		 mp,
		 GPOS_NEW(mp) CScalarProjectList(mp),
		 lower_proj_elem_array
		 );

	*upper_proj_list = GPOS_NEW(mp) CExpression
		(
		 mp,
		 GPOS_NEW(mp) CScalarProjectList(mp),
		 upper_proj_elem_array
		 );
}
// EOF
