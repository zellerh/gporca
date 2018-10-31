//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2018 Pivotal Inc.
//
//  @filename:
//      CXformEagerAgg.cpp
//
//  @doc:
//      Implementation for eagerly pushing aggregates below join
//          (with no foreign key restriction on the join condition)
//          The aggregate is pushed down only on the outer child
//          (since the inner child alternative will be explored through
//           commutativity)
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
    CExpression *join_expr = (*pexpr)[0];
    CExpression *join_outer_child_expr = (*join_expr)[0];
    CExpression *join_inner_child_expr = (*join_expr)[1];
    CExpression *join_condition_expr = (*join_expr)[2];
    CExpression *agg_proj_list_expr = (*pexpr)[1];

    //  'push_down_gb_crs' represents the column references that are used for
    //  grouping in the pushed-down aggregate. This is the union of the original
    //  grouping columns and join predicate columns, retaining only the columns
    //  from the outer child.
    CColRefSet *push_down_gb_crs = GPOS_NEW(mp) CColRefSet
                                                (
                                                 mp,
                                                 *(CDrvdPropScalar::GetDrvdScalarProps(
                                                     join_condition_expr->PdpDerive())->PcrsUsed())
                                                );
    CColRefSet *grouping_crs = (CLogicalGbAgg::PopConvert(pexpr->Pop()))->PcrsLocalUsed();
    push_down_gb_crs->Union(grouping_crs);

    /* verify if this aggregate can be applied */
    if (!CanApplyTransform(pexpr))
    {
        push_down_gb_crs->Release();
        return;
    }

	/* only keep columns from outer child in the new grouping col set */
	CColRefSet *outer_child_crs =  CDrvdPropRelational::GetRelationalProperties(
														join_outer_child_expr->PdpDerive())->PcrsOutput();
	push_down_gb_crs->Intersection(outer_child_crs);

    /* create new project lists for the two new Gb aggregates */
    CExpression *lower_expr_proj_list = NULL;
    CExpression *upper_expr_proj_list = NULL;
    (void) PopulateLowerUpperProjectList
            (
             mp,
             agg_proj_list_expr,
             &lower_expr_proj_list,
             &upper_expr_proj_list
             );

    /* create lower agg, join, and upper agg expressions */

    // lower agg expression
	CColRefArray *push_down_gb_cra = push_down_gb_crs->Pdrgpcr(mp);

    join_outer_child_expr->AddRef();
	CExpression *lower_agg_expr = GPOS_NEW(mp) CExpression
												(
												 mp,
												 GPOS_NEW(mp) CLogicalGbAgg
												 (
												  mp,
												  push_down_gb_cra,
												  COperator::EgbaggtypeLocal
												 ),
												 join_outer_child_expr,
												 lower_expr_proj_list
												 );

	// join expression
	COperator *join_operator = join_expr->Pop();
	join_operator->AddRef();
	join_inner_child_expr->AddRef();
	join_condition_expr->AddRef();
	CExpression *new_join_expr = GPOS_NEW(mp) CExpression
											  (
											   mp,
											   join_operator,
											   lower_agg_expr,
											   join_inner_child_expr,
											   join_condition_expr
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

BOOL CXformEagerAgg::CanPushAggBelowJoin
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
		/* currently only supporting single-input aggregates */
        return false;
    }

    CExpression *agg_child_expr = (*scalar_agg_func_expr)[0];
    IMDId *agg_child_mdid = CScalar::PopConvert(agg_child_expr->Pop())->MdidType();
    const IMDType *agg_child_type = md_accessor->RetrieveType(agg_child_mdid);

    if (!(agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggMin))) &&
        	!(agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggMax))) &&
        	!(agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggSum))) &&
        	!(agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggCount))) &&
			!(agg_mdid->Equals(agg_child_type->GetMdidForAggType(IMDType::EaggAvg))))
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
CXformEagerAgg::CanApplyTransform
(
 CExpression *gb_agg_pexpr
)
const
{
	CExpression *join_expr = (*gb_agg_pexpr)[0];
	CExpression *agg_proj_list_expr = (*gb_agg_pexpr)[1];
	CExpression *join_outer_child_expr = (*join_expr)[0];
	
    /* currently only supporting aggregate column references from outer child */
    CColRefSet *join_outer_child_crs = CDrvdPropRelational::GetRelationalProperties(
                                   join_outer_child_expr->PdpDerive())->PcrsOutput();
    CColRefSet *agg_proj_list_crs = CDrvdPropScalar::GetDrvdScalarProps(
                                agg_proj_list_expr->PdpDerive())->PcrsUsed();
    if (!join_outer_child_crs->ContainsAll(agg_proj_list_crs))
    {
		// all columns used by the Gb aggregate should only be present in outer child
        return false;
    }
	
	const ULONG n_aggregates = agg_proj_list_expr->Arity();
	if (n_aggregates == 0)
    {
        // at least one aggregate must be present to push down
        return false;
    }
    for (ULONG agg_index = 0; agg_index < n_aggregates; agg_index++)
    {
		CExpression *scalar_agg_proj_expr = (*agg_proj_list_expr)[agg_index];
        if (!CanPushAggBelowJoin((*scalar_agg_proj_expr)[0]))
        {
			return false;
        }
    }
	return true;
}

// populate the lower and upper aggregate's project list after
// splitting and pushing down an aggregate.
void
CXformEagerAgg::PopulateLowerUpperProjectList
(
 IMemoryPool *mp,                // memory pool
 CExpression *orig_proj_list,    // project list of the original global aggregate
 CExpression **lower_proj_list,  // project list of the new lower aggregate
 CExpression **upper_proj_list    // project list of the new upper aggregate
 ) const
{
	//CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	//CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	// building an array of project elements for the new lower and upper aggregates
	CExpressionArray *lower_proj_elem_array = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *upper_proj_elem_array = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG n_proj_elements = orig_proj_list->Arity();

	// loop over each project element
	for (ULONG ul = 0; ul < n_proj_elements; ul++)
	{
		CExpression *orig_proj_elem_expr = (*orig_proj_list)[ul];
		CScalarProjectElement *orig_proj_elem =
		CScalarProjectElement::PopConvert(orig_proj_elem_expr->Pop());

		
		CExpression *orig_agg_expr = (*orig_proj_elem_expr)[0];
		
        if (CanPushAggBelowJoin(orig_agg_expr))
        {
        	CScalarAggFunc *orig_agg_func = CScalarAggFunc::PopConvert(orig_agg_expr->Pop());
            IMDId *orig_agg_mdid = orig_agg_func->MDId();
			// min and max
			CExpression *lower_proj_elem_expr = NULL;
			PopulateLowerProjectElement
				(
				 mp,
				 orig_agg_mdid,
				 GPOS_NEW(mp) CWStringConst(mp, orig_agg_func->PstrAggFunc()->GetBuffer()),
				 orig_agg_expr->PdrgPexpr(),
				 orig_agg_func->IsDistinct(),
				 &lower_proj_elem_expr
				);
			lower_proj_elem_array->Append(lower_proj_elem_expr);


			CExpression *upper_proj_elem_expr = NULL;
			PopulateUpperProjectElement
				(
				 mp,
				 orig_agg_mdid,
				 GPOS_NEW(mp) CWStringConst(mp, orig_agg_func->PstrAggFunc()->GetBuffer()),
				 CScalarProjectElement::PopConvert(lower_proj_elem_expr->Pop())->Pcr(),
				 orig_proj_elem->Pcr(),
				 orig_agg_func->IsDistinct(),
				 &upper_proj_elem_expr
				 );
			upper_proj_elem_array->Append(upper_proj_elem_expr);
        }
        else
        {
            // if unsupported, add the original project element as-is in the
            // upper project list
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

// populate the lower aggregate's project element after
// splitting and pushing down a single aggregate.
void
CXformEagerAgg::PopulateLowerProjectElement
(
 IMemoryPool *mp,                // memory pool
 IMDId *agg_mdid,    	// original global aggregate function
 CWStringConst *agg_name,
 CExpressionArray *agg_arg_array,
 BOOL is_distinct,
 CExpression **lower_proj_elem_expr  // output project element of the new lower aggregate
) const
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	agg_mdid->AddRef();
	CScalarAggFunc *lower_agg_func  = CUtils::PopAggFunc
										(
										 mp,
										 agg_mdid,
										 agg_name,
										 is_distinct,
										 EaggfuncstageLocal, /* fGlobal */
										 true /* fSplit */
										 );
	// add the arguments for the lower aggregate function, which is
	// going to be the same as the original aggregate function
	agg_arg_array->AddRef();
	CExpression *lower_agg_expr = GPOS_NEW(mp) CExpression
												(
												 mp,
												 lower_agg_func,
												 agg_arg_array
												 );

	/* 2. create new aggregate function for the upper aggregate operator */
		// determine the return type of the lower aggregate function
	IMDId *lower_agg_ret_mdid = NULL;
//	if (orig_agg_func->FHasAmbiguousReturnType())
//	{
//			// Agg has an ambiguous return type, use the resolved type instead
//		lower_agg_ret_mdid = orig_agg_func->MdidType();
//	}
//	else
	{
		lower_agg_ret_mdid = md_accessor->RetrieveAgg(agg_mdid)->GetIntermediateResultTypeMdid();
	}
	
	const IMDType *lower_agg_ret_type = md_accessor->RetrieveType(lower_agg_ret_mdid);
	
	// create a column reference for the new created aggregate function
	CColRef *lower_cr = col_factory->PcrCreate(lower_agg_ret_type, default_type_modifier);
	// create new project element for the aggregate function
	*lower_proj_elem_expr = CUtils::PexprScalarProjectElement
								(
								 mp,
								 lower_cr,
								 lower_agg_expr
								 );
}

// populate the upper aggregate's project element
// corresponding to a single aggregate.
void
CXformEagerAgg::PopulateUpperProjectElement
(
 IMemoryPool *mp,                // memory pool
 IMDId *agg_mdid,    	// original global aggregate function
 CWStringConst *agg_name,
 CColRef *lower_cr,
 CColRef *output_cr,
 BOOL is_distinct,
 CExpression **upper_proj_elem_expr  // output project element of the new lower aggregate
) const
{
		// create a new operator
	agg_mdid->AddRef();
	CScalarAggFunc *upper_agg_func = CUtils::PopAggFunc
		(
		 mp,
		 agg_mdid,
		 agg_name,
		 is_distinct,
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
	*upper_proj_elem_expr = CUtils::PexprScalarProjectElement
								(
								 mp,
								 output_cr,
								 upper_agg_expr
								 );
}
// EOF
