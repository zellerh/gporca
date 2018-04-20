//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayCoerceExpr.cpp
//
//	@doc:
//		Implementation of DXL scalar array coerce expr
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayCoerceExpr.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayCoerceExpr::CDXLScalarArrayCoerceExpr
	(
	IMemoryPool *pmp,
	IMDId *pmdidElementFunc,
	IMDId *pmdidResultType,
	INT iTypeModifier,
	BOOL fIsExplicit,
	EdxlCoercionForm edxlcf,
	INT iLoc,
	OID oidResultCollation,
	OID oidInputCollation
	)
	:
	CDXLScalarCoerceBase(pmp, pmdidResultType, iTypeModifier, edxlcf, iLoc, oidResultCollation),
	m_pmdidElementFunc(pmdidElementFunc),
	m_fIsExplicit(fIsExplicit),
	m_oidInputCollation(oidInputCollation)
{
	GPOS_ASSERT(NULL != pmdidElementFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayCoerceExpr::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArrayCoerceExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayCoerceExpr::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayCoerceExpr::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdidElementFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenElementFunc));
	PmdidResultType()->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsExplicit), m_fIsExplicit);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCoercionForm), (ULONG) Edxlcf());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenLocation), ILoc());
	if (OidInvalidCollation != OidResultCollation())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCollation), OidResultCollation());
	}
	if (OidInvalidCollation != OidInputCollation())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInputCollation), OidInputCollation());
	}

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
