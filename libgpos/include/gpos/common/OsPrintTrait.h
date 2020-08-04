//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2020 VMware, and affiliates Inc.
//
//	OsPrintTrait.h
//
//	C++98 implementation of C++11 language features such as decltype,
//  enable_if, and a template-based method to test for existence of
//  an OsPrint() method.
//---------------------------------------------------------------------------
#ifndef GPOS_OsPrintTrait_H
#define GPOS_OsPrintTrait_H

#include "gpos/base.h"

namespace gpos
{
			// -------------------------------------------------------------------------------
			// A more complex, template-based C++98 implementation to print the array elements
			// if they have an OsPrint method. This can be done more easily in C++14.
			// -------------------------------------------------------------------------------

			// Poor man's void_t<decltype(...)> in C++98.
			// Instantiated as val_type<sizeof(expression)> so that ill-formed expressions
			// lead to SFINAE.
			template <size_t n>
			struct val_type {};

			template <class E, class = val_type<0> >
			struct COPImpl
			{
			  static const bool value = false;
			};

			// this class template partial specialization is SFINAE'd away when either of
			// the following occurs:
			// 1. T doesn't have a non-static member OsPrint
			// 2. said member is not a function that matches the signature we expect
			//
			// N.B. for exposition purposes this requirement is overly strict to achieve
			// simplicity: we _really_ only care about contravariant parameter types and
			// covariant return types. In plain English, the parameter types can be broader,
			// and the return type can be narrower. The C++14 version achieves this.
			template <class E>
			struct COPImpl<E, val_type<0 * sizeof(static_cast<IOstream& (E::*)(IOstream&) const>(&E::OsPrint))> >
			{
			  static const bool value = true;
			};

			// Poor man's std::enable_if (available in C++11 and above)
			template <bool Condition, class E = void>
			struct enable_if {};

			template <class E>
			struct enable_if<true, E>
			{
			  typedef E type;
			};

			template <class E>
			struct CanOsPrint : COPImpl<E>
			{};

			// generic template, used when there is no suitable OsPrint method in class T
			template <class E, class CanOsPrint = void>
			struct OsPrintTrait
			{
			  static void OsPrintElem(IOstream &, E *, BOOL)
				{
					// do nothing, unable to print this element type
				}
			};

			// specialized template, used if there is a OsPrint with the correct signature
			template <class E>
			struct OsPrintTrait<E, typename enable_if<CanOsPrint<E>::value>::type>
			{
				static void OsPrintElem(IOstream& os, E *elem, BOOL printASeparator)
				{
					if (printASeparator)
					{
						os << ", ";
					}
					(void) elem->OsPrint(os);
				}
			};

}

#endif /* GPOS_OsPrintTrait_H */
