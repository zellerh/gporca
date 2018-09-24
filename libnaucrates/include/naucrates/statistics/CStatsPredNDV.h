//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CStatsPredNDV.h
//
//	@doc:
//		Statistics filter for NDV based equality predicate
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredNDV_H
#define GPNAUCRATES_CStatsPredNDV_H

#include "gpos/base.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatsPred.h"

// fwd declarations
namespace gpopt
{
	class CColRef;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatsPredNDV
	//
	//	@doc:
	//		Statistics filter for NDV based equality predicate
	//---------------------------------------------------------------------------
	class CStatsPredNDV : public CStatsPred
	{
		private:

			// private copy ctor
			CStatsPredNDV(const CStatsPredNDV &);

			// private assignment operator
			CStatsPredNDV& operator=(CStatsPredNDV &);

			// cap on the number of distinct values
			ULONG m_num_distinct_values;

		public:

			// ctor
			CStatsPredNDV
				(
				const CColRef *colref,
				ULONG num_distinct_values
				);

			virtual
			ULONG NumDistinctValue() const
			{
				return m_num_distinct_values;
			}

			// filter type id
			virtual
			EStatsPredType GetPredStatsType() const
			{
				return CStatsPred::EsptNDV;
			}

			// conversion function
			static
			CStatsPredNDV *ConvertPredStats
				(
				CStatsPred *pred_stats
				)
			{
				GPOS_ASSERT(NULL != pred_stats);
				GPOS_ASSERT(CStatsPred::EsptNDV == pred_stats->GetPredStatsType());

				return dynamic_cast<CStatsPredNDV*>(pred_stats);
			}

	}; // class CStatsPredNDV

}

#endif // !GPNAUCRATES_CStatsPredNDV_H

// EOF
