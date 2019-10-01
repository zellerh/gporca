/*---------------------------------------------------------------------------
 *	Greenplum Database
 *	Copyright (c) 2019 Pivotal Software, Inc.
 *
 *	@filename:
 *		CDebugCounter.h
 *
 *	@doc:
 *		Event counters for debugging purposes
 *
 *---------------------------------------------------------------------------*/

#ifndef GPOS_CDebugCounter_h
#define GPOS_CDebugCounter_h

/*--------------------------------------------------------------------------
                              How to use this class
   --------------------------------------------------------------------------

 1. Ensure the GPOS_DEBUG_COUNTERS define is enabled:

    Use a debug build or change the code below to enable GPOS_DEBUG_COUNTERS
    unconditionally, so you can measure a retail build

 2. Make up name(s) of the event or events you want to count

 3. Add one or more of the following lines to your code:

      #include "gpos/common/CDebugCounter.h"

      // to simply count how many times we reached this point:
      GPOS_DEBUG_COUNTER_BUMP("my counter name 1");

      // to sum up things to be counted for each query:
      GPOS_DEBUG_COUNTER_ADD("my counter name 2", num_widgets_allocated);

      // to sum up a floating point number
      GPOS_DEBUG_COUNTER_ADD_DOUBLE("my counter name 3", scan_cost);

      // to count the total time spent to do something
      GPOS_DEBUG_COUNTER_START_TIME("my counter name 4");
      // do something
      GPOS_DEBUG_COUNTER_STOP_TIME("my counter name 4");

    Note that you can do this multiple times per statement. ORCA will
    log a summary of the counter values after each statement.

 4. Run some statements

    To make it easier to read the resulting log with multiple queries,
    you can name a query by adding a constant select with a single string
    starting with exactly the text 'query name: ':

      select 'query name: my so very challenging query 7';
      -- now the actual query
      explain select ...;

 5. Extract the log information into a CSV format, using the
    script xyz

     TBD

 6. Clean up the code before submitting a PR
    - completely remove all of your modifications
    - in rare cases, if you really do want to keep some lines with
      GPOS_DEBUG_COUNTER_... macros in the code, surround them
      with #ifdefs that are controlled from within this header file

      // leave a commented-out line to enable your feature
      // uncomment this to enable counting of xforms
      // #define GPOS_DEBUG_COUNTER_XFORMS

      // in the instrumented code
      #ifdef GPOS_DEBUG_COUNTER_XFORMS
      GPOS_DEBUG_COUNTER_BUMP(xform_name);
      #endif

  ---------------------------------------------------------------------------*/


#include "gpos/base.h"

#ifdef GPOS_DEBUG
// enable this feature
#define GPOS_DEBUG_COUNTERS
#endif

// define macros
#ifdef GPOS_DEBUG_COUNTERS

#define GPOS_DEBUG_COUNTER_BUMP(q)             gpos::CDebugCounter::Bump(q)
#define GPOS_DEBUG_COUNTER_ADD(q, d)           gpos::CDebugCounter::Add(q, d)
#define GPOS_DEBUG_COUNTER_ADD_DOUBLE(q, d)    gpos::CDebugCounter::AddDouble(q, d)
#define GPOS_DEBUG_COUNTER_START_TIME(q)       gpos::CDebugCounter::StartTime(q)
#define GPOS_DEBUG_COUNTER_END_TIME(q)         gpos::CDebugCounter::EndTime(q)
#else
// empty definitions otherwise
#define GPOS_DEBUG_COUNTER_BUMP(q)
#define GPOS_DEBUG_COUNTER_ADD(q, d)
#define GPOS_DEBUG_COUNTER_ADD_DOUBLE(q, d)
#define GPOS_DEBUG_COUNTER_START(q)
#define GPOS_DEBUG_COUNTER_END(q)
#endif

namespace gpos
{

#ifdef GPOS_DEBUG_COUNTERS
	class CDebugCounter
	{

	public:

		// methods used by ORCA to provide the infrastructure for event counting
		CDebugCounter(CMemoryPool *mp);
		// called once in the lifetime of the process from gpos_init
		static void Init();

		// Prepare for running the next query, providing an optional name for the next
		// query. Note that if a name is provided, we assume the current query is a
		// no-op, just used to provide the query name
		static void NextQry(const char *next_qry_name);

		// methods used by developers who want to count events

		// record an event to be counted
		static void Bump(const char *counter_name);
/*
		static void Add(const char *counter_name, long delta);
		static void AddDouble(const char *counter_name, double delta);

		// start recording time for a time-based event
		static void StartTime(const char *counter_name);
		static void EndTime(const char *counter_name);
 */

	private:

		// put this on the stack to temporarily disable debug counters while
		// in one of our methods
		class AutoDisable
		{
		public:
			AutoDisable() { m_instance->m_suppress_counting = true; }
			~AutoDisable() { m_instance->m_suppress_counting = false; }
		};

		// prevent crashes and infinite recursion when trying to call these methods at the wrong time
		static bool OkToProceed() { return (NULL != m_instance && !m_instance->m_suppress_counting); }

		CMemoryPool *m_mp;
		
		// the single instance of this object, allocated by Init()
		static CDebugCounter *m_instance;

		// turn off debug counters triggered by internal calls
		bool m_suppress_counting;

		// statement number, increased every time a SQL statement gets optimized by ORCA
		ULONG m_qry_number;

		// optional query name, assigned by explaining or running a query of a special form,
		// see above
		std::string m_qry_name;

	};

#endif /* GPOS_DEBUG_COUNTERS */
} // namespace
#endif /* GPOS_CDebugCounter_h */

