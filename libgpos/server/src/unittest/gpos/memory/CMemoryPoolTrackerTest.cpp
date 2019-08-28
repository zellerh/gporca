//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2019 Pivotal Software, Inc.
//
//
//	@filename:
//		CMemoryPoolTrackerTest.cpp
//
//	@doc:
//		Test for CMemoryPoolTracker (using dlmalloc)
//
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/assert.h"
#include "gpos/memory/CMemoryPoolAlloc.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/test/CUnittest.h"
#include "unittest/gpos/memory/CMemoryPoolTrackerTest.h"

#include <stdlib.h>

using namespace gpos;

int AllocTester::starting_ix = 0;


CMemoryPoolTrackerTest::CMemoryPoolTrackerTest() :
	alloc_entries(NULL),
	m_high_water_mark(0),
	m_next_entry(-1),
	m_curr_alloc_sequence_num(0),
	m_num_used_entries(0),
	m_increasing_size(0)
{
	// initialize all the alloc_entries
	alloc_entries = new alloc_entry[NUM_ALLOC_ENTRIES];

	for (int i=0; i<NUM_ALLOC_ENTRIES; i++)
	{
		alloc_entries[i].ptr = NULL;
	}


}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTrackerTest::EresUnittest
//
//	@doc:
//		unit test driver
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolTrackerTest::EresUnittest() {
	CUnittest rgut[] =
	{
		GPOS_UNITTEST_FUNC(CMemoryPoolTrackerTest::EresUnittest_Stress)
	};

	// execute unit tests
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTrackerTest::EresUnittest_AllocFree
//
//	@doc:
//	  Unit test to test a simple allocation and free through custom allocator
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolTrackerTest::EresUnittest_Stress() {
	CMemoryPoolAlloc allocPool(clib::Malloc, clib::Free);
	CMemoryPoolTracker cmt(&allocPool, gpos::ullong_max, false, false);

	// an instance of object CMemoryPoolTrackerTest
	CMemoryPoolTrackerTest test_obj;

	// describe the types, weights and functions of the tests, the weights should add up to 1.0
	// (cumulative weight is calculated later, no need to set it here)
	struct TestTypeEntry tests[] =
	{
		{ TestTypeAllocRandom,     0.500, -1.0, true  },
		{ TestTypeAllocIncreasing, 0.195, -1.0, true  },
		{ TestTypeAllocLarge,      0.005, -1.0, true  },
		{ TestTypeDealloc,         0.300, -1.0, false }
	};

	// A seed for our pseudo-random generator
	srand(12345);

	// some sanity checks of the setup
	GPOS_RTL_ASSERT(sizeof(tests) / sizeof(TestTypeEntry) == NUM_TEST_TYPES);
	double total_weights = 0.0;
	double delete_tests_fraction = 0.0;
	int num_alloc_tests = 0;

	for (int t=0; t<NUM_TEST_TYPES; t++)
	{
		GPOS_RTL_ASSERT((TestType) tests[t].type == (TestType) t);
		total_weights += tests[t].weight;
		// compute the cumulative weight of this and all previous tests
		tests[t].cumulative_weight = total_weights;
		if (tests[t].is_allocation)
		{
			num_alloc_tests++;
		}
		else
		{
			delete_tests_fraction += tests[t].weight;
		}
	}

	// the weights should sum up to 100%
	GPOS_RTL_ASSERT(total_weights >= 1.0 && total_weights <= 1.001);

	// at least one of the tests has to do an allocation
	GPOS_RTL_ASSERT(num_alloc_tests > 0);

	// If the number of allocation arrays isn't greater than the expected number of
	// allocations minus the expected deallocations, we are likely to run out of space.
	// Note that we might run out of space even if this assert passes.
	GPOS_ASSERT(NUM_TOTAL_ITERATIONS * (1.0-delete_tests_fraction) <= NUM_ALLOC_ENTRIES);

	// -------------------------------------------------------------------------
	// main loop, doing an allocation or deallocation for each iteration, each
	// allocation is recorded in the "alloc_entries" array
	// -------------------------------------------------------------------------
	for (int it_num = 0; it_num < NUM_TOTAL_ITERATIONS; it_num++)
	{
		// pick a test type at random
		TestType testType = test_obj.pickARandomTestType(tests);

		int entry_ix;

		if (tests[testType].is_allocation)
		{
			// allocation test, find a free empty to use
			entry_ix = test_obj.findEmptyEntry();
		}
		else
		{
			// deallocation test, find an entry to delete
			entry_ix = test_obj.findEntryToDelete();
			if (entry_ix < 0)
			{
				// we have nothing allocated, can't delete anything right now
				continue;
			}
		}

		// now call the actual test function that will do the allocation or deallocation
		// of memory in CMemoryPoolTracker
		bool result = test_obj.callTestFunction(&cmt, (TestType) testType, entry_ix);
		GPOS_RTL_ASSERT(result);

#ifdef GPOS_DEBUG
		if (it_num % 2000 == 0)
		{
			cmt.checkConsistency();
		}
#endif
	}

#ifdef GPOS_DEBUG
	cmt.checkConsistency();
#endif
	cmt.TearDown();

	return GPOS_OK;
}

CMemoryPoolTrackerTest::TestType CMemoryPoolTrackerTest::pickARandomTestType(struct TestTypeEntry *tests)
{
	int rn = rand();
	// a random number between 0 and 1
	double norm_rn = (double) rn / RAND_MAX;

	for (int t=0; t < NUM_TEST_TYPES; t++)
	{
		// the cumulative weight is used here as a cumulative distribution function
		if (tests[t].cumulative_weight >= norm_rn)
		{
			return (TestType) t;
		}
	}

	// should get here only if we have rounding errors
	return (TestType) ((int) NUM_TEST_TYPES - 1);
}

int CMemoryPoolTrackerTest::findEmptyEntry()
{
	// make sure there is at least one free entry
	GPOS_RTL_ASSERT(m_num_used_entries < NUM_ALLOC_ENTRIES);

	// bookkeeping
	m_num_used_entries++;

	if (m_high_water_mark < NUM_ALLOC_ENTRIES)
	{
		// we haven't exhausted our array, just use the next unused entry
		// and bump the high water mark
		return m_high_water_mark++;
	}

	// we have used all of our alloc_entries at least once, so walk the array,
	// starting at m_next_entry and search for a free entry
	return findNextFreeOrDeletedEntry(m_next_entry, true);
}

int CMemoryPoolTrackerTest::findEntryToDelete()
{
	if (m_num_used_entries == 0)
	{
		// there are no entries to delete
		return -1;
	}

	// pick a random entry from the used entries
	int search_start = rand() % m_high_water_mark;

	// bookkeeping
	m_num_used_entries--;

	return findNextFreeOrDeletedEntry(search_start, false);
}

int CMemoryPoolTrackerTest::findNextFreeOrDeletedEntry(int &start_ix, bool find_a_free_entry)
{
	// we have already verified that there is at least one entry of the
	// kind we are looking for

	// use a do-while loop to make sure we bump m_next_entry at least
	// once every time we call this
	do
	{
		// go to the next entry
		// (the very first time we do this we'll go to entry 0)
		start_ix = (start_ix + 1) % m_high_water_mark;
	}
	while (find_a_free_entry == (alloc_entries[start_ix].ptr != NULL));

	return start_ix;
}

bool CMemoryPoolTrackerTest::nonArrayAlloc(CMemoryPoolTracker *mp, int entry_ix, int alloc_size)
{
	alloc_entry &entry = alloc_entries[entry_ix];

	GPOS_RTL_ASSERT(entry.ptr == NULL);

	entry.ptr = mp->AggregatedNew(alloc_size
#ifdef GPOS_DEBUG
								  ,
								  __FILE__,
								  __LINE__
#endif
								  );
	entry.num_bytes_requested = alloc_size;
	entry.num_array_elements = -1;
	entry.size_of_one_element = -1;
	entry.uses_alloc_tester = false;
	entry.alloc_sequence_num = next_alloc_sequence_num();
	m_total_requested_bytes += entry.num_bytes_requested;

	return (entry.ptr != NULL);
}

bool CMemoryPoolTrackerTest::arrayAlloc(CMemoryPoolTracker *mp, int entry_ix, int num_elements)
{
	alloc_entry &entry = alloc_entries[entry_ix];
	AllocTester::starting_ix = 0;

	GPOS_RTL_ASSERT(entry.ptr == NULL);

	entry.ptr = GPOS_NEW_ARRAY(mp, AllocTester, num_elements);
	entry.num_bytes_requested = num_elements * sizeof(AllocTester);
	entry.num_array_elements = num_elements;
	entry.size_of_one_element = sizeof(AllocTester);
	entry.uses_alloc_tester = true;
	entry.alloc_sequence_num = next_alloc_sequence_num();
	m_total_requested_bytes += entry.num_bytes_requested;

	return (entry.ptr != NULL);
}

bool CMemoryPoolTrackerTest::dealloc(CMemoryPoolTracker *, int entry_ix)
{
	alloc_entry &entry = alloc_entries[entry_ix];

	GPOS_RTL_ASSERT(entry.ptr != NULL);

	if (entry.num_array_elements > 0)
	{
		GPOS_DELETE_ARRAY((AllocTester *) entry.ptr);
	}
	else
	{
		GPOS_DELETE((char *) entry.ptr);
	}

	entry.ptr = NULL;
	m_total_requested_bytes -= entry.num_bytes_requested;

	return true;
}

bool CMemoryPoolTrackerTest::allocateARandomChunk(CMemoryPoolTracker *mp, int entry_ix)
{
	size_t alloc_size = rand() % MAX_RAND_ALLOC_SIZE;
	bool use_array_alloc = (((double) rand()) / RAND_MAX) < ((double) ARRAY_PERCENT) / 100.0;

	if (use_array_alloc)
	{
		int num_array_elems = rand() % MAX_ARRAY_ELEMENTS + 1;

		return arrayAlloc(mp, entry_ix, num_array_elems);
	}
	else
	{
		return nonArrayAlloc(mp, entry_ix, alloc_size);
	}
}

bool CMemoryPoolTrackerTest::allocateAnIncreasingChunk(CMemoryPoolTracker *mp, int entry_ix)
{
	m_increasing_size += INCREASING_ALLOC_INCREMENT;

	return nonArrayAlloc(mp, entry_ix, m_increasing_size);
}

bool CMemoryPoolTrackerTest::allocateALargeChunk(CMemoryPoolTracker *mp, int entry_ix)
{
	int alloc_size = MIN_LARGE_ALLOC_SIZE * (1 << (rand() % MAX_LARGE_ALLOC_SIZE_POWER_OF_2_MULT));

	return nonArrayAlloc(mp, entry_ix, alloc_size);
}

bool CMemoryPoolTrackerTest::deallocateARandomChunk(CMemoryPoolTracker *mp, int entry_ix)
{
	return dealloc(mp, entry_ix);
}



// EOF
