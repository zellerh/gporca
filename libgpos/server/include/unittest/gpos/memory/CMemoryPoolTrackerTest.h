//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2019 Pivotal Software, Inc.
//
//	@filename:
//		CMemoryPoolTrackerTest.h
//
//	@doc:
//		Test for CMemoryPoolTracker
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTrackerTest_H
#define GPOS_CMemoryPoolTrackerTest_H

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CMainArgs.h"

#define GPOS_INIT(pma)  struct gpos_init_params init_params = { \
(FUseCustomAllocator(pma) ? CCustomAllocator::fnAlloc : NULL), \
(FUseCustomAllocator(pma) ? CCustomAllocator::fnFree : NULL), NULL}; \
gpos_init(&init_params);

namespace gpos
{
	// forward reference
	class CMemoryPoolTracker;

	// Helper classes, to be allocated on the memory pool
	class AllocTester
	{
	public:
		static int starting_ix;

		int array_ix;
		char *null_ptr;

		AllocTester() : array_ix(starting_ix++), null_ptr(NULL) {};
		~AllocTester() {}

	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolTrackerTest
	//
	//	@doc:
	//		Unittest for CMemoryPoolTracker
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolTrackerTest
	{
		// The stress test loops NUM_TOTAL_ITERATIONS times and decides for each
		// iteration what to do, using a pseudo-random number:
		// - allocate a new chunk of random size
		// - allocate a new chunk of monotonically increasing size
		// - allocate a new large chunk
		// - free a pseudo-random previously allocated chunk

		// the total number of allocations/deallocations
		static const int NUM_TOTAL_ITERATIONS = 300000;

		// the maximum number of allocated chunks we will support, make sure
		// this is large enough to hold all the entries we'll need
		static const int NUM_ALLOC_ENTRIES = 250000;

		// Random allocation size is chosen as a random number between 1 and MAX_RAND_ALLOC_SIZE
		static const int MAX_RAND_ALLOC_SIZE = 5000;

		// increment by which to increase the monotonically increasing allocations
		static const int INCREASING_ALLOC_INCREMENT = 4;

		// Large allocation size is chosen as a minimum size times 2**n
		static const int MIN_LARGE_ALLOC_SIZE = 256 * 1024; // min 256KB
		static const int MAX_LARGE_ALLOC_SIZE_POWER_OF_2_MULT = 4; // max 256K * 2**4 = 4MB

		// percent of allocations to be done as array allocates
		static const int ARRAY_PERCENT = 10;

		// max number of array elements to allocate, number is chosen as a random
		// number 1 <= n <= MAX_ARRAY_ELEMENTS
		static const int MAX_ARRAY_ELEMENTS = 100;

		// an entry to keep track of one memory allocation
		struct alloc_entry
		{
			// pointer to allocated memory (or NULL if entry is unused)
			void *ptr;
			// number of bytes requested for the allocation
			int num_bytes_requested;
			// allocation sequence number (inreases with time of allocation)
			long alloc_sequence_num;
			// number of array elements allocated or -1 if this is not an array
			int num_array_elements;
			// size of one array element, if this is an array allocation
			int size_of_one_element;
			// true, if the allocated memory is of type AllocTester, false otherwise
			bool uses_alloc_tester;
		};

		// types of tests we do
		enum TestType
		{
			TestTypeAllocRandom = 0,
			TestTypeAllocIncreasing = 1,
			TestTypeAllocLarge = 2,
			TestTypeDealloc = 3,
			NUM_TEST_TYPES = 4
		};

		// An array that describes each test
		struct TestTypeEntry
		{
			TestType type;
			double weight;
			double cumulative_weight;
			bool is_allocation;
		};

		// all the allocations we have done in this test
		struct alloc_entry *alloc_entries;

		// index of the highest used entry in entries + 1
		int m_high_water_mark;
		// starting point for a new entry to use in entries, wrap around if needed
		int m_next_entry;
		// number of allocations done so far (not counting deallocations)
		long m_curr_alloc_sequence_num;
		// number of used entries in the entries array
		int m_num_used_entries;
		// total number of bytes requested
		long m_total_requested_bytes;
		// size of last allocation for increasing size
		int m_increasing_size;

		inline long next_alloc_sequence_num() { return ++m_curr_alloc_sequence_num; }

		// private constructor
		CMemoryPoolTrackerTest();

		// helper methods to find a random entry to use
		TestType pickARandomTestType(struct TestTypeEntry *tests);
		int findEmptyEntry();
		int findEntryToDelete();
		int findNextFreeOrDeletedEntry(int &start_ix, bool find_a_free_entry);

		// lowest level in this file: Methods to do the actual allocation
		bool nonArrayAlloc(CMemoryPoolTracker *mp, int entry_ix, int alloc_size);
		bool arrayAlloc(CMemoryPoolTracker *mp, int entry_ix, int num_elements);
		bool dealloc(CMemoryPoolTracker *mp, int entry_ix);

		// medium level in this file: Functions, executed for the various test types
		bool allocateARandomChunk(CMemoryPoolTracker *mp, int entry_ix);
		bool allocateAnIncreasingChunk(CMemoryPoolTracker *mp, int entry_ix);
		bool allocateALargeChunk(CMemoryPoolTracker *mp, int entry_ix);
		bool deallocateARandomChunk(CMemoryPoolTracker *mp, int entry_ix);

		inline bool callTestFunction(CMemoryPoolTracker *mp, TestType t, int entry_ix)
		{
			switch (t)
			{
				case TestTypeAllocRandom:
					return allocateARandomChunk(mp, entry_ix);
				case TestTypeAllocIncreasing:
					return allocateAnIncreasingChunk(mp, entry_ix);
				case TestTypeAllocLarge:
					return allocateALargeChunk(mp, entry_ix);
				case TestTypeDealloc:
					return deallocateARandomChunk(mp, entry_ix);
				default:
					return false;
			}
		}

	public:

		// top-level functions in this file, test drivers
		static GPOS_RESULT EresUnittest();
		static GPOS_RESULT EresUnittest_Stress();

	};
}

#endif // !GPOS_CMemoryPoolTrackerTest_H

// EOF
