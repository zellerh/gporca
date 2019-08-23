//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.h
//
//	@doc:
//		Memory pool that allocates from an underlying allocator and adds on
//		statistics and debugging
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTracker_H
#define GPOS_CMemoryPoolTracker_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/dlmalloc.h"

namespace gpos
{
	// memory pool with statistics and debugging support
	class CMemoryPoolTracker : public CMemoryPool
	{
		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		AllocHeader
			//
			//	@doc:
			//		Defines memory block header layout for all allocations;
			//	 	does not include the pointer to the pool;
			//
			//---------------------------------------------------------------------------
			struct SAllocHeader
			{
				// sequence number
				ULLONG m_serial;

				// user-visible size
				ULONG m_size;

				// file name
				const CHAR *m_filename;

				// line in file
				ULONG m_line;

#ifdef GPOS_DEBUG
				// allocation stack
				CStackDescriptor m_stack_desc;
#endif // GPOS_DEBUG

				// link for allocation list
				SLink m_link;
			};

			// statistics
			CMemoryPoolStatistics m_memory_pool_statistics;

			// allocation sequence number
			ULONG m_alloc_sequence;

			// memory pool capacity;
			// if equal to ULLONG, checks for exceeding max memory are bypassed
			const ULLONG m_capacity;

			// size of reserved memory;
			// this includes total allocated memory and pending allocations;
			ULLONG m_reserved;

			// list of allocated (live) objects
			CList<SAllocHeader> m_allocations_list;

			// Members for aggregating memory pools

			// should we aggregate allocations to reduce memory/bookkeeping overhead?
			BOOL m_aggregate;

			struct malloc_state m_malloc_state;

			void* dlmalloc(size_t bytes);
			void* dlmalloc_array(size_t bytes, int32_t num_elems);
			int init_mstate();
			void dlmalloc_delete_segments(bool check_free);
			void* sys_alloc(malloc_state *m, size_t nb);
			size_t dlmalloc_footprint();
			size_t dlmalloc_max_footprint();

			// attempt to reserve memory for allocation
			BOOL Reserve(ULONG ulAlloc);

			// revert memory reservation
			void Unreserve(ULONG alloc, BOOL mem_available);

			// private copy ctor
			CMemoryPoolTracker(CMemoryPoolTracker &);

		protected:

			// dtor
			virtual
			~CMemoryPoolTracker();

		public:

			// ctor
			CMemoryPoolTracker
				(
				CMemoryPool *underlying_memory_pool,
				ULLONG size,
				BOOL thread_safe,
				BOOL owns_underlying_memory_pool
				);

			// allocate memory
			virtual
			void *Allocate
				(
				const ULONG bytes
#ifdef GPOS_DEBUG
				,
				const CHAR *file,
				const ULONG line
#endif
				);

			// free memory
			virtual
			void Free(void *ptr);

			// prepare the memory pool to be deleted
			virtual
			void TearDown();

			virtual
			void *AggregatedNew
				(
				 SIZE_T size
#ifdef GPOS_DEBUG
				 ,
				 const CHAR * filename,
				 ULONG line
#endif
				 );

			virtual
			void *AggregatedArrayNew
				(
				 SIZE_T size,
#ifdef GPOS_DEBUG
				 const CHAR * filename,
				 ULONG line,
#endif
				 ULONG num_elements
				);

			virtual
			void ReleaseUnusedAggregatedMemory();

			static void dlfree(void* mem);

			static int dlmalloc_num_array_elements(const void *mem);

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			virtual
			BOOL StoresPoolPointer() const
			{
				return true;
			}

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const
			{
				return m_memory_pool_statistics.TotalAllocatedSize();
			}

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL SupportsLiveObjectWalk() const
			{
				return true;
			}

			// walk the live objects
			virtual
			void WalkLiveObjects(gpos::IMemoryVisitor *visitor);

			// check if statistics tracking is supported
			virtual
			BOOL SupportsStatistics() const
			{
				return true;
			}

			// return the current statistics
			virtual
			void UpdateStatistics(CMemoryPoolStatistics &memory_pool_statistics);

#endif // GPOS_DEBUG

	};
}

#endif // !GPOS_CMemoryPoolTracker_H

// EOF

