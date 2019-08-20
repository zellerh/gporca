//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPool.cpp
//
//	@doc:
//		Implementation of abstract interface; 
//		implements helper functions for extraction of allocation
//		header from memory block;
//---------------------------------------------------------------------------

#ifdef GPOS_DEBUG
#include "gpos/error/CFSimulator.h"
#endif // GPOS_DEBUG
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/task/ITask.h"


using namespace gpos;

// invalid exception
const ULONG_PTR CMemoryPool::m_invalid = ULONG_PTR_MAX;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::CMemoryPool
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemoryPool::CMemoryPool
	(
	CMemoryPool *underlying_memory_pool,
	BOOL owns_underlying_memory_pool,
	BOOL thread_safe
	)
	:
	m_ref_counter(0),
	m_hash_key(0),
	m_underlying_memory_pool(underlying_memory_pool),
	m_owns_underlying_memory_pool(owns_underlying_memory_pool),
	m_thread_safe(thread_safe)
{
	GPOS_ASSERT_IMP(owns_underlying_memory_pool, NULL != underlying_memory_pool);

	m_hash_key = reinterpret_cast<ULONG_PTR>(this);
#ifdef GPOS_DEBUG
	m_stack_desc.BackTrace();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::~CMemoryPool
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemoryPool::~CMemoryPool()
{
	if (m_owns_underlying_memory_pool)
	{
		CMemoryPoolManager::GetMemoryPoolMgr()->DeleteUnregistered(m_underlying_memory_pool);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::FinalizeAlloc
//
//	@doc:
//		Set allocation header and footer, return pointer to user data
//
//---------------------------------------------------------------------------
void *
CMemoryPool::FinalizeAlloc
	(
	void *ptr,
	ULONG alloc,
	EAllocationType eat
	)
{
	GPOS_ASSERT(NULL != ptr);

	AllocHeader *header = static_cast<AllocHeader*>(ptr);
	header->m_mp = this;
	header->m_alloc = alloc;
	header->m_zero_marker = 0;

	BYTE *alloc_type = reinterpret_cast<BYTE*>(header + 1) + alloc;
	*alloc_type = eat;

	return header + 1;
}


ULONG
CMemoryPool::SizeOfAlloc
	(
	const void *ptr
	)
{
	GPOS_ASSERT(NULL != ptr);

	const AllocHeader *header = static_cast<const AllocHeader*>(ptr) - 1;
	return header->m_alloc;
}


// return allocation to owning memory pool
void
CMemoryPool::FreeAlloc
(
 void *ptr,
 EAllocationType eat
 )
{
	GPOS_ASSERT(ptr != NULL);

	AllocHeader *header = static_cast<AllocHeader*>(ptr) - 1;
	if (0 != header->m_zero_marker)
	{
		// if it's non-zero, we have an aggregated allocation
		CMemoryPoolTracker::dlfree(ptr);
	}
	else
	{
		BYTE *alloc_type = static_cast<BYTE*>(ptr) + header->m_alloc;
		GPOS_RTL_ASSERT(*alloc_type == eat);
		header->m_mp->Free(header);

	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::NewImpl
//
//	@doc:
//		Implementation of New that can be used by "operator new" functions
////
//---------------------------------------------------------------------------
void*
CMemoryPool::NewImpl
	(
	SIZE_T size,
#ifdef GPOS_DEBUG
	const CHAR *filename,
	ULONG line,
#endif
	CMemoryPool::EAllocationType eat
	)
{
	GPOS_ASSERT(gpos::ulong_max >= size);
	GPOS_ASSERT_IMP
	(
	(NULL != CMemoryPoolManager::GetMemoryPoolMgr()) && (this == CMemoryPoolManager::GetMemoryPoolMgr()->GetGlobalMemoryPool()),
	 CMemoryPoolManager::GetMemoryPoolMgr()->IsGlobalNewAllowed() &&
	 "Use of new operator without target memory pool is prohibited, use New(...) instead"
	);

	ULONG alloc_size = CMemoryPool::GetAllocSize((ULONG) size);
	void *ptr = Allocate(alloc_size
#ifdef GPOS_DEBUG
						 , filename, line
#endif
						);

	GPOS_OOM_CHECK(ptr);
	
	return this->FinalizeAlloc(ptr, (ULONG) size, eat);
}

void*
CMemoryPool::AggregatedNew
	(
	 SIZE_T size,
#ifdef GPOS_DEBUG
	 const CHAR * filename,
	 ULONG line,
#endif
	 EAllocationType type
	 )
{
	return NewImpl(size,
#ifdef GPOS_DEBUG
				   filename,
				   line,
#endif
				   type);
}

//---------------------------------------------------------------------------
//	@function:
//		DeleteImpl
//
//	@doc:
//		implementation of Delete that can be used by operator new functions
//
//---------------------------------------------------------------------------
void
CMemoryPool::DeleteImpl
	(
	void *ptr,
	EAllocationType eat
	)
{
	// deletion of NULL pointers is legal
	if (NULL == ptr)
	{
		return;
	}

	// release allocation
	FreeAlloc(ptr, eat);

}  // namespace gpos

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::Print
//
//	@doc:
//		Walk all objects and print identification
//
//---------------------------------------------------------------------------
IOstream &
CMemoryPool::OsPrint
	(
	IOstream &os
	)
{
	os << "Memory pool: " << this;

	ITask *task = ITask::Self();
	if (NULL != task && task->IsTraceSet(EtracePrintMemoryLeakStackTrace))
	{
		os << ", stack trace: " << std::endl;
		m_stack_desc.AppendTrace(os, 8 /*ulDepth*/);
	}
	else
	{
		os << std::endl;
	}

	if (SupportsLiveObjectWalk())
	{
		CMemoryVisitorPrint visitor(os);
		WalkLiveObjects(&visitor);
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::AssertEmpty
//
//	@doc:
//		Static helper function to print out if the given pool is empty;
//		if the pool is not empty and we are compiled with debugging on then
//		an assertion will be thrown;
//
//---------------------------------------------------------------------------
void
CMemoryPool::AssertEmpty
	(
	IOstream &os
	)
{
	if (SupportsLiveObjectWalk() && NULL != ITask::Self() &&
	    !GPOS_FTRACE(EtraceDisablePrintMemoryLeak))
	{
		CMemoryVisitorPrint visitor(os);
		WalkLiveObjects(&visitor);

		if (0 != visitor.GetNumVisits())
		{
			os
				<< "Unfreed memory in memory pool "
				<< (void*)this
				<< ": "
				<< visitor.GetNumVisits()
				<< " objects leaked"
				<< std::endl;

			GPOS_ASSERT(!"leak detected");
		}
	}
}

#endif // GPOS_DEBUG


// EOF

