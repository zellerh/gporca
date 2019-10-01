/*---------------------------------------------------------------------------
 *	Greenplum Database
 *	Copyright (c) 2019 Pivotal Software, Inc.
 *
 *	@filename:
 *		CDebugCounter.cpp
 *
 *	@doc:
 *		Event counters for debugging purposes
 *
 *---------------------------------------------------------------------------*/

#include "gpos/common/CDebugCounter.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CAutoMemoryPool.h"

using namespace gpos;

// initialize static variables

CDebugCounter * CDebugCounter::m_instance = NULL;

CDebugCounter::CDebugCounter(CMemoryPool *mp) :
	m_mp(mp),
	m_suppress_counting(false),
	m_qry_number(0),
	m_qry_name("")

{
}

void CDebugCounter::Init()
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	GPOS_ASSERT(NULL == m_instance);
	m_instance = GPOS_NEW(mp) CDebugCounter(mp);

	// detach safety
	(void) amp.Detach();
}

void CDebugCounter::NextQry(const char *next_qry_name)
{
	m_instance->m_qry_number++;
	if (NULL != next_qry_name)
	{
		m_instance->m_qry_name = next_qry_name;
	}
	else
	{
		m_instance->m_qry_name = "";
	}
}

void CDebugCounter::Bump(const char *counter_name)
{
	if (OkToProceed())
	{
		AutoDisable preventIninitRecursion;
		CAutoTrace at(m_instance->m_mp);

		at.Os() << "CDebugCounterEvent(qryid, qryname, counter, val),"
				<< m_instance->m_qry_number << ","
				<< "\"" << m_instance->m_qry_name.c_str() << "\"" << ","
				<< "\"" << counter_name << "\"" << ","
				<< 1;
	}
}

/*
void CDebugCounter::Add(const char *counter_name, long delta)
{

}

void CDebugCounter::AddDouble(const char *counter_name, double delta)
{

}

void CDebugCounter::StartTime(const char *counter_name)
{

}

void CDebugCounter::EndTime(const char *counter_name)
{

}
*/
