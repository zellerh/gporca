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

ULONG CDebugCounter::SDebugCounterKey::HashValue(const CDebugCounter::SDebugCounterKey *key)
{
	ULONG result = 0;
	int key_size = key->m_counter_name.size();

	for (int i=0; i<key_size; i++)
	{
		result += result * 257 + key->m_counter_name.at(i);
	}

	return result;
}

CDebugCounter::CDebugCounter(CMemoryPool *mp) :
	m_mp(mp),
	m_suppress_counting(false),
	m_qry_number(0),
	m_qry_name(""),
	m_is_name_constant_get(false),
	m_hashmap(NULL)

{
	m_hashmap = GPOS_NEW(mp) CounterKeyToValueMap(mp);
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
	if (NULL != next_qry_name && '\0' != *next_qry_name)
	{
		m_instance->m_qry_name = next_qry_name;
		m_instance->m_is_name_constant_get = true;
	}
	else
	{
		if (m_instance->m_is_name_constant_get)
		{
			// use the previously specified name and
			// suppress logging for this simple constant get
			m_instance->m_is_name_constant_get = false;
		}
		else
		{
			// an unnamed query
			m_instance->m_qry_name = "";
		}
	}

	if (0 < m_instance->m_hashmap->Size())
	{
		if (!m_instance->m_is_name_constant_get)
		{
			// log all counter values
			CAutoTrace at(m_instance->m_mp);

			CounterKeyToValueMapIterator iter(m_instance->m_hashmap);

			while (iter.Advance())
			{
				at.Os() << "CDebugCounterEvent(qryid, qryname, counter, val),"
				<< m_instance->m_qry_number << ","
				<< "\"" << m_instance->m_qry_name.c_str() << "\"" << ","
				<< "\"" << iter.Key()->m_counter_name.c_str() << "\"" << ","
				<< iter.Value()->m_counter;
			}
		}

		// clear the hash map by allocating a new,
		// empty one in its stead
		m_instance->m_hashmap->Release();
		m_instance->m_hashmap = GPOS_NEW(m_instance->m_mp) CounterKeyToValueMap(m_instance->m_mp);
	}
}

BOOL CDebugCounter::FindByName
	(
	 const char *counter_name,
	 SDebugCounterKey **key,
	 SDebugCounterValue **val
	)
{
	GPOS_ASSERT(NULL != key && NULL == *key);
	GPOS_ASSERT(NULL != val && NULL == *val);

	// search with a newly made key
	*key = GPOS_NEW(m_mp) SDebugCounterKey(counter_name);
	*val = m_hashmap->Find(*key);

	if (NULL != *val)
	{
		// return the yet unused key and the existing value
		return true;
	}

	// return a new key and value pair
	*val = GPOS_NEW(m_mp) SDebugCounterValue();
	return false;
}

// insert or update a key value pair that was generated
// by FindByName()
void CDebugCounter::InsertOrUpdateCounter
	(
	 SDebugCounterKey *key,
	 SDebugCounterValue *val,
	 BOOL update
	)
{
	if (update)
	{
		// we updated the key in-place, delete the extra
		// key that was used for searching
		key->Release();
	}
	else
	{
		// insert a new key/value pair
		m_hashmap->Insert(key, val);
	}
}


void CDebugCounter::Bump(const char *counter_name)
{
	if (OkToProceed())
	{
		AutoDisable preventInfiniteRecursion;

		SDebugCounterKey *key = NULL;
		SDebugCounterValue *val = NULL;
		BOOL found = m_instance->FindByName(counter_name,
											&key,
											&val);

		val->m_counter++;

		m_instance->InsertOrUpdateCounter(key, val, found);
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
