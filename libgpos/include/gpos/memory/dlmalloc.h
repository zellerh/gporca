/*-------------------------------------------------------------------------
 *
 * dlmalloc.h
 *
 * This is a version (aka dlmalloc) of malloc/free/realloc written by
 * Doug Lea and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain.  Send questions,
 * comments, complaints, performance data, etc to dl@cs.oswego.edu
 *
 * Portions Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef dlmalloc_h
#define dlmalloc_h

/* ------------- Global malloc_state and malloc_params ------------------- */
#define NSMALLBINS        (32U)
#define NTREEBINS         (32U)

#include <stddef.h>

/* header used to surround memory allocated with dlmalloc, see dlmalloc.cpp
   for more information. See also the comment in CMemoryPool::AllocHeader
 */

struct malloc_chunk {
	size_t               prev_foot;  /* Size of previous chunk (if free).  */
	size_t               head;       /* Size and inuse bits. */
	struct malloc_chunk* fd;         /* double links -- used only if free. */
	struct malloc_chunk* bk;
};

typedef struct malloc_chunk  mchunk;
typedef struct malloc_chunk* mchunkptr;
typedef struct malloc_chunk* sbinptr;  /* The type of bins of chunks */
typedef unsigned int binmap_t;         /* Described below */
typedef unsigned int flag_t;           /* The type of various bit flag sets */

/* descriptor of a contiguous memory area allocated from a lower layer,
   for use by dlmalloc
 */

struct malloc_segment {
	char*        base;             /* base address */
	size_t       size;             /* allocated size */
	struct malloc_segment* next;   /* ptr to next segment */
	flag_t       sflags;           /* mmap and extern flag */
};

typedef struct malloc_segment  msegment;
typedef struct malloc_segment* msegmentptr;

struct malloc_tree_chunk;
typedef struct malloc_tree_chunk* tbinptr; /* The type of bins of trees */

/* ---------------------------- malloc_state ----------------------------- */

/*
 A malloc_state holds all of the bookkeeping for a space.
 The main fields are:

 Top
 The topmost chunk of the currently active segment. Its size is
 cached in topsize.  The actual size of topmost space is
 topsize+TOP_FOOT_SIZE, which includes space reserved for adding
 fenceposts and segment records if necessary when getting more
 space from the system.

 Designated victim (dv)
 This is the preferred chunk for servicing small requests that
 don't have exact fits.  It is normally the chunk split off most
 recently to service another small request.  Its size is cached in
 dvsize. The link fields of this chunk are not maintained since it
 is not kept in a bin.

 SmallBins
 An array of bin headers for free chunks.  These bins hold chunks
 with sizes less than MIN_LARGE_SIZE bytes. Each bin contains
 chunks of all the same size, spaced 8 bytes apart.  To simplify
 use in double-linked lists, each bin header acts as a malloc_chunk
 pointing to the real first node, if it exists (else pointing to
 itself).  This avoids special-casing for headers.  But to avoid
 waste, we allocate only the fd/bk pointers of bins, and then use
 repositioning tricks to treat these as the fields of a chunk.

 TreeBins
 Treebins are pointers to the roots of trees holding a range of
 sizes. There are 2 equally spaced treebins for each power of two
 from TREE_SHIFT to TREE_SHIFT+16. The last bin holds anything
 larger.

 Bin maps
 There is one bit map for small bins ("smallmap") and one for
 treebins ("treemap).  Each bin sets its bit when non-empty, and
 clears the bit when empty.  Bit operations are then used to avoid
 bin-by-bin searching -- nearly all "search" is done without ever
 looking at bins that won't be selected.  The bit maps
 conservatively use 32 bits per map word, even if on 64bit system.
 For a good description of some of the bit-based techniques used
 here, see Henry S. Warren Jr's book "Hacker's Delight" (and
 supplement at http://hackersdelight.org/). Many of these are
 intended to reduce the branchiness of paths through malloc etc, as
 well as to reduce the number of memory locations read or written.

 Segments
 A list of segments headed by an embedded malloc_segment record
 representing the initial space.

 Address check support
 The least_addr field is the least address ever obtained from
 MORECORE or MMAP. Attempted frees and reallocs of any address less
 than this are trapped (unless INSECURE is defined).

 Magic tag
 A cross-check field to validate pointers to malloc_state.

 Flags
 Bits recording whether to use MMAP, locks, or contiguous MORECORE

 Statistics
 Each space keeps track of current and maximum system memory
 obtained via MORECORE or MMAP.

 */

/* forward declaration */
namespace gpos
{
  class CMemoryPool;
}

/* function pointers used to allocate and free larger chunks of memory
   from the underlying layers
 */
typedef void *dlmalloc_ll_alloc_func(gpos::CMemoryPool *, size_t);
typedef void dlmalloc_ll_dealloc_func(gpos::CMemoryPool *, void *);

struct malloc_state {
	binmap_t   smallmap;
	binmap_t   treemap;
	size_t     dvsize;
	size_t     topsize;
	mchunkptr  dv;
	mchunkptr  top;
	size_t     magic;
	size_t     page_size;
	size_t     granularity;
	gpos::CMemoryPool *pool;
	dlmalloc_ll_alloc_func *ll_alloc_func;
	dlmalloc_ll_dealloc_func *ll_dealloc_func;
	mchunkptr  smallbins[(NSMALLBINS+1)*2];
	tbinptr    treebins[NTREEBINS];
	size_t     footprint;
	size_t     max_footprint;
	msegment   seg;
};

#define dlmalloc_is_initialized(M)  ((M)->top != 0)

#endif /* dlmalloc_h */
