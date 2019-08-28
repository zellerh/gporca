/*-------------------------------------------------------------------------
 *
 * dlmalloc.cpp
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

#include "gpos/error/CException.h"
#include "gpos/memory/dlmalloc.h"
#include "gpos/memory/CMemoryPoolTracker.h"

using namespace gpos;

/*
 * Version 2.8.3 Thu Sep 22 11:16:15 2005  Doug Lea  (dl at gee)

   Note: There may be an updated version of this malloc obtainable at
           ftp://gee.cs.oswego.edu/pub/misc/malloc.c
         Check before installing!

* Quickstart

  This library is all in one file to simplify the most common usage:
  ftp it, compile it (-O3), and link it into another program. All of
  the compile-time options default to reasonable values for use on
  most platforms.  You might later want to step through various
  compile-time and dynamic tuning options.

  For convenience, an include file for code using this malloc is at:
     ftp://gee.cs.oswego.edu/pub/misc/malloc-2.8.3.h
  You don't really need this .h file unless you call functions not
  defined in your system include files.  The .h file contains only the
  excerpts from this file needed for using this malloc on ANSI C/C++
  systems, so long as you haven't changed compile-time options about
  naming and tuning parameters.  If you do, then you can create your
  own malloc.h that does include all settings by cutting at the point
  indicated below. Note that you may already by default be using a C
  library containing a malloc that is based on some version of this
  malloc (for example in linux). You might still want to use the one
  in this file to customize settings or to avoid overheads associated
  with library versions.

* Vital statistics:

  Supported pointer/size_t representation:       4 or 8 bytes
       size_t MUST be an unsigned type of the same width as
       pointers. (If you are using an ancient system that declares
       size_t as a signed type, or need it to be a different width
       than pointers, you can use a previous release of this malloc
       (e.g. 2.7.2) supporting these.)

  Alignment:                                     8 bytes (default)
       This suffices for nearly all current machines and C compilers.
       However, you can define MALLOC_ALIGNMENT to be wider than this
       if necessary (up to 128bytes), at the expense of using more space.

  Minimum overhead per allocated chunk:   4 or  8 bytes (if 4byte sizes)
                                          8 or 16 bytes (if 8byte sizes)
       Each malloced chunk has a hidden word of overhead holding size
       and status information, and additional cross-check word

  Minimum allocated size: 4-byte ptrs:  16 bytes    (including overhead)
                          8-byte ptrs:  32 bytes    (including overhead)

       Even a request for zero bytes (i.e., malloc(0)) returns a
       pointer to something of the minimum allocatable size.
       The maximum overhead wastage (i.e., number of extra bytes
       allocated than were requested in malloc) is less than or equal
       to the minimum size, except for requests >= mmap_threshold that
       are serviced via mmap(), where the worst case wastage is about
       32 bytes plus the remainder from a system page (the minimal
       mmap unit); typically 4096 or 8192 bytes.

  Security: static-safe; optionally more or less
       The "security" of malloc refers to the ability of malicious
       code to accentuate the effects of errors (for example, freeing
       space that is not currently malloc'ed or overwriting past the
       ends of chunks) in code that calls malloc.  This malloc
       guarantees not to modify any memory locations below the base of
       heap, i.e., static variables, even in the presence of usage
       errors.  The routines additionally detect most improper frees
       and reallocs.  All this holds as long as the static bookkeeping
       for malloc itself is not corrupted by some other means.  This
       is only one aspect of security -- these checks do not, and
       cannot, detect all possible programming errors.

       To be able to free memory without a pointer to the malloc_state,
       each chunk carries an additional word that points back to the
       malloc_state.

       By default detected errors cause the program to abort (calling
       "GPOS_ABORT"). You can override this to instead proceed past
       errors by defining PROCEED_ON_ERROR.  In this case, a bad free
       has no effect, and a malloc that encounters a bad address
       caused by user overwrites will ignore the bad address by
       dropping pointers and indices to all known memory. This may
       be appropriate for programs that should continue if at all
       possible in the face of programming errors, although they may
       run out of memory because dropped memory is never reclaimed.

       If you don't like either of these options, you can define
       CORRUPTION_ERROR_ACTION and USAGE_ERROR_ACTION to do anything
       else.

  Thread-safety: NOT thread-safe!

  System requirements: Any combination of MORECORE and/or MMAP/MUNMAP
       This malloc can use unix sbrk or any emulation (invoked using
       the CALL_MORECORE macro) and/or mmap/munmap or any emulation
       (invoked using CALL_MMAP/CALL_MUNMAP) to get and release system
       memory.  On most unix systems, it tends to work best if both
       MORECORE and MMAP are enabled.  On Win32, it uses emulations
       based on VirtualAlloc. It also uses common C library functions
       like memset.

  Compliance: I believe it is compliant with the Single Unix Specification
       (See http://www.unix.org). Also SVID/XPG, ANSI C, and probably
       others as well.

* Overview of algorithms

  This is not the fastest, most space-conserving, most portable, or
  most tunable malloc ever written. However it is among the fastest
  while also being among the most space-conserving, portable and
  tunable.  Consistent balance across these factors results in a good
  general-purpose allocator for malloc-intensive programs.

  In most ways, this malloc is a best-fit allocator. Generally, it
  chooses the best-fitting existing chunk for a request, with ties
  broken in approximately least-recently-used order. (This strategy
  normally maintains low fragmentation.) However, for requests less
  than 256bytes, it deviates from best-fit when there is not an
  exactly fitting available chunk by preferring to use space adjacent
  to that used for the previous small request, as well as by breaking
  ties in approximately most-recently-used order. (These enhance
  locality of series of small allocations.)  And for very large requests
  (>= 256Kb by default), it relies on system memory mapping
  facilities, if supported.  (This helps avoid carrying around and
  possibly fragmenting memory used only for large chunks.)

  All operations (except malloc_stats and mallinfo) have execution
  times that are bounded by a constant factor of the number of bits in
  a size_t, not counting any clearing in calloc or copying in realloc,
  or actions surrounding MORECORE and MMAP that have times
  proportional to the number of non-contiguous regions returned by
  system allocation routines, which is often just 1.

  The implementation is not very modular and seriously overuses
  macros. Perhaps someday all C compilers will do as good a job
  inlining modular code as can now be done by brute-force expansion,
  but now, enough of them seem not to.

  Some compilers issue a lot of warnings about code that is
  dead/unreachable only on some platforms, and also about intentional
  uses of negation on unsigned types. All known cases of each can be
  ignored.

  For a longer but out of date high-level description, see
     http://gee.cs.oswego.edu/dl/html/malloc.html

 -------------------------  Compile-time options ---------------------------

Be careful in setting #define values for numerical constants of type
size_t. On some systems, literal values are not automatically extended
to size_t precision unless they are explicitly casted.

WIN32                    ---- code has been removed

MALLOC_ALIGNMENT         default: (size_t)8
  Controls the minimum alignment for malloc'ed chunks.  It must be a
  power of two and at least 8, even on machines for which smaller
  alignments would suffice. It may be defined as larger than this
  though. Note however that code and data structures are optimized for
  the case of 8-byte alignment.

MSPACES                  ---- code has been removed

ONLY_MSPACES             ---- code has been removed

USE_LOCKS                ---- code has been removed

FOOTERS                  ---- code has been removed, always set

INSECURE                 ---- code has been partially removed,
                              use DLMALLOC_DEBUG for partial check

USE_DL_PREFIX            default: NOT defined
  Causes compiler to prefix all public routines with the string 'dl'.
  This can be useful when you only want to use this malloc in one part
  of a program, using your regular system malloc elsewhere.

ABORT                    ---- code has been removed, use GPOS_ABORT

PROCEED_ON_ERROR           default: defined as 0 (false)
  Controls whether detected bad addresses cause them to bypassed
  rather than aborting. If set, detected bad arguments to free and
  realloc are ignored. And all bookkeeping information is zeroed out
  upon a detected overwrite of freed heap space, thus losing the
  ability to ever return it from malloc again, but enabling the
  application to proceed. If PROCEED_ON_ERROR is defined, the
  static variable malloc_corruption_error_count is compiled in
  and can be examined to see if errors have occurred. This option
  generates slower code than the default abort policy.

DLMALLOC_DEBUG           default: NOT defined
  The DLMALLOC_DEBUG setting is mainly intended for people trying to modify
  this code or diagnose problems when porting to new platforms.
  However, it may also be able to better isolate user errors than just
  using runtime checks.  The assertions in the check routines spell
  out in more detail the assumptions and invariants underlying the
  algorithms.  The checking is fairly extensive, and will slow down
  execution noticeably. Calling malloc_stats or mallinfo with DEBUG
  set will attempt to check every non-mmapped allocated and free chunk
  in the course of computing the summaries.

ABORT_ON_ASSERT_FAILURE   default: defined as 1 (true)
  Debugging assertion failures can be nearly impossible if your
  version of the assert macro causes malloc to be called, which will
  lead to a cascade of further failures, blowing the runtime stack.
  ABORT_ON_ASSERT_FAILURE cause assertions failures to call abort(),
  which will usually make debugging easier.

MALLOC_FAILURE_ACTION     ---- code removed
HAVE_MORECORE             ---- code removed
MORECORE                  ---- replaced with malloc or lower-layer call

MORECORE_CONTIGUOUS       ---- code removed, always 0
  If true, take advantage of fact that consecutive calls to MORECORE
  with positive arguments always return contiguous increasing
  addresses.  This is true of unix sbrk. It does not hurt too much to
  set it true anyway, since malloc copes with non-contiguities.
  Setting it false when definitely non-contiguous saves time
  and possibly wasted space it would take to discover this though.

MORECORE_CANNOT_TRIM      ---- code removed, trim is not allowed
  True if MORECORE cannot release space back to the system when given
  negative arguments. This is generally necessary only if you are
  using a hand-crafted MORECORE function that cannot handle negative
  arguments.

HAVE_MMAP                 ---- code removed

HAVE_MREMAP               default: 1 on linux, else 0
  If true realloc() uses mremap() to re-allocate large blocks and
  extend or shrink allocation spaces.

MMAP_CLEARS               ---- code removed
  True if mmap clears memory so calloc doesn't need to. This is true
  for standard unix mmap using /dev/zero.

USE_BUILTIN_FFS            default: 0 (i.e., not used)
  Causes malloc to use the builtin ffs() function to compute indices.
  Some compilers may recognize and intrinsify ffs to be faster than the
  supplied C version. Also, the case of x86 using gcc is special-cased
  to an asm instruction, so is already as fast as it can be, and so
  this setting has no effect. (On most x86s, the asm version is only
  slightly faster than the C version.)

malloc_getpagesize         default: derive from system includes, or 4096.
  The system page size. To the extent possible, this malloc manages
  memory from the system in page-size units.  This may be (and
  usually is) a function rather than a constant. This is ignored
  if WIN32, where page size is determined using getSystemInfo during
  initialization.

USE_DEV_RANDOM             ---- removed

NO_MALLINFO                ---- removed, no mallinfo

MALLINFO_FIELD_TYPE        ---- removed
  The type of the fields in the mallinfo struct. This was originally
  defined as "int" in SVID etc, but is more usefully defined as
  size_t. The value is used only if  HAVE_USR_INCLUDE_MALLOC_H is not set

REALLOC_ZERO_BYTES_FREES    default: not defined
  This should be set if a call to realloc with zero bytes should 
  be the same as a call to free. Some people think it should. Otherwise, 
  since this malloc returns a unique pointer for malloc(0), so does 
  realloc(p, 0).

LACKS_UNISTD_H, LACKS_FCNTL_H, LACKS_SYS_PARAM_H, LACKS_SYS_MMAN_H
LACKS_STRINGS_H, LACKS_STRING_H, LACKS_SYS_TYPES_H,  LACKS_ERRNO_H
LACKS_STDLIB_H             ---- removed
  Define these if your system does not have these header files.
  You might need to manually insert some of the declarations they provide.

DEFAULT_GRANULARITY        ---- replaced with MIN_GRANULARITY,
                                INC_GRANULARITY and MAX_GRANULARITY
 default: 32KB min, increasing in factors of 8, up to 64 MB
  The unit for allocating and deallocating memory from the system.
  We start with a relatively small size and then increase it exponentially
  to be able to handle larger memories without too much fragmentation.

DEFAULT_TRIM_THRESHOLD    ---- removed

DEFAULT_MMAP_THRESHOLD    ---- removed

*/

#include <sys/types.h>  /* For size_t */

/* The maximum possible size_t value has all bits set */
#define MAX_SIZE_T           (~(size_t)0)

#define MALLOC_ALIGNMENT ((size_t)8U)
#define PROCEED_ON_ERROR 0
#define INSECURE 0
#define USE_BUILTIN_FFS 0

/* granularity (size of chunks allocated from underlying layers,
   increasing exponentially
 */
#define MIN_GRANULARITY ((size_t)32U * (size_t)1024U)
#define INC_GRANULARITY 8
#define MAX_GRANULARITY ((size_t)64U * (size_t)1024U * (size_t)1024U)

/* used to validate footer points to a valid malloc_state */
#define MALLOC_STATE_MAGIC 0x58585858U

/*
  mallopt tuning options.  SVID/XPG defines four standard parameter
  numbers for mallopt, normally defined in malloc.h.  None of these
  are used in this malloc, so setting them has no effect. But this
  malloc does support the following options.
*/


/* ------------------- Declarations of public routines ------------------- */


/*
  dlmalloc(size_t n)
  Returns a pointer to a newly allocated chunk of at least n bytes, or
  null if no space is available.

  If n is zero, malloc returns a minimum-sized chunk. (The minimum size
  is 32 bytes on 64bit systems.)  Note that size_t is an unsigned type, so calls
  with arguments that would be negative if signed are interpreted as
  requests for huge amounts of space, which will often fail. The
  maximum supported value of n differs across systems, but is in all
  cases less than the maximum representable value of a size_t.
*/

/*
  dlfree(void* p)
  Releases the chunk of memory pointed to by p, that had been previously
  allocated using malloc or a related routine such as realloc.
  It has no effect if p is null. If p was not malloced or already
  freed, free(p) will by default cause the current program to abort.
*/

/*
  malloc_footprint();
  Returns the number of bytes obtained from the system.  The total
  number of bytes allocated by malloc, realloc etc., is less than this
  value. Unlike mallinfo, this function returns only a precomputed
  result, so can be called frequently to monitor memory consumption.
  Even if locks are otherwise defined, this function does not use them,
  so results might not be up to date.
*/

/*
  malloc_max_footprint();
  Returns the maximum number of bytes obtained from the system. This
  value will be greater than current footprint if deallocated space
  has been reclaimed by the system. The peak number of bytes allocated
  by malloc, realloc etc., is less than this value. Unlike mallinfo,
  this function returns only a precomputed result, so can be called
  frequently to monitor memory consumption.  Even if locks are
  otherwise defined, this function does not use them, so results might
  not be up to date.
*/

/*------------------------------ internal #includes ---------------------- */

#include <stdio.h>       /* for printing in malloc_stats */

/* ------------------- size_t and alignment properties -------------------- */

/* The byte and bit size of a size_t */
#define SIZE_T_SIZE         (sizeof(size_t))
#define SIZE_T_BITSIZE      (sizeof(size_t) << 3)

/* Some constants coerced to size_t */
/* Annoying but necessary to avoid errors on some plaftorms */
#define SIZE_T_ZERO         ((size_t)0)
#define SIZE_T_ONE          ((size_t)1)
#define SIZE_T_TWO          ((size_t)2)
#define SIZE_T_FOUR         ((size_t)4)
#define TWO_SIZE_T_SIZES    (SIZE_T_SIZE<<1)
#define FOUR_SIZE_T_SIZES   (SIZE_T_SIZE<<2)
#define SIX_SIZE_T_SIZES    (FOUR_SIZE_T_SIZES+TWO_SIZE_T_SIZES)
#define HALF_MAX_SIZE_T     (MAX_SIZE_T / 2U)

/* The bit mask value corresponding to MALLOC_ALIGNMENT */
#define CHUNK_ALIGN_MASK    (MALLOC_ALIGNMENT - SIZE_T_ONE)

/* True if address a has acceptable alignment */
#define is_aligned(A)       (((size_t)((A)) & (CHUNK_ALIGN_MASK)) == 0)

/* the number of bytes to offset an address to align it */
#define align_offset(A)\
 ((((size_t)(A) & CHUNK_ALIGN_MASK) == 0)? 0 :\
  ((MALLOC_ALIGNMENT - ((size_t)(A) & CHUNK_ALIGN_MASK)) & CHUNK_ALIGN_MASK))

/* -----------------------  Chunk representations ------------------------ */

/*
  (The following includes lightly edited explanations by Colin Plumb.)

  The malloc_chunk declaration below is misleading (but accurate and
  necessary).  It declares a "view" into memory allowing access to
  necessary fields at known offsets from a given base.

  Chunks of memory are maintained using a `boundary tag' method as
  originally described by Knuth.  (See the paper by Paul Wilson
  ftp://ftp.cs.utexas.edu/pub/garbage/allocsrv.ps for a survey of such
  techniques.)  Sizes of free chunks are stored both in the front of
  each chunk and at the end.  This makes consolidating fragmented
  chunks into bigger chunks fast.  The head fields also hold bits
  representing whether chunks are free or in use.

  Here are some pictures to make it clearer.  They are "exploded" to
  show that the state of a chunk can be thought of as extending from
  the high 31 bits of the head field of its header through the
  prev_foot and PINUSE_BIT bit of the following chunk header.

  A chunk that's in use looks like:

   chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
           | Size of previous chunk (if P = 1)                             |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ |P|
         | Size of this chunk                                         1| +-+
   mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         |                                                               |
         +-                                                             -+
         |                                                               |
         +-                                                             -+
         |                                                               :
         +-      size - sizeof(size_t) available payload bytes          -+
         :                                                               |
 chunk-> +-                                                             -+
         |                                                               |
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ |1|
       | Size of next chunk (may or may not be in use)               | +-+
 mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

    And if it's free, it looks like this:

   chunk-> +-                                                             -+
           | User payload (must be in use, or we would have merged!)       |
           +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ |P|
         | Size of this chunk                                         0| +-+
   mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         | Next pointer                                                  |
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         | Prev pointer                                                  |
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         |                                                               :
         +-      size - sizeof(struct chunk) unused bytes               -+
         :                                                               |
 chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         | Size of this chunk                                            |
         +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ |0|
       | Size of next chunk (must be in use, or we would have merged)| +-+
 mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       |                                                               :
       +- User payload                                                -+
       :                                                               |
       +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                                                     |0|
                                                                     +-+
  Note that since we always merge adjacent free chunks, the chunks
  adjacent to a free chunk must be in use.

  Given a pointer to a chunk (which can be derived trivially from the
  payload pointer) we can, in O(1) time, find out whether the adjacent
  chunks are free, and if so, unlink them from the lists that they
  are on and merge them with the current chunk.

  Chunks always begin on even word boundaries, so the mem portion
  (which is returned to the user) is also on an even word boundary, and
  thus at least double-word aligned.

  The P (PINUSE_BIT) bit, stored in the unused low-order bit of the
  chunk size (which is always a multiple of two words), is an in-use
  bit for the *previous* chunk.  If that bit is *clear*, then the
  word before the current chunk size contains the previous chunk
  size, and can be used to find the front of the previous chunk.
  The very first chunk allocated always has this bit set, preventing
  access to non-existent (or non-owned) memory. If pinuse is set for
  any given chunk, then you CANNOT determine the size of the
  previous chunk, and might even get a memory addressing fault when
  trying to do so.

  The C (CINUSE_BIT) bit, stored in the unused second-lowest bit of
  the chunk size redundantly records whether the current chunk is
  inuse. This redundancy enables usage checks within free and realloc,
  and reduces indirection when freeing and consolidating chunks.

  Each freshly allocated chunk must have both cinuse and pinuse set.
  That is, each allocated chunk borders either a previously allocated
  and still in-use chunk, or the base of its memory arena. This is
  ensured by making all allocations from the the `lowest' part of any
  found chunk.  Further, no free chunk physically borders another one,
  so each free chunk is known to be preceded and followed by either
  inuse chunks or the ends of memory.

  Note that the `foot' of the current chunk is actually represented
  as the prev_foot of the NEXT chunk. This makes it easier to
  deal with alignments etc but can be very confusing when trying
  to extend or adapt this code.

  The exceptions to all this are

     1. The special chunk `top' is the top-most available chunk (i.e.,
        the one bordering the end of available memory). It is treated
        specially.  Top is never included in any bin, is used only if
        no other chunk is available, and is released back to the
        system if it is very large (see M_TRIM_THRESHOLD).  In effect,
        the top chunk is treated as larger (and thus less well
        fitting) than any other available chunk.  The top chunk
        doesn't update its trailing size field since there is no next
        contiguous chunk that would have to index off it. However,
        space is still allocated for it (TOP_FOOT_SIZE) to enable
        separation or merging when space is extended.

*/


typedef unsigned long bindex_t;         /* Described below */

/* ------------------- Chunks sizes and alignments ----------------------- */

#define MCHUNK_SIZE         (sizeof(mchunk))

/* the overhead a chunk adds to the allocated memory, 16 bytes */
#define CHUNK_OVERHEAD      (TWO_SIZE_T_SIZES)

/* The smallest size we can malloc is an aligned minimal chunk, 32 bytes */
#define MIN_CHUNK_SIZE\
  ((MCHUNK_SIZE + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK)

/* conversion from malloc headers to user pointers, and back */
#define chunk2mem(p)        ((void*)((char*)(p)       + TWO_SIZE_T_SIZES))
#define mem2chunk(mem)      ((mchunkptr)((char*)(mem) - TWO_SIZE_T_SIZES))
/* chunk associated with aligned address A */
#define align_as_chunk(A)   (mchunkptr)((A) + align_offset(chunk2mem(A)))

/* Bounds on request (not chunk) sizes. */
#define MAX_REQUEST         ((-MIN_CHUNK_SIZE) << 2)
#define MIN_REQUEST         (MIN_CHUNK_SIZE - CHUNK_OVERHEAD - SIZE_T_ONE)

/* pad request bytes into a usable size */
#define pad_request(req) \
   (((req) + CHUNK_OVERHEAD + CHUNK_ALIGN_MASK) & ~CHUNK_ALIGN_MASK)

/* pad request, checking for minimum (but not maximum) */
#define request2size(req) \
  (((req) < MIN_REQUEST)? MIN_CHUNK_SIZE : pad_request(req))


/* ------------------ Operations on head and foot fields ----------------- */

/*
  The head field of a chunk is or'ed with PINUSE_BIT when previous
  adjacent chunk in use, and or'ed with CINUSE_BIT if this chunk is in
  use. The prev_foot field has a pointer to the malloc_segment, if the
  previous chunk is in use.
*/

#define PINUSE_BIT          (SIZE_T_ONE)
#define CINUSE_BIT          (SIZE_T_TWO)
#define ARRAY_BIT           (SIZE_T_FOUR)
#define INUSE_BITS          (PINUSE_BIT|CINUSE_BIT)
#define RESERVED_BITS       (INUSE_BITS|ARRAY_BIT)

/* Head value for fenceposts */
#define FENCEPOST_HEAD      (RESERVED_BITS|SIZE_T_SIZE)

/* extraction of fields from head words */
#define cinuse(p)           ((p)->head & CINUSE_BIT)
#define pinuse(p)           ((p)->head & PINUSE_BIT)
#define is_array_alloc(p)   ((p)->head & ARRAY_BIT)
#define chunksize(p)        ((p)->head & ~(RESERVED_BITS))

#define clear_pinuse(p)     ((p)->head &= ~PINUSE_BIT)
#define clear_cinuse(p)     ((p)->head &= ~CINUSE_BIT)

#define set_is_array_alloc(p) ((p)->head |= ARRAY_BIT)

/* Treat space at ptr +/- offset as a chunk */
#define chunk_plus_offset(p, s)  ((mchunkptr)(((char*)(p)) + (s)))
#define chunk_minus_offset(p, s) ((mchunkptr)(((char*)(p)) - (s)))

/* Ptr to next or previous physical malloc_chunk. */
#define next_chunk(p) ((mchunkptr)( ((char*)(p)) + ((p)->head & ~RESERVED_BITS)))
#define prev_chunk(p) ((mchunkptr)( ((char*)(p)) - ((p)->prev_foot) ))

/* extract next chunk's pinuse bit */
#define next_pinuse(p)  ((next_chunk(p)->head) & PINUSE_BIT)

/* Get/set size at footer */
#define get_foot(p, s)  (((mchunkptr)((char*)(p) + (s)))->prev_foot)
#define set_foot(p, s)  (((mchunkptr)((char*)(p) + (s)))->prev_foot = (s))

/* Set size, pinuse bit, and foot */
#define set_size_and_pinuse_of_free_chunk(p, s)\
  ((p)->head = (s|PINUSE_BIT), set_foot(p, s))

/* Set size, pinuse bit, foot, and clear next pinuse */
#define set_free_with_pinuse(p, s, n)\
  (clear_pinuse(n), set_size_and_pinuse_of_free_chunk(p, s))

/* Get the internal overhead associated with chunk p */
#define overhead_for(p)\
 (CHUNK_OVERHEAD)

/* Return true if malloced space is not necessarily cleared */
#define calloc_must_clear(p) (1)

/* ---------------------- Overlaid data structures ----------------------- */

/*
  When chunks are not in use, they are treated as nodes of either
  lists or trees.

  "Small"  chunks are stored in circular doubly-linked lists, and look
  like this:

    chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of previous chunk                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `head:' |             Size of chunk, in bytes                         |P|
      mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Forward pointer to next chunk in list             |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Back pointer to previous chunk in list            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Unused space (may be 0 bytes long)                .
            .                                                               .
            .                                                               |
nextchunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `foot:' |             Size of chunk, in bytes                           |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  Larger chunks are kept in a form of bitwise digital trees (aka
  tries) keyed on chunksizes.  Because malloc_tree_chunks are only for
  free chunks greater than 256 bytes, their size doesn't impose any
  constraints on user chunk sizes.  Each node looks like:

    chunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Size of previous chunk                            |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `head:' |             Size of chunk, in bytes                         |P|
      mem-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Forward pointer to next chunk of same size        |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Back pointer to previous chunk of same size       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Pointer to left child (child[0])                  |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Pointer to right child (child[1])                 |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Pointer to parent                                 |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             bin index of this chunk                           |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            |             Unused space                                      .
            .                                                               |
nextchunk-> +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    `foot:' |             Size of chunk, in bytes                           |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  Each tree holding treenodes is a tree of unique chunk sizes.  Chunks
  of the same size are arranged in a circularly-linked list, with only
  the oldest chunk (the next to be used, in our FIFO ordering)
  actually in the tree.  (Tree members are distinguished by a non-null
  parent pointer.)  If a chunk with the same size an an existing node
  is inserted, it is linked off the existing node using pointers that
  work in the same way as fd/bk pointers of small chunks.

  Each tree contains a power of 2 sized range of chunk sizes (the
  smallest is 0x100 <= x < 0x180), which is is divided in half at each
  tree level, with the chunks in the smaller half of the range (0x100
  <= x < 0x140 for the top nose) in the left subtree and the larger
  half (0x140 <= x < 0x180) in the right subtree.  This is, of course,
  done by inspecting individual bits.

  Using these rules, each node's left subtree contains all smaller
  sizes than its right subtree.  However, the node at the root of each
  subtree has no particular ordering relationship to either.  (The
  dividing line between the subtree sizes is based on trie relation.)
  If we remove the last chunk of a given size from the interior of the
  tree, we need to replace it with a leaf node.  The tree ordering
  rules permit a node to be replaced by any leaf below it.

  The smallest chunk in a tree (a common operation in a best-fit
  allocator) can be found by walking a path to the leftmost leaf in
  the tree.  Unlike a usual binary tree, where we follow left child
  pointers until we reach a null, here we follow the right child
  pointer any time the left one is null, until we reach a leaf with
  both child pointers null. The smallest chunk in the tree will be
  somewhere along that path.

  The worst case number of steps to add, find, or remove a node is
  bounded by the number of bits differentiating chunks within
  bins. Under current bin calculations, this ranges from 6 up to 21
  (for 32 bit sizes) or up to 53 (for 64 bit sizes). The typical case
  is of course much better.
*/

struct malloc_tree_chunk {
  /* The first four fields must be compatible with malloc_chunk */
  size_t                    prev_foot;
  size_t                    head;
  struct malloc_tree_chunk* fd;
  struct malloc_tree_chunk* bk;

  struct malloc_tree_chunk* child[2];
  struct malloc_tree_chunk* parent;
  bindex_t                  index;
};

typedef struct malloc_tree_chunk  tchunk;
typedef struct malloc_tree_chunk* tchunkptr;

/* A little helper macro for trees */
#define leftmost_child(t) ((t)->child[0] != 0? (t)->child[0] : (t)->child[1])

/* ----------------------------- Segments -------------------------------- */

/*
  Each malloc space may include non-contiguous segments, held in a
  list headed by an embedded malloc_segment record representing the
  top-most space. Segments also include flags holding properties of
  the space. Large chunks that are directly allocated by mmap are not
  included in this list. They are instead independently created and
  destroyed without otherwise keeping track of them.

  Segment management mainly comes into play for spaces allocated by
  MMAP.  Any call to MMAP might or might not return memory that is
  adjacent to an existing segment.  MORECORE normally contiguously
  extends the current space, so this space is almost always adjacent,
  which is simpler and faster to deal with. (This is why MORECORE is
  used preferentially to MMAP when both are available -- see
  sys_alloc.)  When allocating using MMAP, we don't use any of the
  hinting mechanisms (inconsistently) supported in various
  implementations of unix mmap, or distinguish reserving from
  committing memory. Instead, we just ask for space, and exploit
  contiguity when we get it.  It is probably possible to do
  better than this on some systems, but no general scheme seems
  to be significantly better.

  Management entails a simpler variant of the consolidation scheme
  used for chunks to reduce fragmentation -- new adjacent memory is
  normally prepended or appended to an existing segment. However,
  there are limitations compared to chunk consolidation that mostly
  reflect the fact that segment processing is relatively infrequent
  (occurring only when getting memory from system) and that we
  don't expect to have huge numbers of segments:

  * Segments are not indexed, so traversal requires linear scans.  (It
    would be possible to index these, but is not worth the extra
    overhead and complexity for most programs on most platforms.)
  * New segments are only appended to old ones when holding top-most
    memory; if they cannot be prepended to others, they are held in
    different segments.

  Except for the top-most segment of a malloc_state, each segment record
  is kept at the tail of its segment. Segments are added by pushing
  segment records onto the list headed by &malloc_state.seg for the
  containing malloc_state *.

  Segment flags control allocation/merge/deallocation policies:
  * If IS_MMAPPED_BIT set, the segment may be merged with
    other surrounding mmapped segments and trimmed/de-allocated
    using munmap.
  * If neither bit is set, then the segment was obtained using
    MORECORE so can be merged with surrounding MORECORE'd segments
    and deallocated/trimmed using MORECORE with negative arguments.
*/



/* Bin types, widths and sizes */
#define SMALLBIN_SHIFT    (3U)
#define SMALLBIN_WIDTH    (SIZE_T_ONE << SMALLBIN_SHIFT)
#define TREEBIN_SHIFT     (8U)
#define MIN_LARGE_SIZE    (SIZE_T_ONE << TREEBIN_SHIFT)
#define MAX_SMALL_SIZE    (MIN_LARGE_SIZE - SIZE_T_ONE)
#define MAX_SMALL_REQUEST (MAX_SMALL_SIZE - CHUNK_ALIGN_MASK - CHUNK_OVERHEAD)

/* -------------------------- system alloc setup ------------------------- */

/* granularity-align a size */
#define granularity_align(S)\
  (((S) + (m_malloc_state.granularity)) & ~(m_malloc_state.granularity - SIZE_T_ONE))

/*  True if segment S holds address A */
#define segment_holds(S, A)\
  ((char*)(A) >= S->base && (char*)(A) < S->base + S->size)

#define is_segment_empty(S)\
 (!cinuse((mchunkptr)(S->base)) && chunksize((mchunkptr)(S->base)) + TOP_FOOT_SIZE + MIN_CHUNK_SIZE - 1 >= S->size)

#ifdef GPOS_DEBUG
/* Return segment holding given address */
static msegmentptr segment_holding(malloc_state * m, char* addr) {
  msegmentptr sp = &m->seg;
  for (;;) {
    if (addr >= sp->base && addr < sp->base + sp->size)
      return sp;
    if ((sp = sp->next) == 0)
      return 0;
  }
}
#endif

/*
  TOP_FOOT_SIZE is padding at the end of a segment, including space
  that may be needed to place segment records and fenceposts when new
  noncontiguous segments are added.
*/
#define TOP_FOOT_SIZE\
  (align_offset(TWO_SIZE_T_SIZES)+pad_request(sizeof(struct malloc_segment))+MIN_CHUNK_SIZE)


/* -------------------------------  Hooks -------------------------------- */


/*
  CORRUPTION_ERROR_ACTION is triggered upon detected bad addresses.
  USAGE_ERROR_ACTION is triggered on detected bad frees and
  reallocs. The argument p is an address that might have triggered the
  fault. It is ignored by the two predefined actions, but might be
  useful in custom actions that try to help diagnose errors.
*/

#define CORRUPTION_ERROR_ACTION(m) GPOS_ABORT
#define USAGE_ERROR_ACTION(m,p) GPOS_ABORT

/* -------------------------- Debugging setup ---------------------------- */

#ifndef GPOS_DEBUG

#define check_free_chunk(M,P)
#define check_inuse_chunk(M,P)
#define check_malloced_chunk(M,P,N)
#define check_malloc_state(M)
#define check_top_chunk(M,P)

#else /* GPOS_DEBUG */
#define check_free_chunk(M,P)       do_check_free_chunk(M,P)
#define check_inuse_chunk(M,P)      do_check_inuse_chunk(M,P)
#define check_top_chunk(M,P)        do_check_top_chunk(M,P)
#define check_malloced_chunk(M,P,N) do_check_malloced_chunk(M,P,N)
#define check_malloc_state(M)       do_check_malloc_state(M)

static void   do_check_any_chunk(malloc_state * m, mchunkptr p);
static void   do_check_top_chunk(malloc_state * m, mchunkptr p);
static void   do_check_inuse_chunk(malloc_state * m, mchunkptr p);
static void   do_check_free_chunk(malloc_state * m, mchunkptr p);
static void   do_check_malloced_chunk(malloc_state * m, void* mem, size_t s);
static void   do_check_tree(malloc_state * m, tchunkptr t);
static void   do_check_treebin(malloc_state * m, bindex_t i);
static void   do_check_smallbin(malloc_state * m, bindex_t i);
static void   do_check_malloc_state(malloc_state * m);
static int    bin_find(malloc_state * m, mchunkptr x);
static size_t traverse_and_check(malloc_state * m);
#endif /* GPOS_DEBUG */

/* ---------------------------- Indexing Bins ---------------------------- */

#define is_small(s)         (((s) >> SMALLBIN_SHIFT) < NSMALLBINS)
#define small_index(s)      ((s)  >> SMALLBIN_SHIFT)
#define small_index2size(i) ((i)  << SMALLBIN_SHIFT)
#define MIN_SMALL_INDEX     (small_index(MIN_CHUNK_SIZE))

/* addressing by index. See above about smallbin repositioning */
#define smallbin_at(M, i)   ((sbinptr)((char*)&((M)->smallbins[(i)<<1])))
#define treebin_at(M,i)     (&((M)->treebins[i]))

/* assign tree index for size S to variable I */
#if defined(__GNUC__) && defined(i386)
#define compute_tree_index(S, I)\
{\
  size_t X = S >> TREEBIN_SHIFT;\
  if (X == 0)\
    I = 0;\
  else if (X > 0xFFFF)\
    I = NTREEBINS-1;\
  else {\
    unsigned int K;\
    __asm__("bsrl %1,%0\n\t" : "=r" (K) : "rm"  (X));\
    I =  (bindex_t)((K << 1) + ((S >> (K + (TREEBIN_SHIFT-1)) & 1)));\
  }\
}
#else /* GNUC */
#define compute_tree_index(S, I)\
{\
  size_t X = S >> TREEBIN_SHIFT;\
  if (X == 0)\
    I = 0;\
  else if (X > 0xFFFF)\
    I = NTREEBINS-1;\
  else {\
    unsigned int Y = (unsigned int)X;\
    unsigned int N = ((Y - 0x100) >> 16) & 8;\
    unsigned int K = (((Y <<= N) - 0x1000) >> 16) & 4;\
    N += K;\
    N += K = (((Y <<= K) - 0x4000) >> 16) & 2;\
    K = 14 - N + ((Y <<= K) >> 15);\
    I = (K << 1) + ((S >> (K + (TREEBIN_SHIFT-1)) & 1));\
  }\
}
#endif /* GNUC */

/* Bit representing maximum resolved size in a treebin at i */
#define bit_for_tree_index(i) \
   (i == NTREEBINS-1)? (SIZE_T_BITSIZE-1) : (((i) >> 1) + TREEBIN_SHIFT - 2)

/* Shift placing maximum resolved bit in a treebin at i as sign bit */
#define leftshift_for_tree_index(i) \
   ((i == NTREEBINS-1)? 0 : \
    ((SIZE_T_BITSIZE-SIZE_T_ONE) - (((i) >> 1) + TREEBIN_SHIFT - 2)))

/* The size of the smallest chunk held in bin with index i */
#define minsize_for_tree_index(i) \
   ((SIZE_T_ONE << (((i) >> 1) + TREEBIN_SHIFT)) |  \
   (((size_t)((i) & SIZE_T_ONE)) << (((i) >> 1) + TREEBIN_SHIFT - 1)))


/* ------------------------ Operations on bin maps ----------------------- */

/* bit corresponding to given index */
#define idx2bit(i)              ((binmap_t)(1) << (i))

/* Mark/Clear bits with given index */
#define mark_smallmap(M,i)      ((M)->smallmap |=  idx2bit(i))
#define clear_smallmap(M,i)     ((M)->smallmap &= ~idx2bit(i))
#define smallmap_is_marked(M,i) ((M)->smallmap &   idx2bit(i))

#define mark_treemap(M,i)       ((M)->treemap  |=  idx2bit(i))
#define clear_treemap(M,i)      ((M)->treemap  &= ~idx2bit(i))
#define treemap_is_marked(M,i)  ((M)->treemap  &   idx2bit(i))

/* index corresponding to given bit */

#if defined(__GNUC__) && defined(i386)
#define compute_bit2idx(X, I)\
{\
  unsigned int J;\
  __asm__("bsfl %1,%0\n\t" : "=r" (J) : "rm" (X));\
  I = (bindex_t)J;\
}

#else /* GNUC */
#if  USE_BUILTIN_FFS
#define compute_bit2idx(X, I) I = ffs(X)-1

#else /* USE_BUILTIN_FFS */
#define compute_bit2idx(X, I)\
{\
  unsigned int Y = X - 1;\
  unsigned int K = Y >> (16-4) & 16;\
  unsigned int N = K;        Y >>= K;\
  N += K = Y >> (8-3) &  8;  Y >>= K;\
  N += K = Y >> (4-2) &  4;  Y >>= K;\
  N += K = Y >> (2-1) &  2;  Y >>= K;\
  N += K = Y >> (1-0) &  1;  Y >>= K;\
  I = (bindex_t)(N + Y);\
}
#endif /* USE_BUILTIN_FFS */
#endif /* GNUC */

/* isolate the least set bit of a bitmap */
#define least_bit(x)         ((x) & -(x))

/* mask with all bits to left of least bit of x on */
#define left_bits(x)         ((x<<1) | -(x<<1))

/* mask with all bits to left of or equal to least bit of x on */
#define same_or_left_bits(x) ((x) | -(x))


/* ----------------------- Runtime Check Support ------------------------- */

/*
  For security, the main invariant is that malloc/free/etc never
  writes to a static address other than malloc_state, unless static
  malloc_state itself has been corrupted, which cannot occur via
  malloc (because of these checks). In essence this means that we
  believe all pointers, sizes, maps etc held in malloc_state, but
  check all of those linked or offsetted from other embedded data
  structures.  These checks are interspersed with main code in a way
  that tends to minimize their run-time cost.

*/

#ifdef DLMALLOC_DEBUG
/* Check if address a is contained in one of the segments */
#define ok_address(M, a) (NULL != segment_holding(M, a))
#else
#define ok_address(M, a) (1)
#endif
/* Check if address of next chunk n is higher than base chunk p */
#define ok_next(p, n)    ((char*)(p) < (char*)(n))
/* Check if p has its cinuse bit on */
#define ok_cinuse(p)     cinuse(p)
/* Check if p has its pinuse bit on */
#define ok_pinuse(p)     pinuse(p)

/* In gcc, use __builtin_expect to minimize impact of checks */
#if !INSECURE
#if defined(__GNUC__) && __GNUC__ >= 3
#define RTCHECK(e)  __builtin_expect(e, 1)
#else /* GNUC */
#define RTCHECK(e)  (e)
#endif /* GNUC */
#else /* !INSECURE */
#define RTCHECK(e)  (1)
#endif /* !INSECURE */

/* macros to set up inuse chunks */

/* Set foot of inuse chunk to be xor of malloc_state * and seed */
#define mark_inuse_foot(M,p,s)\
  (((mchunkptr)((char*)(p) + (s)))->prev_foot = (size_t)(M))

#define get_mstate_for(p)\
  ((malloc_state *)(((mchunkptr)((char*)(p) +\
  (chunksize(p))))->prev_foot))

#define set_inuse_and_pinuse(M,p,s)\
  ((p)->head = (s|PINUSE_BIT|CINUSE_BIT),\
  (((mchunkptr)(((char*)(p)) + (s)))->head |= PINUSE_BIT),\
 mark_inuse_foot(M,p,s))

#define set_size_and_pinuse_of_inuse_chunk(M, p, s)\
  ((p)->head = (s|PINUSE_BIT|CINUSE_BIT),\
  mark_inuse_foot(M, p, s))

/* ------------------------- initalizing the malloc_state ------------------------ */

int gpos::CMemoryPoolTracker::init_mstate() {
  if (m_malloc_state.page_size == 0) {
	m_malloc_state.magic = (size_t) MALLOC_STATE_MAGIC;
    m_malloc_state.page_size = 4096;
    m_malloc_state.granularity = MIN_GRANULARITY;

    /* Sanity-check configuration:
       size_t must be unsigned and as wide as pointer type.
       ints must be at least 4 bytes.
       alignment must be at least 8.
       Alignment, min chunk size, and page size must all be powers of 2.
    */
    if ((sizeof(size_t) != sizeof(char*)) ||
        (MAX_SIZE_T < MIN_CHUNK_SIZE)  ||
        (sizeof(int) < 4)  ||
        (MALLOC_ALIGNMENT < (size_t)8U) ||
        ((MALLOC_ALIGNMENT    & (MALLOC_ALIGNMENT-SIZE_T_ONE))    != 0) ||
        ((MCHUNK_SIZE         & (MCHUNK_SIZE-SIZE_T_ONE))         != 0) ||
        ((m_malloc_state.granularity & (m_malloc_state.granularity-SIZE_T_ONE)) != 0) ||
        ((m_malloc_state.page_size   & (m_malloc_state.page_size-SIZE_T_ONE))   != 0))
      GPOS_ABORT;
  }
  return 0;
}

#ifdef GPOS_DEBUG
/* ------------------------- Debugging Support --------------------------- */

/* Check properties of any chunk, whether free, inuse etc  */
static void do_check_any_chunk(malloc_state * m, mchunkptr p) {
  GPOS_ASSERT((is_aligned(chunk2mem(p))) || (p->head == FENCEPOST_HEAD));
  GPOS_ASSERT(NULL != m);
  GPOS_ASSERT(ok_address(m, p));
}

/* Check properties of top chunk */
static void do_check_top_chunk(malloc_state * m, mchunkptr p) {
  msegmentptr sp = segment_holding(m, (char*)p);
  size_t  sz = chunksize(p);
  GPOS_ASSERT(sp != 0);
  GPOS_ASSERT((is_aligned(chunk2mem(p))) || (p->head == FENCEPOST_HEAD));
  GPOS_ASSERT(ok_address(m, p));
  GPOS_ASSERT(sz == m->topsize);
  GPOS_ASSERT(sz > 0);
  GPOS_ASSERT(sz == ((sp->base + sp->size) - (char*)p) - TOP_FOOT_SIZE);
  GPOS_ASSERT(pinuse(p));
  GPOS_ASSERT(!next_pinuse(p));
}

/* Check properties of inuse chunks */
static void do_check_inuse_chunk(malloc_state * m, mchunkptr p) {
  do_check_any_chunk(m, p);
  GPOS_ASSERT(cinuse(p));
  GPOS_ASSERT(next_pinuse(p));
  /* If not pinuse, previous chunk has OK offset */
  GPOS_ASSERT(pinuse(p) || next_chunk(prev_chunk(p)) == p);
}

/* Check properties of free chunks */
static void do_check_free_chunk(malloc_state * m, mchunkptr p) {
  size_t sz = p->head & ~(PINUSE_BIT|CINUSE_BIT);
  mchunkptr next = chunk_plus_offset(p, sz);
  do_check_any_chunk(m, p);
  GPOS_ASSERT(!cinuse(p));
  GPOS_ASSERT(!next_pinuse(p));
  if (p != m->dv && p != m->top) {
    if (sz >= MIN_CHUNK_SIZE) {
      GPOS_ASSERT((sz & CHUNK_ALIGN_MASK) == 0);
      GPOS_ASSERT(is_aligned(chunk2mem(p)));
      GPOS_ASSERT(next->prev_foot == sz);
      GPOS_ASSERT(pinuse(p));
      GPOS_ASSERT(next == m->top || cinuse(next));
      GPOS_ASSERT(p->fd->bk == p);
      GPOS_ASSERT(p->bk->fd == p);
    }
    else  /* markers are always of size SIZE_T_SIZE */
      GPOS_ASSERT(sz == SIZE_T_SIZE);
  }
}

/* Check properties of malloced chunks at the point they are malloced */
static void do_check_malloced_chunk(malloc_state * m, void* mem, size_t s) {
  if (mem != 0) {
    mchunkptr p = mem2chunk(mem);
    size_t sz = p->head & ~(PINUSE_BIT|CINUSE_BIT);
    do_check_inuse_chunk(m, p);
    GPOS_ASSERT((sz & CHUNK_ALIGN_MASK) == 0);
    GPOS_ASSERT(sz >= MIN_CHUNK_SIZE);
    GPOS_ASSERT(sz >= s);
    /* size is less than MIN_CHUNK_SIZE more than request */
    GPOS_ASSERT(sz < (s + MIN_CHUNK_SIZE));
  }
}

/* Check a tree and its subtrees.  */
static void do_check_tree(malloc_state * m, tchunkptr t) {
  tchunkptr head = 0;
  tchunkptr u = t;
  bindex_t tindex = t->index;
  size_t tsize = chunksize(t);
  bindex_t idx;
  compute_tree_index(tsize, idx);
  GPOS_ASSERT(tindex == idx);
  GPOS_ASSERT(tsize >= MIN_LARGE_SIZE);
  GPOS_ASSERT(tsize >= minsize_for_tree_index(idx));
  GPOS_ASSERT((idx == NTREEBINS-1) || (tsize < minsize_for_tree_index((idx+1))));

  do { /* traverse through chain of same-sized nodes */
    do_check_any_chunk(m, ((mchunkptr)u));
    GPOS_ASSERT(u->index == tindex);
    GPOS_ASSERT(chunksize(u) == tsize);
    GPOS_ASSERT(!cinuse(u));
    GPOS_ASSERT(!next_pinuse(u));
    GPOS_ASSERT(u->fd->bk == u);
    GPOS_ASSERT(u->bk->fd == u);
    if (u->parent == 0) {
      GPOS_ASSERT(u->child[0] == 0);
      GPOS_ASSERT(u->child[1] == 0);
    }
    else {
      GPOS_ASSERT(head == 0); /* only one node on chain has parent */
      head = u;
      GPOS_ASSERT(u->parent != u);
      GPOS_ASSERT(u->parent->child[0] == u ||
              u->parent->child[1] == u ||
              *((tbinptr*)(u->parent)) == u);
      if (u->child[0] != 0) {
        GPOS_ASSERT(u->child[0]->parent == u);
        GPOS_ASSERT(u->child[0] != u);
        do_check_tree(m, u->child[0]);
      }
      if (u->child[1] != 0) {
        GPOS_ASSERT(u->child[1]->parent == u);
        GPOS_ASSERT(u->child[1] != u);
        do_check_tree(m, u->child[1]);
      }
      if (u->child[0] != 0 && u->child[1] != 0) {
        GPOS_ASSERT(chunksize(u->child[0]) < chunksize(u->child[1]));
      }
    }
    u = u->fd;
  } while (u != t);
  GPOS_ASSERT(head != 0);
}

/*  Check all the chunks in a treebin.  */
static void do_check_treebin(malloc_state * m, bindex_t i) {
  tbinptr* tb = treebin_at(m, i);
  tchunkptr t = *tb;
  int empty = (m->treemap & (1U << i)) == 0;
  if (t == 0)
    GPOS_ASSERT(empty);
  if (!empty)
    do_check_tree(m, t);
}

/*  Check all the chunks in a smallbin.  */
static void do_check_smallbin(malloc_state * m, bindex_t i) {
  sbinptr b = smallbin_at(m, i);
  mchunkptr p = b->bk;
  unsigned int empty = (m->smallmap & (1U << i)) == 0;
  if (p == b)
    GPOS_ASSERT(empty);
  if (!empty) {
    for (; p != b; p = p->bk) {
      size_t size = chunksize(p);
      mchunkptr q;
      /* each chunk claims to be free */
      do_check_free_chunk(m, p);
      /* chunk belongs in bin */
      GPOS_ASSERT(small_index(size) == i);
      GPOS_ASSERT(p->bk == b || chunksize(p->bk) == chunksize(p));
      /* chunk is followed by an inuse chunk */
      q = next_chunk(p);
      if (q->head != FENCEPOST_HEAD)
        do_check_inuse_chunk(m, q);
    }
  }
}

/* Find x in a bin. Used in other check functions. */
static int bin_find(malloc_state * m, mchunkptr x) {
  size_t size = chunksize(x);
  if (is_small(size)) {
    bindex_t sidx = small_index(size);
    sbinptr b = smallbin_at(m, sidx);
    if (smallmap_is_marked(m, sidx)) {
      mchunkptr p = b;
      do {
        if (p == x)
          return 1;
      } while ((p = p->fd) != b);
    }
  }
  else {
    bindex_t tidx;
    compute_tree_index(size, tidx);
    if (treemap_is_marked(m, tidx)) {
      tchunkptr t = *treebin_at(m, tidx);
      size_t sizebits = size << leftshift_for_tree_index(tidx);
      while (t != 0 && chunksize(t) != size) {
        t = t->child[(sizebits >> (SIZE_T_BITSIZE-SIZE_T_ONE)) & 1];
        sizebits <<= 1;
      }
      if (t != 0) {
        tchunkptr u = t;
        do {
          if (u == (tchunkptr)x)
            return 1;
        } while ((u = u->fd) != t);
      }
    }
  }
  return 0;
}

/* Traverse each chunk and check it; return total */
static size_t traverse_and_check(malloc_state * m) {
  size_t sum = 0;
  if (dlmalloc_is_initialized(m)) {
    msegmentptr s = &m->seg;
    sum += m->topsize + TOP_FOOT_SIZE;
    while (s != 0) {
      mchunkptr q = align_as_chunk(s->base);
      mchunkptr lastq = 0;
      GPOS_ASSERT(pinuse(q));
      while (segment_holds(s, q) &&
             q != m->top && q->head != FENCEPOST_HEAD) {
        sum += chunksize(q);
        if (cinuse(q)) {
          GPOS_ASSERT(!bin_find(m, q));
          do_check_inuse_chunk(m, q);
        }
        else {
          GPOS_ASSERT(q == m->dv || bin_find(m, q));
          GPOS_ASSERT(lastq == 0 || cinuse(lastq)); /* Not 2 consecutive free */
          do_check_free_chunk(m, q);
        }
        lastq = q;
        q = next_chunk(q);
      }
      s = s->next;
    }
  }
  return sum;
}

/* Check all properties of malloc_state. */
static void do_check_malloc_state(malloc_state * m) {
  bindex_t i;
  size_t total;
  /* check bins */
  for (i = 0; i < NSMALLBINS; ++i)
    do_check_smallbin(m, i);
  for (i = 0; i < NTREEBINS; ++i)
    do_check_treebin(m, i);

  if (m->dvsize != 0) { /* check dv chunk */
    do_check_any_chunk(m, m->dv);
    GPOS_ASSERT(m->dvsize == chunksize(m->dv));
    GPOS_ASSERT(m->dvsize >= MIN_CHUNK_SIZE);
    GPOS_ASSERT(bin_find(m, m->dv) == 0);
  }

  if (m->top != 0) {   /* check top chunk */
    do_check_top_chunk(m, m->top);
    GPOS_ASSERT(m->topsize == chunksize(m->top));
    GPOS_ASSERT(m->topsize > 0);
    GPOS_ASSERT(bin_find(m, m->top) == 0);
  }

  total = traverse_and_check(m);
  GPOS_ASSERT(total <= m->footprint);
  GPOS_ASSERT(m->footprint <= m->max_footprint);
}

void
CMemoryPoolTracker::checkConsistency()
{
	do_check_malloc_state(&m_malloc_state);
}

#endif /* GPOS_DEBUG */

///* ----------------------------- statistics ------------------------------ */
//
//static void internal_malloc_stats(malloc_state * m) {
//  if (!PREACTION(m)) {
//    size_t maxfp = 0;
//    size_t fp = 0;
//    size_t used = 0;
//    check_malloc_state(m);
//    if (dlmalloc_is_initialized(m)) {
//      msegmentptr s = &m->seg;
//      maxfp = m->max_footprint;
//      fp = m->footprint;
//      used = fp - (m->topsize + TOP_FOOT_SIZE);
//
//      while (s != 0) {
//        mchunkptr q = align_as_chunk(s->base);
//        while (segment_holds(s, q) &&
//               q != m->top && q->head != FENCEPOST_HEAD) {
//          if (!cinuse(q))
//            used -= chunksize(q);
//          q = next_chunk(q);
//        }
//        s = s->next;
//      }
//    }
//
//    fprintf(stderr, "max system bytes = %10lu\n", (unsigned long)(maxfp));
//    fprintf(stderr, "system bytes     = %10lu\n", (unsigned long)(fp));
//    fprintf(stderr, "in use bytes     = %10lu\n", (unsigned long)(used));
//
//  }
//}

/* ----------------------- Operations on smallbins ----------------------- */

/*
  Various forms of linking and unlinking are defined as macros.  Even
  the ones for trees, which are very long but have very short typical
  paths.  This is ugly but reduces reliance on inlining support of
  compilers.
*/

/* Link a free chunk into a smallbin  */
#define insert_small_chunk(M, P, S) {\
  bindex_t I  = small_index(S);\
  mchunkptr B = smallbin_at(M, I);\
  mchunkptr F = B;\
  GPOS_ASSERT(S >= MIN_CHUNK_SIZE);\
  if (!smallmap_is_marked(M, I))\
    mark_smallmap(M, I);\
  else if (RTCHECK(ok_address(M, B->fd)))\
    F = B->fd;\
  else {\
    CORRUPTION_ERROR_ACTION(M);\
  }\
  B->fd = P;\
  F->bk = P;\
  P->fd = F;\
  P->bk = B;\
}

/* Unlink a chunk from a smallbin  */
#define unlink_small_chunk(M, P, S) {\
  mchunkptr F = P->fd;\
  mchunkptr B = P->bk;\
  bindex_t I = small_index(S);\
  GPOS_ASSERT(P != B);\
  GPOS_ASSERT(P != F);\
  GPOS_ASSERT(chunksize(P) == small_index2size(I));\
  if (F == B)\
    clear_smallmap(M, I);\
  else if (RTCHECK((F == smallbin_at(M,I) || ok_address(M, F)) &&\
                   (B == smallbin_at(M,I) || ok_address(M, B)))) {\
    F->bk = B;\
    B->fd = F;\
  }\
  else {\
    CORRUPTION_ERROR_ACTION(M);\
  }\
}

/* Unlink the first chunk from a smallbin */
#define unlink_first_small_chunk(M, B, P, I) {\
  mchunkptr F = P->fd;\
  GPOS_ASSERT(P != B);\
  GPOS_ASSERT(P != F);\
  GPOS_ASSERT(chunksize(P) == small_index2size(I));\
  if (B == F)\
    clear_smallmap(M, I);\
  else if (RTCHECK(ok_address(M, F))) {\
    B->fd = F;\
    F->bk = B;\
  }\
  else {\
    CORRUPTION_ERROR_ACTION(M);\
  }\
}

/* Replace dv node, binning the old one */
/* Used only when dvsize known to be small */
#define replace_dv(M, P, S) {\
  size_t DVS = (M).dvsize;\
  if (DVS != 0) {\
    mchunkptr DV = (M).dv;\
    GPOS_ASSERT(is_small(DVS));\
    insert_small_chunk(&(M), DV, DVS);\
  }\
  (M).dvsize = S;\
  (M).dv = P;\
}

/* ------------------------- Operations on trees ------------------------- */

/* Insert chunk into tree */
#define insert_large_chunk(M, X, S) {\
  tbinptr* H;\
  bindex_t I;\
  compute_tree_index(S, I);\
  H = treebin_at(M, I);\
  X->index = I;\
  X->child[0] = X->child[1] = 0;\
  if (!treemap_is_marked(M, I)) {\
    mark_treemap(M, I);\
    *H = X;\
    X->parent = (tchunkptr)H;\
    X->fd = X->bk = X;\
  }\
  else {\
    tchunkptr T = *H;\
    size_t K = S << leftshift_for_tree_index(I);\
    for (;;) {\
      if (chunksize(T) != S) {\
        tchunkptr* C = &(T->child[(K >> (SIZE_T_BITSIZE-SIZE_T_ONE)) & 1]);\
        K <<= 1;\
        if (*C != 0)\
          T = *C;\
        else if (RTCHECK(ok_address(M, C))) {\
          *C = X;\
          X->parent = T;\
          X->fd = X->bk = X;\
          break;\
        }\
        else {\
          CORRUPTION_ERROR_ACTION(M);\
          break;\
        }\
      }\
      else {\
        tchunkptr F = T->fd;\
        if (RTCHECK(ok_address(M, T) && ok_address(M, F))) {\
          T->fd = F->bk = X;\
          X->fd = F;\
          X->bk = T;\
          X->parent = 0;\
          break;\
        }\
        else {\
          CORRUPTION_ERROR_ACTION(M);\
          break;\
        }\
      }\
    }\
  }\
}

/*
  Unlink steps:

  1. If x is a chained node, unlink it from its same-sized fd/bk links
     and choose its bk node as its replacement.
  2. If x was the last node of its size, but not a leaf node, it must
     be replaced with a leaf node (not merely one with an open left or
     right), to make sure that lefts and rights of descendents
     correspond properly to bit masks.  We use the rightmost descendent
     of x.  We could use any other leaf, but this is easy to locate and
     tends to counteract removal of leftmosts elsewhere, and so keeps
     paths shorter than minimally guaranteed.  This doesn't loop much
     because on average a node in a tree is near the bottom.
  3. If x is the base of a chain (i.e., has parent links) relink
     x's parent and children to x's replacement (or null if none).
*/

#define unlink_large_chunk(M, X) {\
  tchunkptr XP = X->parent;\
  tchunkptr R;\
  if (X->bk != X) {\
    tchunkptr F = X->fd;\
    R = X->bk;\
    if (RTCHECK(ok_address(M, F))) {\
      F->bk = R;\
      R->fd = F;\
    }\
    else {\
      CORRUPTION_ERROR_ACTION(M);\
    }\
  }\
  else {\
    tchunkptr* RP;\
    if (((R = *(RP = &(X->child[1]))) != 0) ||\
        ((R = *(RP = &(X->child[0]))) != 0)) {\
      tchunkptr* CP;\
      while ((*(CP = &(R->child[1])) != 0) ||\
             (*(CP = &(R->child[0])) != 0)) {\
        R = *(RP = CP);\
      }\
      if (RTCHECK(ok_address(M, RP)))\
        *RP = 0;\
      else {\
        CORRUPTION_ERROR_ACTION(M);\
      }\
    }\
  }\
  if (XP != 0) {\
    tbinptr* H = treebin_at(M, X->index);\
    if (X == *H) {\
      if ((*H = R) == 0) \
        clear_treemap(M, X->index);\
    }\
    else if (RTCHECK(ok_address(M, XP))) {\
      if (XP->child[0] == X) \
        XP->child[0] = R;\
      else \
        XP->child[1] = R;\
    }\
    else\
      CORRUPTION_ERROR_ACTION(M);\
    if (R != 0) {\
      if (RTCHECK(ok_address(M, R))) {\
        tchunkptr C0, C1;\
        R->parent = XP;\
        if ((C0 = X->child[0]) != 0) {\
          if (RTCHECK(ok_address(M, C0))) {\
            R->child[0] = C0;\
            C0->parent = R;\
          }\
          else\
            CORRUPTION_ERROR_ACTION(M);\
        }\
        if ((C1 = X->child[1]) != 0) {\
          if (RTCHECK(ok_address(M, C1))) {\
            R->child[1] = C1;\
            C1->parent = R;\
          }\
          else\
            CORRUPTION_ERROR_ACTION(M);\
        }\
      }\
      else\
        CORRUPTION_ERROR_ACTION(M);\
    }\
  }\
}

/* Relays to large vs small bin operations */

#define insert_chunk(M, P, S)\
  if (is_small(S)) insert_small_chunk(M, P, S)\
  else { tchunkptr TP = (tchunkptr)(P); insert_large_chunk(M, TP, S); }

#define unlink_chunk(M, P, S)\
  if (is_small(S)) unlink_small_chunk(M, P, S)\
  else { tchunkptr TP = (tchunkptr)(P); unlink_large_chunk(M, TP); }


/* Initialize top chunk and its size */
static void init_top(malloc_state * m, mchunkptr p, size_t psize) {
  /* Ensure alignment */
  size_t offset = align_offset(chunk2mem(p));
  p = (mchunkptr)((char*)p + offset);
  psize -= offset;

  m->top = p;
  m->topsize = psize;
  p->head = psize | PINUSE_BIT;
  p->prev_foot = 0;
  /* set size of fake trailing chunk holding overhead space only once */
  chunk_plus_offset(p, psize)->head = TOP_FOOT_SIZE;
}

/* Initialize bins for a new malloc_state * that is otherwise zeroed out */
static void init_bins(malloc_state * m) {
  /* Establish circular links for smallbins */
  bindex_t i;
  for (i = 0; i < NSMALLBINS; ++i) {
    sbinptr bin = smallbin_at(m,i);
    bin->fd = bin->bk = bin;
  }
}

/* Add a segment to hold a new noncontiguous region */
static void add_segment(malloc_state * m, char* tbase, size_t tsize) {
  /* Determine locations and sizes of segment, fenceposts, old top */
  char* old_top = (char*)m->top;
  /* segment holding the top */
  msegmentptr oldsp = &(m->seg);
  GPOS_ASSERT(old_top == NULL || oldsp == segment_holding(m, old_top));
  char* old_end = oldsp->base + oldsp->size;
  size_t ssize = pad_request(sizeof(struct malloc_segment));
  char* rawsp = old_end - (ssize + FOUR_SIZE_T_SIZES + CHUNK_ALIGN_MASK);
  size_t offset = align_offset(chunk2mem(rawsp));
  char* asp = rawsp + offset;
  /* if old_top is a chunk too small to be used, then discard it */
  char* csp = (old_top != NULL && asp < (old_top + MIN_CHUNK_SIZE))? old_top : asp;
  mchunkptr sp = (mchunkptr)csp;
  msegmentptr ss = (msegmentptr)(chunk2mem(sp));
  mchunkptr tnext = chunk_plus_offset(sp, ssize);
  mchunkptr p = tnext;
  int nfences = 0;

  /* reset top to new space */
  init_top(m, (mchunkptr)tbase, tsize - TOP_FOOT_SIZE);

  /* Set up segment record */
  GPOS_ASSERT(is_aligned(ss));
  set_size_and_pinuse_of_inuse_chunk(m, sp, ssize);
  *ss = m->seg; /* Push current record */
  m->seg.base = tbase;
  m->seg.size = tsize;
  m->seg.sflags = 0;
  m->seg.next = ss;

  /* Insert trailing fenceposts */
  for (;;) {
    mchunkptr nextp = chunk_plus_offset(p, SIZE_T_SIZE);
    p->head = FENCEPOST_HEAD;
    ++nfences;
    if ((char*)(&(nextp->head)) < old_end)
      p = nextp;
    else
      break;
  }
  GPOS_ASSERT(nfences >= 2);

  /* Insert the rest of old top into a bin as an ordinary free chunk */
  if (old_top != NULL && csp != old_top) {
    mchunkptr q = (mchunkptr)old_top;
    size_t psize = csp - old_top;
    mchunkptr tn = chunk_plus_offset(q, psize);
    set_free_with_pinuse(q, psize, tn);
    insert_chunk(m, q, psize);
  }

  check_top_chunk(m, m->top);
}

/* Unlink an empty segment from the data structures in preparation to deleting its memory,
   return whether we were able to unlink the segment
 */
static bool unlink_segment(malloc_state * m, msegmentptr s, msegmentptr *next_seg) {
	GPOS_ASSERT(NULL != s->base);
	*next_seg = s->next;
	if (!is_segment_empty(s)) {
		return false;
	}
	if (segment_holds(s, m->top)) {
		/* this segment is the top chunk, set the top chunk to NULL */
		m->top = NULL;
		m->topsize = 0;
	}
	else if (segment_holds(s, m->dv)) {
		/* this segment is the designated victim (dv), reset the dv */
		m->dvsize = 0;
		m->dv = 0;
	}
	else {
		/* unlink it from the bins, if needed */
		mchunkptr p = (mchunkptr) s->base;
		unlink_chunk(m, p, chunksize(p));
	}
	/* unlink the segment from the list of segments */
	msegmentptr ls = &(m->seg);

	if (ls == s) {
		if (s->next != NULL) {
			/* restore the next segment in the list back into m */
			*ls = *(s->next);
			*next_seg = ls;
		}
		else {
			/* we unlinked the last segment, set m->seg back to an empty state */
			ls->base = NULL;
			ls->size = 0;
			ls->next = NULL;
			*next_seg = NULL;
		}
	}
	else {
		/* we unlinked a segment other than the one in m, unlink it from the list */
		while (ls != NULL) {
			if (ls->next == s) {
				ls->next = s->next;
				break;
			}
			ls = ls->next;
		}
		GPOS_ASSERT(ls != NULL);
	}
	return true;
}

/* -------------------------- System allocation -------------------------- */

/* Get memory from system using MORECORE or MMAP */
void* gpos::CMemoryPoolTracker::sys_alloc(malloc_state *m, size_t nb) {
  char* tbase = NULL;
  size_t tsize = 0;
  flag_t mmap_flag = 0;

  init_mstate();

  /*
	Get memory from the underlying memory provider
  */

  size_t asize = granularity_align(nb + TOP_FOOT_SIZE + SIZE_T_ONE);
  if (asize < HALF_MAX_SIZE_T) {
	/* Call the function pointer provided to get memory from a lower layer */
	char* br = (char*) (*(m->ll_alloc_func))(m->pool, asize);
	if (br != NULL) {
      tbase = br;
      tsize = asize;
      if (m->granularity < MAX_GRANULARITY) {
        /* as our memory pool grows, increase the size allocated through sys_alloc */
        m->granularity *= INC_GRANULARITY;
      }
    }
  }

  if (tbase != NULL) {

    if ((m_malloc_state.footprint += tsize) > m_malloc_state.max_footprint)
      m_malloc_state.max_footprint = m_malloc_state.footprint;

    if (!dlmalloc_is_initialized(&m_malloc_state)) { /* first-time initialization */
      m_malloc_state.seg.base = tbase;
      m_malloc_state.seg.size = tsize;
      m_malloc_state.seg.sflags = mmap_flag;
      init_bins(&m_malloc_state);
	  init_top(&m_malloc_state, (mchunkptr)tbase, tsize - TOP_FOOT_SIZE);
    }
    else {
      add_segment(&m_malloc_state, tbase, tsize);
    }

    if (nb < m_malloc_state.topsize) { /* Allocate from new or extended top space */
      size_t rsize = m_malloc_state.topsize -= nb;
      mchunkptr p = m_malloc_state.top;
      mchunkptr r = m_malloc_state.top = chunk_plus_offset(p, nb);
      r->head = rsize | PINUSE_BIT;
      set_size_and_pinuse_of_inuse_chunk(&m_malloc_state, p, nb);
      check_top_chunk(&m_malloc_state, m_malloc_state.top);
      check_malloced_chunk(&m_malloc_state, chunk2mem(p), nb);
      return chunk2mem(p);
    }
  }

  return NULL;
}

/* ---------------------------- malloc support --------------------------- */

/* allocate a large request from the best fitting chunk in a treebin */
static void* tmalloc_large(malloc_state * m, size_t nb) {
  tchunkptr v = 0;
  size_t rsize = -nb; /* Unsigned negation */
  tchunkptr t;
  bindex_t idx;
  compute_tree_index(nb, idx);

  if ((t = *treebin_at(m, idx)) != 0) {
    /* Traverse tree for this bin looking for node with size == nb */
    size_t sizebits = nb << leftshift_for_tree_index(idx);
    tchunkptr rst = 0;  /* The deepest untaken right subtree */
    for (;;) {
      tchunkptr rt;
      size_t trem = chunksize(t) - nb;
      if (trem < rsize) {
        v = t;
        if ((rsize = trem) == 0)
          break;
      }
      rt = t->child[1];
      t = t->child[(sizebits >> (SIZE_T_BITSIZE-SIZE_T_ONE)) & 1];
      if (rt != 0 && rt != t)
        rst = rt;
      if (t == 0) {
        t = rst; /* set t to least subtree holding sizes > nb */
        break;
      }
      sizebits <<= 1;
    }
  }

  if (t == 0 && v == 0) { /* set t to root of next non-empty treebin */
    binmap_t leftbits = left_bits(idx2bit(idx)) & m->treemap;
    if (leftbits != 0) {
      bindex_t i;
      binmap_t leastbit = least_bit(leftbits);
      compute_bit2idx(leastbit, i);
      t = *treebin_at(m, i);
    }
  }

  while (t != 0) { /* find smallest of tree or subtree */
    size_t trem = chunksize(t) - nb;
    if (trem < rsize) {
      rsize = trem;
      v = t;
    }
    t = leftmost_child(t);
  }

  /*  If dv is a better fit, return 0 so malloc will use it */
  if (v != 0 && rsize < (size_t)(m->dvsize - nb)) {
    if (RTCHECK(ok_address(m, v))) { /* split */
      mchunkptr r = chunk_plus_offset(v, nb);
      GPOS_ASSERT(chunksize(v) == rsize + nb);
      if (RTCHECK(ok_next(v, r))) {
        unlink_large_chunk(m, v);
        if (rsize < MIN_CHUNK_SIZE)
          set_inuse_and_pinuse(m, v, (rsize + nb));
        else {
          set_size_and_pinuse_of_inuse_chunk(m, v, nb);
          set_size_and_pinuse_of_free_chunk(r, rsize);
          insert_chunk(m, r, rsize);
        }
        return chunk2mem(v);
      }
    }
    CORRUPTION_ERROR_ACTION(m);
  }
  return 0;
}

/* allocate a small request from the best fitting chunk in a treebin */
static void* tmalloc_small(malloc_state * m, size_t nb) {
  tchunkptr t, v;
  size_t rsize;
  bindex_t i;
  binmap_t leastbit = least_bit(m->treemap);
  compute_bit2idx(leastbit, i);

  v = t = *treebin_at(m, i);
  rsize = chunksize(t) - nb;

  while ((t = leftmost_child(t)) != 0) {
    size_t trem = chunksize(t) - nb;
    if (trem < rsize) {
      rsize = trem;
      v = t;
    }
  }

  if (RTCHECK(ok_address(m, v))) {
    mchunkptr r = chunk_plus_offset(v, nb);
    GPOS_ASSERT(chunksize(v) == rsize + nb);
    if (RTCHECK(ok_next(v, r))) {
      unlink_large_chunk(m, v);
      if (rsize < MIN_CHUNK_SIZE)
        set_inuse_and_pinuse(m, v, (rsize + nb));
      else {
        set_size_and_pinuse_of_inuse_chunk(m, v, nb);
        set_size_and_pinuse_of_free_chunk(r, rsize);
        replace_dv(*m, r, rsize);
      }
      return chunk2mem(v);
    }
  }

  CORRUPTION_ERROR_ACTION(m);
  return 0;
}


/* -------------------------- public routines ---------------------------- */


void* gpos::CMemoryPoolTracker::dlmalloc(size_t bytes) {
  /*
     Basic algorithm:
     If a small request (< 256 bytes minus per-chunk overhead):
       1. If one exists, use a remainderless chunk in associated smallbin.
          (Remainderless means that there are too few excess bytes to
          represent as a chunk.)
       2. If it is big enough, use the dv chunk, which is normally the
          chunk adjacent to the one used for the most recent small request.
       3. If one exists, split the smallest available chunk in a bin,
          saving remainder in dv.
       4. If it is big enough, use the top chunk.
       5. If available, get memory from system and use it
     Otherwise, for a large request:
       1. Find the smallest available binned chunk that fits, and use it
          if it is better fitting than dv chunk, splitting if necessary.
       2. If better fitting than any binned chunk, use the dv chunk.
       3. If it is big enough, use the top chunk.
       4. If request size >= mmap threshold, try to directly mmap this chunk.
       5. If available, get memory from system and use it
  */

  void* mem;
  size_t nb;
  if (bytes <= MAX_SMALL_REQUEST) {
    bindex_t idx;
    binmap_t smallbits;
    nb = (bytes < MIN_REQUEST)? MIN_CHUNK_SIZE : pad_request(bytes);
    idx = small_index(nb);
    smallbits = m_malloc_state.smallmap >> idx;

    if ((smallbits & 0x3U) != 0) { /* Remainderless fit to a smallbin. */
      mchunkptr b, p;
      idx += ~smallbits & 1;       /* Uses next bin if idx empty */
      b = smallbin_at(&m_malloc_state, idx);
      p = b->fd;
      GPOS_ASSERT(chunksize(p) == small_index2size(idx));
      unlink_first_small_chunk(&m_malloc_state, b, p, idx);
      set_inuse_and_pinuse(&m_malloc_state, p, small_index2size(idx));
      mem = chunk2mem(p);
      check_malloced_chunk(&m_malloc_state, mem, nb);
      return mem;
    }
    else if (nb > m_malloc_state.dvsize) {
      if (smallbits != 0) { /* Use chunk in next nonempty smallbin */
        mchunkptr b, p, r;
        size_t rsize;
        bindex_t i;
        binmap_t leftbits = (smallbits << idx) & left_bits(idx2bit(idx));
        binmap_t leastbit = least_bit(leftbits);
        compute_bit2idx(leastbit, i);
        b = smallbin_at(&m_malloc_state, i);
        p = b->fd;
        GPOS_ASSERT(chunksize(p) == small_index2size(i));
        unlink_first_small_chunk(&m_malloc_state, b, p, i);
        rsize = small_index2size(i) - nb;
        /* Fit here cannot be remainderless if 4byte sizes */
        if (SIZE_T_SIZE != 4 && rsize < MIN_CHUNK_SIZE)
          set_inuse_and_pinuse(&m_malloc_state, p, small_index2size(i));
        else {
          set_size_and_pinuse_of_inuse_chunk(&m_malloc_state, p, nb);
          r = chunk_plus_offset(p, nb);
          set_size_and_pinuse_of_free_chunk(r, rsize);
          replace_dv(m_malloc_state, r, rsize);
        }
        mem = chunk2mem(p);
        check_malloced_chunk(&m_malloc_state, mem, nb);
        return mem;
      }

      else if (m_malloc_state.treemap != 0 && (mem = tmalloc_small(&m_malloc_state, nb)) != 0) {
        check_malloced_chunk(&m_malloc_state, mem, nb);
        return mem;
      }
    }
  }
  else if (bytes >= MAX_REQUEST)
    nb = MAX_SIZE_T; /* Too big to allocate. Force failure (in sys alloc) */
  else {
    nb = pad_request(bytes);
    if (m_malloc_state.treemap != 0 && (mem = tmalloc_large(&m_malloc_state, nb)) != 0) {
      check_malloced_chunk(&m_malloc_state, mem, nb);
      return mem;
    }
  }

  if (nb <= m_malloc_state.dvsize) {
    size_t rsize = m_malloc_state.dvsize - nb;
    mchunkptr p = m_malloc_state.dv;
    if (rsize >= MIN_CHUNK_SIZE) { /* split dv */
      mchunkptr r = m_malloc_state.dv = chunk_plus_offset(p, nb);
      m_malloc_state.dvsize = rsize;
      set_size_and_pinuse_of_free_chunk(r, rsize);
      set_size_and_pinuse_of_inuse_chunk(&m_malloc_state, p, nb);
    }
    else { /* exhaust dv */
      size_t dvs = m_malloc_state.dvsize;
      m_malloc_state.dvsize = 0;
      m_malloc_state.dv = 0;
      set_inuse_and_pinuse(&m_malloc_state, p, dvs);
    }
    mem = chunk2mem(p);
    check_malloced_chunk(&m_malloc_state, mem, nb);
    return mem;
  }

  else if (nb < m_malloc_state.topsize) { /* Split top */
    size_t rsize = m_malloc_state.topsize -= nb;
    mchunkptr p = m_malloc_state.top;
    mchunkptr r = m_malloc_state.top = chunk_plus_offset(p, nb);
    r->head = rsize | PINUSE_BIT;
    set_size_and_pinuse_of_inuse_chunk(&m_malloc_state, p, nb);
    mem = chunk2mem(p);
    check_top_chunk(&m_malloc_state, m_malloc_state.top);
    check_malloced_chunk(&m_malloc_state, mem, nb);
    return mem;
  }

#ifdef GPOS_DEBUG
  do_check_malloc_state(&m_malloc_state);
#endif

  mem = sys_alloc(&m_malloc_state, nb);

  return mem;
}

void gpos::CMemoryPoolTracker::dlfree(void* mem) {
  /*
     Consolidate freed chunks with preceeding or succeeding bordering
     free chunks, if they exist, and then place in a bin.  Intermixed
     with special cases for top, dv, mmapped chunks, and usage errors.
  */

  if (mem != 0) {
    mchunkptr p  = mem2chunk(mem);
	malloc_state *mstate = get_mstate_for(p);
	GPOS_ASSERT(mstate->magic == MALLOC_STATE_MAGIC);
    check_inuse_chunk(mstate, p);
    if (RTCHECK(ok_address(mstate, p) && ok_cinuse(p))) {
      size_t psize = chunksize(p);
      mchunkptr next = chunk_plus_offset(p, psize);

#ifdef GPOS_DEBUG
      clib::Memset(mem, GPOS_MEM_FREED_PATTERN_CHAR, chunksize(p) - CHUNK_OVERHEAD);
#endif
      if (!pinuse(p)) {
        size_t prevsize = p->prev_foot;
        mchunkptr prev = chunk_minus_offset(p, prevsize);
        psize += prevsize;
        p = prev;
        if (RTCHECK(ok_address(mstate, prev))) { /* consolidate backward */
          if (p != mstate->dv) {
            unlink_chunk(mstate, p, prevsize);
          }
          else if ((next->head & INUSE_BITS) == INUSE_BITS) {
            mstate->dvsize = psize;
            set_free_with_pinuse(p, psize, next);
			return;
          }
        }
        else
			USAGE_ERROR_ACTION(&m_malloc_state, p);
      }

      if (RTCHECK(ok_next(p, next) && ok_pinuse(next))) {
        if (!cinuse(next)) {  /* consolidate forward */
          if (next == mstate->top) {
            size_t tsize = mstate->topsize += psize;
            mstate->top = p;
            p->head = tsize | PINUSE_BIT;
            if (p == mstate->dv) {
              mstate->dv = 0;
              mstate->dvsize = 0;
            }
			/*
            if (should_trim(fm, tsize))
              sys_trim(fm, 0);
			 */
          }
          else if (next == mstate->dv) {
            size_t dsize = mstate->dvsize += psize;
            mstate->dv = p;
            set_size_and_pinuse_of_free_chunk(p, dsize);
          }
          else {
            size_t nsize = chunksize(next);
            psize += nsize;
            unlink_chunk(mstate, next, nsize);
            set_size_and_pinuse_of_free_chunk(p, psize);
            if (p == mstate->dv) {
              mstate->dvsize = psize;
		      return;
            }
		    insert_chunk(mstate, p, psize);
		    check_free_chunk(mstate, p);
          }
        }
		else {
          set_free_with_pinuse(p, psize, next);
          insert_chunk(mstate, p, psize);
          check_free_chunk(mstate, p);
		}
      }
    }

  }
}

void* gpos::CMemoryPoolTracker::dlmalloc_array(size_t bytes, int32_t num_elems) {
	/* add an integer value to the end and store the number of elements in it */
	void *result = dlmalloc(bytes + sizeof(int32_t));
	int32_t *num_elems_in_mem = (int32_t *) (((char *) result)               /* start of memory after chunk header */
											 + chunksize(mem2chunk(result))  /* end of trailing chunk header */
											 - TWO_SIZE_T_SIZES              /* beginning of trailing chunk header */
											 - sizeof(int32_t));             /* the last four bytes of usable memory
																			    in the chunk */

	*num_elems_in_mem = num_elems;
	set_is_array_alloc(mem2chunk(result));

	return result;
}

int gpos::CMemoryPoolTracker::dlmalloc_num_array_elements(const void *mem) {
	mchunkptr p = mem2chunk(mem);

	if (is_array_alloc(p)) {
		int32_t *num_elems_in_mem = (int32_t *) (((char *) mem)               /* start of memory after chunk header */
												 + chunksize(mem2chunk(mem))  /* end of trailing chunk header */
												 - TWO_SIZE_T_SIZES           /* beginning of trailing chunk header */
												 - sizeof(int32_t));          /* the last four bytes of usable memory
																				 in the chunk */
		GPOS_ASSERT(0 < *num_elems_in_mem);
		return *num_elems_in_mem;
	}

	return -1;
}


size_t gpos::CMemoryPoolTracker::dlmalloc_footprint(void) {
  return m_malloc_state.footprint;
}

size_t gpos::CMemoryPoolTracker::dlmalloc_max_footprint(void) {
  return m_malloc_state.max_footprint;
}

void gpos::CMemoryPoolTracker::dlmalloc_delete_segments(bool check_free) {
  msegmentptr s = &(m_malloc_state.seg);
  while (s != 0) {
    /* deferred deletion, since s might be part of the segment to delete */
    msegmentptr next_segment = s->next;
    void *segment_memory = s->base;

    if (NULL != segment_memory &&
		(!check_free || unlink_segment(&m_malloc_state, s, &next_segment)))
	  {
		  /* We have a segment allocated and it is empty (if we checked).
		     Note that in some cases we may have needed to adjust next_segment.
		   */
		  (*(m_malloc_state.ll_dealloc_func))(this, segment_memory);
	  }
    s = next_segment;
  }

  if (!check_free) {
    /* we deleted all the segments, set segment pointer to NULL */
    m_malloc_state.seg.base = NULL;
    m_malloc_state.seg.size = 0;
    m_malloc_state.seg.next = NULL;
  }
}

