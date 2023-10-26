// Copyright Embedded Artistry LLC 2017
// Released under CC0 1.0 Universal License

#ifndef __TMALLOC_H_
#define __TMALLOC_H_

#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif //__cplusplus

	/**
	 * Initialize malloc with a memory address and pool size
	 */
	void tmalloc_addblock(void* addr, size_t size);

	/**
	 * Free-list malloc implementation
	 */
	void* tmalloc(size_t size, size_t alignment);

	/**
	 * Corresponding free-list free implementation
	 */
	void tfree(void* ptr);

#ifdef __cplusplus
}
#endif //__cplusplus

#endif //__TMALLOC_H_