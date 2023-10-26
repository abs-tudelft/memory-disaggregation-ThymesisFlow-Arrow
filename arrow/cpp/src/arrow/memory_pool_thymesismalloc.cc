// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/memory_pool_internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep
#include "arrow/status.h" // RETURN_NOT_OK

#include "arrow/tmalloc.h"

#include <iostream>
#ifdef __GLIBC__
#include <malloc.h>
#endif

// We can't put the jemalloc memory pool implementation into
// memory_pool.c because jemalloc.h may redefine malloc() and its
// family by macros. If malloc() and its family are redefined by
// jemalloc, our system memory pool will also use jemalloc's malloc() and
// its family.

namespace arrow {

namespace memory_pool {

namespace internal {

std::mutex ThymesismallocAllocator::mutex_;

Status ThymesismallocAllocator::AllocateAligned(int64_t size, uint8_t** out) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (size == 0) {
    *out = memory_pool::internal::kZeroSizeArea;
    return Status::OK();
  }

  // // NOTE: MODIFIED Arrow spec! Required as cache line size in power pc is 128. 
  // //   We need the allocations to be cache line aligned for cache flushing/invalidate 
  // //   of a cache line to only affect at most one arrow buffer
  // *out = reinterpret_cast<uint8_t *>(tmalloc(size, memory_pool::internal::kAlignment));
  *out = reinterpret_cast<uint8_t *>(tmalloc(size, 128));
  if (*out == NULL) {
    return Status::ExecutionError("tmalloc failed");
  }

  RETURN_NOT_OK(Orchestrator::FlushRemoteRegion(*out, size));

  std::cout << "AllocateAligned: " << std::hex << *(reinterpret_cast<void**>(out)) << " (" << std::dec << size << ")" << std::endl;
  return Status::OK();
}

Status ThymesismallocAllocator::ReallocateAligned(int64_t old_size, int64_t new_size,
                                            uint8_t** ptr) {
  // return Status::NotImplemented("ReallocateAligned");

  std::cout << "ReallocateAligned" << std::endl;

  uint8_t* previous_ptr = *ptr;
  if (previous_ptr == memory_pool::internal::kZeroSizeArea) {
    DCHECK_EQ(old_size, 0);
    return AllocateAligned(new_size, ptr);
  }
  if (new_size == 0) {
    DeallocateAligned(previous_ptr, old_size);
    *ptr = memory_pool::internal::kZeroSizeArea;
    return Status::OK();
  }
  // Note: We cannot use realloc() here as it doesn't guarantee alignment.

  // Allocate new chunk
  uint8_t* out = nullptr;
  RETURN_NOT_OK(AllocateAligned(new_size, &out));
  DCHECK(out);
  // Copy contents and release old memory chunk
  memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));

  tfree(*ptr);
  *ptr = out;
  return Status::OK();
}

void ThymesismallocAllocator::DeallocateAligned(uint8_t* ptr, int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::cout << "DeallocateAligned: " << std::hex << reinterpret_cast<void*>(ptr) << std::dec << std::endl;

  if (ptr == memory_pool::internal::kZeroSizeArea) {
    DCHECK_EQ(size, 0);
  } else {
    tfree(ptr);
  }
}

void ThymesismallocAllocator::ReleaseUnused() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::cout << "ReleaseUnused" << std::endl;
  // TODO

  #ifdef __GLIBC__
    // The return value of malloc_trim is not an error but to inform
    // you if memory was actually released or not, which we do not care about here
    ARROW_UNUSED(malloc_trim(0));
  #endif
}

}  // namespace internal

}  // namespace memory_pool


}  // namespace arrow
