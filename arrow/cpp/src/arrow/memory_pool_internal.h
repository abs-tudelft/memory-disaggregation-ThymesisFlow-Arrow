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

#pragma once

#include <mutex>

#include "arrow/memory_pool.h"
#include "arrow/util/config.h"

#ifdef ARROW_THYMESISMALLOC
#include "arrow/tf_orchestrate.h"
#endif
// #define ARROW_THYMESISMALLOC

namespace arrow {

namespace memory_pool {

namespace internal {

static constexpr size_t kAlignment = 64;

static constexpr int64_t kDebugXorSuffix = -0x181fe80e0b464188LL;

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.  Note the correct value for DebugAllocator
// checks is hardcoded.
extern int64_t zero_size_area[1];
static uint8_t* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

#ifdef ARROW_JEMALLOC

// Helper class directing allocations to the jemalloc allocator.
class JemallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out);
  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr);
  static void DeallocateAligned(uint8_t* ptr, int64_t size);
  static void ReleaseUnused();
};

#endif  // defined(ARROW_JEMALLOC)

#ifdef ARROW_THYMESISMALLOC

// Helper class directing allocations to the thymesis allocator.
class ThymesismallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out);
  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr);
  static void DeallocateAligned(uint8_t* ptr, int64_t size);
  static void ReleaseUnused();

  // static Status read_config(const char* filename);
  // static void print_loaded_config();
  // static Status do_map();
 private:
  static std::mutex mutex_;
};



#endif  // defined(ARROW_THYMESISMALLOC)

}  // namespace internal

}  // namespace memory_pool

}  // namespace arrow
