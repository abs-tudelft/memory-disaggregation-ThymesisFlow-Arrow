// Copyright 2023 Philip Groet
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once


#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"
#include "arrow/table.h"
#include "arrow/memory_pool_internal.h"

#include "arrow/tf_orchestrate.grpc.pb.h"


namespace arrow {

// Per remote node we have one client
// The client sends rpc calls to remote nodes
class ARROW_EXPORT OrchestratorClient {
 public:
  OrchestratorClient(std::shared_ptr<grpc::Channel> gChannel);
  Status SendTableFull(std::shared_ptr<Table> &table, uint32_t start_index);
  Status SendRecordBatchMD   (std::shared_ptr<RecordBatch> rbatch, uint32_t start_index);
  Status SendRecordBatchMD_v1(std::shared_ptr<RecordBatch> rbatch, uint32_t start_index);
  Status SendState(uint32_t node_id, uint32_t flag);
  Result<uintptr_t *> Malloc(uint32_t size);
  Result<uintptr_t *> Reallocate(uint32_t old_size, uint32_t new_size, uint8_t* ptr);
  Status Free(uintptr_t* address, uint32_t size);
  Status FlushRecordBatch(std::shared_ptr<RecordBatch> rbatch);
  Status FlushRegion(void * pointer, uint64_t size);
  Status Ping();
  Status ClearChunks();
 private:
  std::unique_ptr<grpc_tfo::OrchestratorServer::Stub> stub_;
};


enum ARROW_EXPORT tf_device_type {
  LOCAL_MAP = 0,
  PCIE_MAP = 1,
  FILE_MAP = 2,
  SHMEM_FILE_MAP = 3,
  TF_MAP = 4
};

struct ARROW_EXPORT tf_device {
  uint32_t id;
  bool is_local; // When this machine owns this memory region. Can only be one local region
  const char * grpc_address; // If remote, we need to be able to communicate with it
  std::shared_ptr<OrchestratorClient> client;
  std::shared_ptr<MemoryPool> memory_pool;

  tf_device_type type;

  void* start;
  uint64_t length;

  std::string filename; // Used for file and pcie map

  uint32_t state_flag;
};

struct received_chunk {
  received_chunk(uint64_t start_index_, std::shared_ptr<Table> table_) : start_index(start_index_), table(table_) {}

  uint64_t start_index;

  std::shared_ptr<arrow::Table> table;
};



// Acts as client API, starts the OrchestratorServer
class ARROW_EXPORT Orchestrator {
 public:
  // Register "AddDevice" called by application
    // Tell allocator
  static Status AddDevice(std::shared_ptr<tf_device> dev);
  static Status AddDevicePCIe(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length, 
    std::string filename);
  static Status AddDeviceLocal(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length);
  static Status AddDeviceSharedMemoryFile(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length,
    std::string filename);
  static Status AddDeviceFile(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length,
    std::string filename);

  static Status MapDevice(uint32_t id);

  // Ping all regions
  static Status PingAllRegions();

  static void WaitAllDevicesOnline();

  // Passive listen for remote RPC, non-blocking starts threadpool internally
  static Status InitializeServer();
  static Status ShutdownServer();

  static Status FlushRemoteRegion(void * pointer, uint64_t size);
  static Status SendRecordBatchMD(std::shared_ptr<RecordBatch> &rbatch, uint32_t start_index);

  // Send a given table to all other nodes
  static Status SendTableFull(std::shared_ptr<Table> &table, uint32_t start_index);

  static void DebugRecordBatchPrintMetadata(std::shared_ptr<RecordBatch> rbatch);

  // Combine all receive chunks into a "huge table". Clear the list of received tables
  static Result<std::shared_ptr<Table>> GetHugeTable();


  // Wait for all nodes to be in this state, set own local state to this.
  static void SyncWait(uint32_t waitFlagMask, uint32_t waitFlagValues);

  // Send my flag to all nodes
  static Status SetFlag(uint32_t flag_, uint32_t mask);
  // Called by grpc server when we receive a flag from another node
  static Status SetFlagRemote(uint32_t node_id, uint32_t flag);

  static Status AddChunk(std::shared_ptr<arrow::Table> table, uint64_t start_index);
  static void ClearChunks();

  static void PrintConfig() {
    printf("Config:\n");
    for (std::shared_ptr<tf_device> dev: devices) {
      printf("id: %d\n\tstart address: %p\n\tlength: %u\n\ttype: %d\n",
      dev->id, dev->start, (unsigned int)dev->length, dev->type);
      if (local_device >= 0 && (uint32_t)local_device == dev->id) {
        printf("\tLOCAL\n");
      }
    }
  }

  static Result<std::shared_ptr<tf_device>> GetDevice(int node_id);

  //// Benchmarks
  static Status bm_serializer  (double * measurements, uint64_t NUM, std::shared_ptr<RecordBatch> rbatch);
  static Status bm_deserializer(double * measurements, uint64_t NUM, std::shared_ptr<RecordBatch> rbatch);
  static Status bm_malloc_local(double * measurements, uint64_t NUM);
  static Status bm_malloc_remote(double * measurements, uint64_t NUM, int node_id);
  static Status bm_flush_empty(double * measurements, uint64_t NUM, void* address, size_t size);
  static Status bm_flush_full(double * measurements, uint64_t NUM, void* address, size_t size);
  ////


  static int local_device;
  static std::mutex devices_lock;
  static std::vector<std::shared_ptr<tf_device>> devices;
 private:
  static uint32_t flag;
  static std::mutex received_chunks_lock;
  static std::vector<std::shared_ptr<received_chunk>> received_chunks;

  static std::unique_ptr<grpc::Server> grpc_server;


};



} // namespace arrow