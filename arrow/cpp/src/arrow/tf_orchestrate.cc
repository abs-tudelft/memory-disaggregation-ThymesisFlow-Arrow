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


#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/array.h"

#include "arrow/memory_pool_internal.h"
#include "arrow/tmalloc.h"
#include "arrow/tf_serializer.h"
#include "arrow/tf_orchestrate.h"
#include "arrow/tperf.h"

#include <unistd.h> // sleep
#include <string>       // std::string
#include <iostream>     // std::cout
#include <sstream>      // std::stringstream, std::stringbuf
#include <stdlib.h> // rand

#include <fcntl.h> // open and flags
#include <unistd.h> // close
#include <sys/mman.h> // mmap
#include <sys/stat.h> // fstat
#include <unistd.h> // sysconf(_SC_PAGESIZE)

#include <chrono>
using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;

// #ifdef ARROW_THYMESISFLOW
//   #include "tf_shmem_api.h"
// #endif


#ifdef GRPCPP_GRPCPP_H
#error "gRPC headers should not be in public API"
#endif

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

// #include <grpcpp/ext/proto_server_reflection_plugin.h>
// #include <grpcpp/grpcpp.h>
// #include <grpcpp/health_check_service_interface.h>



namespace arrow {

// namespace tf_orchestrate {

// namespace internal {

#define ARROW_THYMESISFLOW
#ifdef ARROW_THYMESISFLOW

/* POWER9 cacheline size
 * TODO: make this more robust*/
#define CACHELINE_SIZE 0x80
#define CACHELINE_MASK (CACHELINE_SIZE - 1)

#define FLUSH_LINE(addr) \
	asm volatile (\
    "dcbf 0x0,%0\n"\
		:\
		: "b"(addr)\
		: "memory"\
	)


#define FLUSH_LINE_SYNC(addr) \
	asm volatile (\
    "dcbf 0x0,%0\n"\
		"sync\n"\
		:\
		: "b"(addr)\
		: "memory"\
	)

#define HWSYNC() \
  asm volatile (\
    "hwsync" \
  )

#define INVALIDATE_LINE(addr) \
	asm volatile (\
    "dcbi 0x0,%0\n"\
		:\
		: "b"(addr)\
		: "memory"\
	)
// Because comments not allowed in the multiline macro...
// asm volatile (
//   "dcbi 0x0,%0\n"
// 	: // if !=0: Offset RB
// 	: "b"(addr)
// 	: "memory" // "clobber argument": tells compiler this instr performs reads and writes on memory beside the argument
// )

// sync ensures all instructions before it have been completed
#define INVALIDATE_LINE_SYNC(addr) \
	asm volatile (\
    "dcbi 0x0,%0\n"\
		"sync\n"\
		:\
		: "b"(addr)\
		: "memory"\
	)

// From ThymesisFlow Github: https://github.com/OpenCAPI/ThymesisFlow/blob/f69f7ae0bdc245e4e969e488e118359dde9c8b40/libtfshmem/test/test_compute.c#L48
static inline void flush_memory_block (const void *addr, uint64_t size, int sync) {
	uint64_t start_addr = (uint64_t) addr;
	uint64_t end_addr = (start_addr + size);
	unsigned int count = 0;
  tperf(NULL);
  HWSYNC();

	if ((start_addr & CACHELINE_MASK) != 0) {
		if (sync)
			FLUSH_LINE_SYNC(start_addr);
		else {
			// printf("\n#%u flushing line @ address: %" PRIx64, count, start_addr);
			// fflush(stdout);
			FLUSH_LINE(start_addr);
		}
		start_addr += CACHELINE_SIZE - (start_addr & CACHELINE_MASK);
	}

	for ( ; start_addr < end_addr; start_addr += CACHELINE_SIZE, count++){
		if (sync)
			FLUSH_LINE_SYNC(start_addr);
		else {
			// printf("\n#%u flushing line @ address: %" PRIx64, count, start_addr);
			// fflush(stdout);
			FLUSH_LINE(start_addr);
		}
	}
  HWSYNC();

  tperf("flushing");
	printf("\tflush buffer @ address: %p of size %lu sync=%d\n", addr, size, (int8_t)sync);
}

// static inline void invalidate_memory_block (const void *addr, uint64_t size, int sync) {
// 	uint64_t start_addr = (uint64_t) addr;
// 	uint64_t end_addr = (start_addr + size);
// 	unsigned int count = 0;
// 	printf("\tinvalidate buffer @ address: %p of size %lu\n", addr, size);

// 	if ((start_addr & CACHELINE_MASK) != 0) {
// 		if (sync)
// 			INVALIDATE_LINE_SYNC(start_addr);
// 		else {
// //			printf("\n#%u flushing line @ address: %" PRIx64, count, start_addr);
// 			// fflush(stdout);
// 			INVALIDATE_LINE(start_addr);
// 		}
// 		start_addr += CACHELINE_SIZE - (start_addr & CACHELINE_MASK);
// 	}

// 	for ( ; start_addr < end_addr; start_addr += CACHELINE_SIZE, count++){
// 		if (sync)
// 			INVALIDATE_LINE_SYNC(start_addr);
// 		else {
// //			printf("\n#%u flushing line @ address: %" PRIx64, count, start_addr);
// 			// fflush(stdout);
// 			INVALIDATE_LINE(start_addr);
// 		}
// 	}
// }
#endif

std::shared_ptr<DataType> FixedTypeFromId(Type::type id) {
  // return std::make_shared<>();
  #define TYPECASE(type, TYPE) \
    case type: {return TYPE;} break;

  switch (id) {
    // These are all fixed width types
    TYPECASE(Type::UINT8, uint8())
    TYPECASE(Type::INT8, int8())
    TYPECASE(Type::UINT16, uint16())
    TYPECASE(Type::INT16, int16())
    TYPECASE(Type::UINT32, uint32())
    TYPECASE(Type::INT32, int32())
    TYPECASE(Type::UINT64, uint64())
    TYPECASE(Type::INT64, int64())
    TYPECASE(Type::HALF_FLOAT, float16())
    TYPECASE(Type::FLOAT, float32())
    TYPECASE(Type::DOUBLE, float64())
    TYPECASE(Type::FIXED_SIZE_BINARY, binary())
    TYPECASE(Type::DATE32, date32())
    TYPECASE(Type::DATE64, date64())
    // TYPECASE(TIMESTAMP, ()) // FIXME Derived types
    // TYPECASE(TIME32, ())
    // TYPECASE(TIME64, ())
    // TYPECASE(DECIMAL128, ())

    default: {
      std::cout << "Unknow type id: " << id << std::endl;
      return int8(); // Default to int8
    }
  }
}

std::shared_ptr<Schema> SchemaFromGrpc(const grpc_tfo::RecordBatchMD_v2* grbatch) {
    std::vector<std::shared_ptr<Field>> fields;

    for (int i = 0; i < grbatch->field_size(); i++) {
      const grpc_tfo::RecordBatchMD_v2::Field& gfield = grbatch->field(i);
      std::shared_ptr<Field> field = std::make_shared<Field>(
        gfield.name(), 
        FixedTypeFromId(static_cast<Type::type>(gfield.type())),
        (bool)(gfield.null_count()),
        nullptr
      );

      fields.push_back(field);
    }

    return schema(fields);
  }

class OrchestratorServerImpl final : public grpc_tfo::OrchestratorServer::Service {
  grpc::Status Malloc(grpc::ServerContext* context, const grpc_tfo::MallocRequest* request,
                             grpc_tfo::MallocReply* reply) override {
    std::cout << "Received remote malloc request for size=" << request->size() << std::endl;
                              
    MemoryPool* pool = default_memory_pool();
    uint8_t* address;
    Status status = pool->Allocate(request->size(), &address);
    if (!status.ok()) {
      reply->set_address(0);
      return grpc::Status::OK;
    }
    
    printf("\tallocated region %p, sending back\n", address);

    reply->set_address((uint64_t)address);
    return grpc::Status::OK;
  }

  grpc::Status Reallocate(grpc::ServerContext* context, const grpc_tfo::ReallocateRequest* request,
                             grpc_tfo::ReallocateReply* reply) override {
    MemoryPool* pool = default_memory_pool();
    uint8_t* address = (uint8_t *)request->address();
    Status status = pool->Reallocate(
      request->old_size(),
      request->new_size(),
      &address
    );
    if (!status.ok()) {
      reply->set_address(0);
      return grpc::Status::OK;
    }
    
    reply->set_address((uint64_t)address);
    return grpc::Status::OK;
  }

  grpc::Status Free(grpc::ServerContext* context, const grpc_tfo::FreeRequest* request,
                             grpc_tfo::FreeReply* reply) override {
    std::cout << "Received remote free" << std::endl;
    MemoryPool* pool = default_memory_pool();
    pool->Free((uint8_t *)request->address(), request->size());
    
    reply->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status FlushBlocks(grpc::ServerContext* context, const grpc_tfo::FlushRequest* request,
                             grpc_tfo::FlushReply* reply) override {
    std::cout << "Received remote flush block request" << std::endl;
    
    for (int i = 0; i < request->block_size(); i++) {
      flush_memory_block((const uint8_t *)request->block(i).pointer(), request->block(i).size(), false);
    }
    
    // TODO Sync?

    reply->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status Unlock(grpc::ServerContext* context, const grpc_tfo::UnlockRequest* request,
                      grpc_tfo::UnlockReply* reply) override {
    // Lock central table
    // unlock malloc'ed region
    // unlock central table
    reply->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status SendTable(grpc::ServerContext* context, const grpc_tfo::TableChunk* chunk,
                         grpc_tfo::TableChunkReply* reply) override {
    uint32_t start_index = chunk->start_index();
    std::string payload = chunk->ipc_table_payload();

    // We copy the payload into a separate buffer here with a lifecycle bound to shared_ptr
    //  Otherwise if this function goes out of scope the referenced memory in the table
    //  to the "payload" variable has been freed already while the table is still intact
	  auto buffer_res = AllocateBuffer(payload.size());
    if (!buffer_res.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
    std::shared_ptr<arrow::Buffer> buffer = std::move(buffer_res.ValueOrDie());
    memcpy((int8_t*)buffer->mutable_data(), payload.c_str(), payload.size());

    io::BufferReader arrowBufferReader{buffer};
    Result<std::shared_ptr<ipc::RecordBatchStreamReader>> streamReaderResult = ipc::RecordBatchStreamReader::Open(&arrowBufferReader);
    if (!streamReaderResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
    std::shared_ptr<ipc::RecordBatchStreamReader> streamReader = streamReaderResult.ValueOrDie();

    std::vector<std::shared_ptr<RecordBatch>> batches;
    while (true) {
      Result<std::shared_ptr<RecordBatch>> rBatchResult = streamReader->Next();
      if (!rBatchResult.ok()) {
        reply->set_success(false);
        return grpc::Status::OK;
      }
      std::shared_ptr<RecordBatch> rBatch = rBatchResult.ValueOrDie();

      if (!rBatch) break; // End of stream

      batches.push_back(rBatch);
    }

    Result<std::shared_ptr<Table>> tableResult = Table::FromRecordBatches(batches);
    if (!tableResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
    std::shared_ptr<Table> table = tableResult.ValueOrDie();

    Status addResult = Orchestrator::AddChunk(table, start_index);
    if (!addResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }

    std::cout << std::dec<< "Received table (payloadLength=" << payload.size() << ") with start_index " << start_index << " and rows=" << table->num_rows() << std::endl;
    reply->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status SendRecordBatchMD_v1(grpc::ServerContext* context, const grpc_tfo::RecordBatchMD_v1* grbatch,
                                 grpc_tfo::SendRecordBatchMDReply_v1* reply) override {
    std::vector<std::shared_ptr<ArrayData>> v;

    for (uint32_t i = 0; i < (uint32_t)grbatch->buffer_size(); i++) {
      const grpc_tfo::RecordBatchMD_v1::Buffer& gbuffer = grbatch->buffer(i);
      
      std::shared_ptr<Buffer> buffer = std::make_shared<Buffer>((uint8_t*)(gbuffer.pointer()), gbuffer.size(), gbuffer.capacity());
      // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, AllocateBuffer(gbuffer.get_capacity()));
      std::shared_ptr<ArrayData> data = ArrayData::Make(int8(), buffer->size(), {nullptr, buffer});
	    // std::shared_ptr<arrow::Array> arr = MakeArray(data);

      v.push_back(data);
    }

    // Ugly hardcoded prototype schema...
    auto field_day = field("Day", int8());
	  auto field_month = field("Month", int8());
    auto schema = arrow::schema({field_day, field_month});
    std::shared_ptr<RecordBatch> rbatch = RecordBatch::Make(schema, (int64_t)grbatch->num_rows(), v);

    Result<std::shared_ptr<Table>> tableResult = Table::FromRecordBatches({rbatch});
    if (!tableResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
    std::shared_ptr<Table> table = tableResult.ValueOrDie();
    Status addResult = Orchestrator::AddChunk(table, grbatch->start_index());
    if (!addResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }

    Orchestrator::DebugRecordBatchPrintMetadata(rbatch);

    reply->set_success(true);

    return grpc::Status::OK;
  }

  grpc::Status SendRecordBatchMD_v2(grpc::ServerContext* context, const grpc_tfo::RecordBatchMD_v2* grbatch,
                                 grpc_tfo::SendRecordBatchMDReply_v2* reply) override {
    std::shared_ptr<Schema> schema = SchemaFromGrpc(grbatch);

    std::vector<std::shared_ptr<Array>> columns;

    // Reconstruct fields
    for (int i = 0; i < grbatch->field_size(); i++) {
      grpc_tfo::RecordBatchMD_v2::Field field = grbatch->field(i);

      // Reconstruct all buffers
      std::vector<std::shared_ptr<Buffer>> buffers;
      for (int j = 0; j < field.buffer_size(); j++) {
        if (field.buffer(j).pointer() == 0) {
          buffers.push_back(nullptr);
          printf("\tbuffer=nullptr\n");
        } else {
          buffers.push_back(std::make_shared<Buffer>((const uint8_t *)field.buffer(j).pointer(), field.buffer(j).size()));
          printf("\tbuffer=%d pointer=%p size=%ld\n", j, (const uint8_t *)field.buffer(j).pointer(), field.buffer(j).size());

          // Inavlidate instruction has been made obsolete
          // #ifdef ARROW_THYMESISFLOW
          //   // sync not required as st to these regions are invalidated anyways
          //   invalidate_memory_block((const uint8_t *)field.buffer(j).pointer(), field.buffer(j).size(), false);
          // #endif
        }
      }

      // Reconstruct Array from buffers
      auto data = ArrayData::Make(schema->field(i)->type(), grbatch->rows(), buffers);
      std::shared_ptr<Array> array = MakeArray(data);
      Status arr_val = array->Validate();
      if (!arr_val.ok()) {
        std::cout << "Array invalid format!! " << arr_val.message() << std::endl;
        reply->set_success(false);
        return grpc::Status::OK;
      }

      columns.push_back(array);
    }

    // Combine schema and fields into a recordBatch
    std::shared_ptr<RecordBatch> rbatch = RecordBatch::Make(schema, grbatch->rows(), columns);

    Result<std::shared_ptr<Table>> tableResult = Table::FromRecordBatches({rbatch});
    if (!tableResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }
    std::shared_ptr<Table> table = tableResult.ValueOrDie();
    Status addResult = Orchestrator::AddChunk(table, grbatch->start_index());
    if (!addResult.ok()) {
      reply->set_success(false);
      return grpc::Status::OK;
    }

    Orchestrator::DebugRecordBatchPrintMetadata(rbatch);

    reply->set_success(true);

    return grpc::Status::OK;
  }

  grpc::Status SendState(grpc::ServerContext* context, const grpc_tfo::ApplicationState* request,
                         grpc_tfo::StateReply* reply) override {
    if (request->node_id() >= Orchestrator::devices.size()) {
      printf("RX: SendState: node_id() out of range");
      reply->set_success(false);
      return grpc::Status::OK;
    }

    printf("Received from node %d state=%08X\n", request->node_id(), request->flag());

    Status res = Orchestrator::SetFlagRemote(request->node_id(), request->flag());
    if (!res.ok()) {
      printf("RX: SendState: SetFlagRemote fail");
      reply->set_success(false);
      return grpc::Status::OK;
    }

    reply->set_success(true);
    return grpc::Status::OK;
  }

  grpc::Status ClearAllHugeTables(grpc::ServerContext* context, const grpc_tfo::PingMessage* request,
                    grpc_tfo::PingReply* reply) override {
    
    Orchestrator::ClearChunks();

    reply->set_success(1);
    return grpc::Status::OK;
  }

  grpc::Status Ping(grpc::ServerContext* context, const grpc_tfo::PingMessage* request,
                    grpc_tfo::PingReply* reply) override {
    reply->set_success(1);
    return grpc::Status::OK;
  }
};

// Global instance for server
OrchestratorServerImpl service;

OrchestratorClient::OrchestratorClient(std::shared_ptr<grpc::Channel> gChannel)
  : stub_(grpc_tfo::OrchestratorServer::NewStub(gChannel)) {}

Status OrchestratorClient::SendTableFull(std::shared_ptr<Table> &table, uint32_t start_index) {
  grpc_tfo::TableChunkReply reply;
  grpc::ClientContext context;
  grpc::Status gStatus;

  std::cout << "Sending table with " << table->num_rows() << " rows" << std::endl;

  ARROW_ASSIGN_OR_RAISE(auto output, io::BufferOutputStream::Create());

  ARROW_ASSIGN_OR_RAISE(auto batchWriter, ipc::MakeStreamWriter(output, table->schema()));
  ARROW_RETURN_NOT_OK(batchWriter->WriteTable(*table));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, output->Finish());

  grpc_tfo::TableChunk chunk;
  chunk.set_start_index(start_index);
  chunk.set_ipc_table_payload(std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size()));

  gStatus = stub_->SendTable(&context, chunk, &reply);
  if (!gStatus.ok()) {
    std::stringstream ss;
    ss << "SendTable grpc failed: " << gStatus.error_message();
    return Status::IOError(ss.str());
  }
  if (!reply.success()) {
    return Status::IOError("SendTable grpc failed: remote returned non success");
  }

  return Status::OK();
}

Status OrchestratorClient::SendRecordBatchMD(std::shared_ptr<RecordBatch> rbatch, uint32_t start_index) {
  grpc_tfo::RecordBatchMD_v2 grbatch;
  grpc_tfo::SendRecordBatchMDReply_v2 reply;
  grpc::ClientContext context;
  grpc::Status status;

  tf_orchestrate::RecordBatchDescription rbd;
  tf_orchestrate::RecordBatchAnalyzer rba(&rbd);
  ARROW_RETURN_NOT_OK(rba.Analyze(*rbatch));

  ARROW_RETURN_NOT_OK(rbd.ToProto(&grbatch));
  grbatch.set_start_index(start_index);

  #ifdef ARROW_THYMESISFLOW
    // Flush memory regions to ensure data is written to underlying memory
    for (auto field : rbd.fields) {
      for (auto buffer : field.buffers) {
        if (buffer.raw_buffer_ != nullptr) {
          flush_memory_block(buffer.raw_buffer_, buffer.size_, false);
        }
      }
    }
  #endif
  status = stub_->SendRecordBatchMD_v2(&context, grbatch, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "SendRecordBatchMD_v2: " << status.error_message();
    return Status::IOError(ss.str());
  } 

  return Status::OK();
}

// Warning, does not contain cache coherency flushing/invalidations
Status OrchestratorClient::SendRecordBatchMD_v1(std::shared_ptr<RecordBatch> rbatch, uint32_t start_index) {
  grpc_tfo::RecordBatchMD_v1 grbatch;
  grpc_tfo::SendRecordBatchMDReply_v1 reply;
  grpc::ClientContext context;
  grpc::Status status;

  std::shared_ptr<Schema> schema = rbatch->schema();
  for (int i = 0; i < schema->num_fields(); ++i) {
    std::shared_ptr<Field> field = schema->field(i);
    // field.name();
    std::shared_ptr<DataType> type = field->type();
    if (type != int8()) {
      return Status::Invalid("Only In8Type is supported");
    }
  }
  
  grbatch.set_start_index(start_index);
  grbatch.set_num_rows((uint32_t)rbatch->num_rows());
  for (std::shared_ptr<Array> array: rbatch->columns()) {
    std::shared_ptr<ArrayData> array_data = array->data();
    
    for (unsigned int i = 0; i < array_data->buffers.size(); i++) {
      if (array_data->buffers[i]) {
        grpc_tfo::RecordBatchMD_v1::Buffer* buffer = grbatch.add_buffer();
        buffer->set_size((uint32_t)array_data->buffers[i]->size());
        buffer->set_capacity((uint32_t)array_data->buffers[i]->capacity());
        buffer->set_pointer(array_data->buffers[i]->address());
      } else {
        // std::cout << "\t\tNULLPTR" << std::endl;
      }
    }
  }

  status = stub_->SendRecordBatchMD_v1(&context, grbatch, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "SendRecordBatchMD_v1: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return Status::OK();
}

Status OrchestratorClient::SendState(uint32_t node_id, uint32_t flag) {
  grpc_tfo::ApplicationState state;
  grpc_tfo::StateReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  state.set_node_id(node_id);
  state.set_flag(flag);

  status = stub_->SendState(&context, state, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "SendState: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return Status::OK();
}

Result<uintptr_t *> OrchestratorClient::Malloc(uint32_t size) {
  std::cout << "OrchestratorClient::Malloc size=" << size << std::endl;
  grpc_tfo::MallocRequest req;
  grpc_tfo::MallocReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  req.set_size(size);

  status = stub_->Malloc(&context, req, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "OrchestratorClient::Malloc: " << status.error_message();
    return Status::IOError(ss.str());
  }

  printf("\tpointer received=%p\n", (uintptr_t *)reply.address());
  return (uintptr_t *)reply.address();
}

Result<uintptr_t *> OrchestratorClient::Reallocate(uint32_t old_size, uint32_t new_size, uint8_t* ptr) {
  std::cout << "OrchestratorClient::Reallocate" << std::endl;

  // TODO Don't allow reallocate on non owned regions

  grpc_tfo::ReallocateRequest req;
  grpc_tfo::ReallocateReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  req.set_old_size(old_size);
  req.set_new_size(new_size);
  req.set_address((uint64_t)ptr);

  status = stub_->Reallocate(&context, req, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "OrchestratorClient::Reallocate: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return (uintptr_t *)reply.address();
}

Status OrchestratorClient::Free(uintptr_t* address, uint32_t size) {
  std::cout << "OrchestratorClient::Free" << std::endl;
  grpc_tfo::FreeRequest req;
  grpc_tfo::FreeReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  req.set_address((uint64_t)address);
  req.set_size(size);

  status = stub_->Free(&context, req, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "OrchestratorClient::Free: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return Status::OK();
}

Status OrchestratorClient::FlushRecordBatch(std::shared_ptr<RecordBatch> rbatch) {
  grpc_tfo::FlushRequest flush_request;
  grpc_tfo::FlushReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  for (std::shared_ptr<Array> array: rbatch->columns()) {
    std::shared_ptr<ArrayData> array_data = array->data();
    
    for (unsigned int i = 0; i < array_data->buffers.size(); i++) {
      if (array_data->buffers[i]) {
        grpc_tfo::FlushRequest::Block* block = flush_request.add_block();
        block->set_pointer(array_data->buffers[i]->address());
        block->set_size((uint32_t)array_data->buffers[i]->size());
      } else {
        // std::cout << "\t\tNULLPTR" << std::endl;
      }
    }
  }

  status = stub_->FlushBlocks(&context, flush_request, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "FlushRecordBatch: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return Status::OK();
}

Status OrchestratorClient::FlushRegion(void * pointer, uint64_t size) {
  grpc_tfo::FlushRequest flush_request;
  grpc_tfo::FlushReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  grpc_tfo::FlushRequest::Block* block = flush_request.add_block();
  block->set_pointer((uint64_t)pointer);
  block->set_size(size);

  status = stub_->FlushBlocks(&context, flush_request, &reply);
  if (!status.ok()) {
    std::stringstream ss;
    ss << "FlushRegion: " << status.error_message();
    return Status::IOError(ss.str());
  }

  return Status::OK();
}

Status OrchestratorClient::Ping() {
  grpc_tfo::PingMessage msg;
  grpc_tfo::PingReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  status = stub_->Ping(&context, msg, &reply);
  if (!status.ok()) {
    // std::cout << "Ping error: " << status.error_message() << std::endl;
    return Status::IOError("Ping error");
  }

  return Status::OK();
}

Status OrchestratorClient::ClearChunks() {
  grpc_tfo::PingMessage msg;
  grpc_tfo::PingReply reply;
  grpc::ClientContext context;
  grpc::Status status;

  status = stub_->ClearAllHugeTables(&context, msg, &reply);
  if (!status.ok()) {
    return Status::IOError("ClearChunks error");
  }

  return Status::OK();
}

int map_shmem_file(std::shared_ptr<tf_device> dev) {
  const char* fn = dev->filename.c_str();

  int fd;
  if (dev->is_local)
    shm_unlink(fn); // If previous still exists, remove it

  if ((uint64_t)dev->start % sysconf(_SC_PAGESIZE) != 0) {
    perror("start address not page aligned");
    return -1;
  }

  if ((uint64_t)dev->length % sysconf(_SC_PAGESIZE) != 0) {
    perror("length not page aligned");
    return -1;
  }

  // If local, we create the shared memory region if needed
  if (dev->is_local) {
    fd = shm_open(fn, 
      O_CREAT | O_RDWR,
      S_IRUSR | S_IWUSR);
    if (fd < 0) {
      perror("shm_open");
      return -1;
    }

    // Non documented error case in macos darwin kernel? 
    //  If ftruncate is called by non creating process we get EINVAL
    if (ftruncate(fd, dev->length) == -1) {
      perror("ftruncate");
      return -1;
    }
  } else {
    fd = shm_open(fn, 
      O_RDWR,
      0);
    if (fd < 0) {
      perror("shm_open");
      return -1;
    }
  }

  printf("fn=%s, fd=%d, dev->length=%lu\n", fn, fd, dev->length);

  void *map = mmap(/* addr */  dev->start,
                   /* length*/ dev->length,
                   /* prot */  PROT_READ | PROT_WRITE,
                   /* flags */ MAP_SHARED | MAP_FIXED,
                   /* file */  fd,
                   /* offset */ 0);
  if (map == MAP_FAILED) {
    perror("sh+mmap");
    return -1;
  }

  // close(fd);
  // shm_unlink(fn);

  return 0;
}

int map_file(std::shared_ptr<tf_device> dev) {
  const char* fn = dev->filename.c_str();

  // We first stat the file, before we open it with the O_CREAT
  // flag, whereby we can determine lateron, if the file has
  // existsted beforehand. 
  int fd = open(fn, O_RDWR, 0600);

  // We could not open/create the file. This is not good and we
  // cannot and do not want to recover from it.
  if (fd < 0) {
    perror("file non existant");
    return -1;
  }

  // // If stat failed, the file did not exist beforehand, and we want
  // // to initialize our backing storage. We also initialize the file,
  // // if its size is different from out persistent size.
  // if (rc == -1 || s.st_size != (long long)dev->length) {
  //     // First, we shrink/expand the file to the correct size. We
  //     // use ftruncate(2) for this
  //     if (ftruncate(fd, dev->length) != 0)
  //         return -1;

  //     // // We copy the contents of the initialized persistent section
  //     // // from the DRAM to the file, by writing it into the file descriptor
  //     // if (write(fd, &psec, dev->length) != dev->length)
  //     //     return -1;
  // }

  void *map = mmap(/* addr */  dev->start,
                  /* length*/ dev->length,
                  /* prot */  PROT_READ|PROT_WRITE,
                  /* flags */ MAP_SHARED|MAP_FIXED,
                  /* file */  fd,
                  /* offset */ 0);
  // mmap returns a special pointer (MAP_FAILED) to inidicate that
  // the operation did not succeed.
  if (map == MAP_FAILED) {
      perror("mmapp");
      return -1;
  }
  // This is interesting: We can close the file descriptor and our
  // mapping remains in place. Internally, the struct file is not
  // yet destroyed as the mapping keeps a reference to it.
  close(fd);

  return 0;
}

/**
 * @brief Maps Thymesis device into user space at specified address.
 * 
 * @return non-zero on failure
 */
int map_pcie(std::shared_ptr<tf_device> dev) {
  int status;

  // is_remote == false --> local --> Allocate local memory as lender, to be available to bhorrowers
  // In case of pcie test setup, bothg local and remote do a map of a pcie virt mem dev.

  int fd = open(dev->filename.c_str(), O_RDWR | O_SYNC);
  if (fd < 0) {
    printf("Open failed for file '%s': errno %d, %s\n",
      dev->filename.c_str(), errno, strerror(errno));
    return -1;
  }

  /* PCI memory size */
  struct stat statbuf;
  status = fstat(fd, &statbuf);
  if (status < 0) {
    printf("fstat() failed: errno %d, %s\n",
      errno, strerror(errno));
    return -1;
  }
  // if ((long unsigned int)statbuf.st_size != dev->length) {
  //   printf("Requested size does not match pcie data size (%lu != %lu)\n", dev->length, statbuf.st_size);
  //   return -1;
  // }

  /* Map */
  void* ret = (unsigned char *)mmap(
    dev->start,
    dev->length,
    PROT_READ | PROT_WRITE,
    MAP_SHARED | MAP_FIXED,
    fd,
    0);
  if (ret == (unsigned char *)MAP_FAILED) {
    printf("BARs that are I/O ports are not supported\n");
    close(fd);
    return -1;
  }

  close(fd);

  return 0;
}

int map_local(std::shared_ptr<tf_device> map) {
  if (map->length % sysconf(_SC_PAGESIZE) != 0) {
    printf("PageSize=%lu\n", sysconf(_SC_PAGESIZE));
    perror("length not page aligned");
    return -1;
  }
  if ((unsigned long long)map->start % sysconf(_SC_PAGESIZE) != 0) {
    perror("addr not page aligned");
    return -1;
  }

  /* Map */
  void* ret = (unsigned char *)mmap(
    map->start,
    map->length,
    PROT_READ | PROT_WRITE,
    MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS,
    -1, 0);
  if (ret == MAP_FAILED) {
    printf("errno=%d\n", errno);
    perror("mmap fail");
    return -1;
  }

  return 0;
}


// #ifdef ARROW_THYMESISFLOW
// int tf_init_local(void *address, size_t size) {
//   if (tf_memory_init_fixed(size, address) == -1){
// 		printf ("Error initializing shared memory pool");
// 		return -1;
// 	}

//   void* buf_ptr = tf_buffer_alloc(size);
// 	if (buf_ptr == NULL){
// 		printf ("Error allocating buffer");
// 		return -1;
// 	}

//   return 0;
// }

// int tf_init_remote(void *address, size_t size) {
//   if (tf_compute_init(address, size) == -1){
// 		printf ("Error initializing shared memory pool");
// 		return -1;
// 	}

//   void *buf_ptr = tf_buffer_map(address, size);
// 	if (buf_ptr == NULL){
// 		printf ("Error mapping buffer");
// 		return -1;
// 	}

//   return 0;
// }
// #endif

int map_tf(std::shared_ptr<tf_device> map) {
  
// #ifdef ARROW_THYMESISFLOW
//   if (map->length % sysconf(_SC_PAGESIZE) != 0) {
//     printf("PageSize=%lu\n", sysconf(_SC_PAGESIZE));
//     perror("length not page aligned");
//     return -1;
//   }
//   if ((unsigned long long)map->start % sysconf(_SC_PAGESIZE) != 0) {
//     perror("addr not page aligned");
//     return -1;
//   }

//   if (map->is_local) {
//     if (!tf_init_local(map->length, map->start)) {
//       return -1;
//     }
//   } else {
//     if (!tf_init_remote(map->length, map->start)) {
//       return -1;
//     }
//   }

//   return 0;

// #else
  printf("map_tf: Not compiled with ARROW_THYMESISFLOW\n");
  return -1;
// #endif

}


/*
 * Orchestrator (client API) class definitions 
 * 
 */

// Used to impede shared_ptr delete operator. This is an ugly bodge ;)
template< typename T >
struct fake_deleter
{
  void operator ()( T const * p)
  { 
    ;;
  }
};

int Orchestrator::local_device = -1;

std::mutex Orchestrator::devices_lock;
std::vector<std::shared_ptr<tf_device>> Orchestrator::devices;
std::vector<std::shared_ptr<received_chunk>> Orchestrator::received_chunks;
std::unique_ptr<grpc::Server> Orchestrator::grpc_server;
std::mutex Orchestrator::received_chunks_lock;
uint32_t Orchestrator::flag = 0x00000000;

Status Orchestrator::AddDevice(std::shared_ptr<tf_device> dev) {
  // Check conflicts
  for (std::shared_ptr<tf_device> dev_: devices) {
    // Check if id already taken
    if (dev->id == dev_->id) {
      return Status::AlreadyExists("dev id already taken");
    }

    // Check if requested dev *start* lies within this dev
    if ((unsigned long)dev_->start <= (unsigned long)dev->start && (unsigned long)dev->start < ((unsigned long)dev_->start + dev_->length)) {
      return Status::AlreadyExists("Region overlaps with another dev (id=", dev_->id, ")");
    }

    // Check if requested dev *end* lies within this dev
    if ((unsigned long)dev_->start <= ((unsigned long)dev->start + dev->length) && ((unsigned long)dev->start + dev->length) < ((unsigned long)dev_->start + dev_->length)) {
      return Status::AlreadyExists("Region overlaps with another dev (id=", dev_->id, ")");
    }

    if (dev->is_local && dev_->is_local) {
      return Status::AlreadyExists("There is already a locally mapped dev (id=", dev_->id, ")");
    }
  }

  if (dev->is_local) {
    // Set this map as the one to be used for malloc
    local_device = (int)devices.size();
    MemoryPool* pool;
    RETURN_NOT_OK(thymesismalloc_memory_pool(&pool));
    // this is a huge bodge. We disable the delete operator when this pointer goes out of scope...
    //  To give some context why the tf_device::memory_pool is a shared_ptr instead of a normal ptr. 
    //  MemoryPools are normally statically initialized in the GlobalState struct by Arrow, the
    //  ThymesismallocRemoteMemoryPool, is not. It is instantiated ber client. As we do not know how
    //  many devices are in the cluster we cannot statically instantiate these.
    dev->memory_pool = std::shared_ptr<MemoryPool>(pool, fake_deleter<MemoryPool>());
  } else {
    std::shared_ptr<OrchestratorClient> client = std::make_shared<OrchestratorClient>(grpc::CreateChannel(dev->grpc_address, grpc::InsecureChannelCredentials()));
    dev->client = client;
    std::shared_ptr<MemoryPool> memory_pool = std::make_shared<ThymesismallocRemoteMemoryPool>(client, dev->id);
    dev->memory_pool = memory_pool;
  }

  devices.push_back(dev);

  return Status::OK();
}

Status Orchestrator::MapDevice(uint32_t id) {
  ARROW_ASSIGN_OR_RAISE(auto dev, GetDevice(id));

  int ret;
  switch (dev->type) {
    case LOCAL_MAP:
      ret = map_local(dev);
      break;
    case PCIE_MAP:
      ret = map_pcie(dev);
      break;
    case FILE_MAP:
      ret = map_file(dev);
      break;
    case SHMEM_FILE_MAP:
      ret = map_shmem_file(dev);
      break;
    case TF_MAP:
      ret = map_tf(dev);
      break;

    default:
      return Status::Invalid("Unknown device type");
      break;
  }

  if (ret) {
    return Status::IOError("map failed");
  }

  if (dev->is_local) {
    // Init malloc structure into mapped region
    tmalloc_addblock(dev->start, dev->length);
  }
  return Status::OK();
}

Status Orchestrator::AddDevicePCIe(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length, 
    std::string filename
) {
  // Add to regions member
  std::shared_ptr<tf_device> dev = std::make_shared<tf_device>();
  dev->id = id;
  dev->is_local = is_local;
  dev->grpc_address = grpc_address;
  dev->type = tf_device_type::PCIE_MAP;
  dev->start = start;
  dev->length = length;
  dev->filename = filename;
  
  RETURN_NOT_OK(Orchestrator::AddDevice(dev));

  return Status::OK();
}

Status Orchestrator::AddDeviceSharedMemoryFile(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length, 
    std::string filename
) {
  // Add to regions member
  std::shared_ptr<tf_device> dev = std::make_shared<tf_device>();
  dev->id = id;
  dev->is_local = is_local;
  dev->grpc_address = grpc_address;
  dev->type = tf_device_type::SHMEM_FILE_MAP;
  dev->start = start;
  dev->length = length;
  dev->filename = filename;
  
  RETURN_NOT_OK(Orchestrator::AddDevice(dev));

  return Status::OK();
}

Status Orchestrator::AddDeviceFile(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length, 
    std::string filename
) {
  // Add to regions member
  std::shared_ptr<tf_device> dev = std::make_shared<tf_device>();
  dev->id = id;
  dev->is_local = is_local;
  dev->grpc_address = grpc_address;
  dev->type = tf_device_type::FILE_MAP;
  dev->start = start;
  dev->length = length;
  dev->filename = filename;
  
  RETURN_NOT_OK(Orchestrator::AddDevice(dev));

  return Status::OK();
}

// Used purely for testing purposes. Adds a local malloc region
Status Orchestrator::AddDeviceLocal(
    uint32_t id, bool is_local, const char* grpc_address, void* start, uint64_t length
) {
  // Add to regions member
  std::shared_ptr<tf_device> dev = std::make_shared<tf_device>();
  dev->id = id;
  dev->is_local = is_local;
  dev->grpc_address = grpc_address;
  dev->type = tf_device_type::LOCAL_MAP;
  dev->start = start;
  dev->length = length;
  
  RETURN_NOT_OK(Orchestrator::AddDevice(dev));

  return Status::OK();
}

Status Orchestrator::PingAllRegions() {
  return Status::NotImplemented("PingAllRegions");

}

void Orchestrator::WaitAllDevicesOnline() {
  Status status;
  for (std::shared_ptr<tf_device> dev: devices) {
    if (dev->is_local) continue;
    std::cout << "Waiting for device " << dev->id << " to come online ." << std::flush;
    while (1) {
      status = dev->client->Ping();
      if (status.ok()) break;

      usleep(500*1000);
      std::cout << "." << std::flush;
    }
    std::cout << "ONLINE" << std::endl;
  }
}

Status Orchestrator::InitializeServer(){
  if (local_device < 0) {
    return Status::Invalid("No device configured as local");
  }

  std::string server_address(devices[local_device]->grpc_address);

  grpc::ServerBuilder builder;

  grpc::EnableDefaultHealthCheckService(true);

  builder.SetMaxReceiveMessageSize(-1);
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  // std::unique_ptr<Server> server(builder.BuildAndStart());
  grpc_server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;

  return Status::OK();
}
Status Orchestrator::ShutdownServer(){
  grpc_server->Shutdown();

  return Status::OK();
}

Status Orchestrator::FlushRemoteRegion(void * pointer, uint64_t size) {
  printf("Communicating remote flush of region %p, of size %lu\n", pointer, size);
  for (std::shared_ptr<tf_device> dev: devices) {
    if (dev->is_local) continue; 

    Status reply = dev->client->FlushRegion(pointer, size);
    if (!reply.ok()) {
      printf("Error from device %d\n", dev->id);
      return reply;
    }
  }

  return Status::OK();
}

Status Orchestrator::SendRecordBatchMD(std::shared_ptr<RecordBatch> &rbatch, uint32_t start_index) {
  // Add to local list
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table, Table::FromRecordBatches({rbatch}));
  RETURN_NOT_OK(AddChunk(table, start_index));

  // And add to all remote lists
  for (std::shared_ptr<tf_device> dev: devices) {
    if (dev->is_local) continue; 

    Status reply = dev->client->SendRecordBatchMD(rbatch, start_index);
    if (!reply.ok()) {
      printf("Error from device %d\n", dev->id);
      return reply;
    }
  }

  return Status::OK();
}

Status Orchestrator::SendTableFull(std::shared_ptr<Table> &table, uint32_t start_index) {
  // Add to local list
  RETURN_NOT_OK(AddChunk(table, start_index));

  // And add to all remote lists
  for (std::shared_ptr<tf_device> dev: devices) {
    if (dev->is_local) continue; 

    Status reply = dev->client->SendTableFull(table, start_index);
    if (!reply.ok()) {
      printf("Error from device %d\n", dev->id);
      return reply;
    }
  }

  return Status::OK();
}


void Orchestrator::DebugRecordBatchPrintMetadata(std::shared_ptr<RecordBatch> rbatch) {
  std::cout << std::dec << "RecordBatch:\n" << "\tSize: " << rbatch->num_rows() << std::endl;
  for (std::shared_ptr<Array> array: rbatch->columns()) {
    std::cout << "\tArrayData length=" << array->length() << " offset=" << array->offset();
    std::shared_ptr<ArrayData> array_data = array->data();
    std::cout << " data=" << std::hex << array_data << std::dec << " buffers=" << array_data->buffers.size() << std::endl;
    
    for (unsigned int i = 0; i < array_data->buffers.size(); i++) {
      std::cout << "\t\ti=" << i << std::endl;
      if (array_data->buffers[i]) {
        std::cout << "\t\t\tsize=" << array_data->buffers[i]->size() << std::endl;
        std::cout << "\t\t\tcapacity=" << array_data->buffers[i]->capacity() << std::endl;
        std::cout << "\t\t\tBuffer p=0x" << std::hex << array_data->buffers[i]->address() << std::dec << std::endl;
      } else {
        std::cout << "\t\t\tNULLPTR" << std::endl;
      }
    }
  }
}

bool compareReceivedChunks(std::shared_ptr<received_chunk> chunk1, std::shared_ptr<received_chunk> chunk2) {
  return ((*chunk1).start_index < (*chunk2).start_index);
}

Result<std::shared_ptr<Table>> Orchestrator::GetHugeTable(){
  const std::lock_guard<std::mutex> lock(received_chunks_lock);
  // received_chunks_lock.lock();
  std::cout << "GetHugeTable" << std::endl;

  if (received_chunks.size() == 0) {
    return Status::Invalid("Empty table");
  }

  // In place sort chunk array
  sort(received_chunks.begin(), received_chunks.end(), compareReceivedChunks);

  // Check if contiguous
  uint64_t last_end_index = (received_chunks[0])->start_index;
  for (auto c : received_chunks) {
    if (c->start_index != last_end_index) {
      std::cout << "c->start_index != last_end_index where" << c->start_index << " " << last_end_index << std::endl;
      return Status::Invalid("Non-contiguous table list");
    }

    last_end_index += c->table->num_rows();
    // last_end_index += (*(*c).table).num_rows();
  }

  std::vector<std::shared_ptr<Table>> tables;
  for (auto c : received_chunks) {
    tables.push_back(c->table);
  }

  arrow::Result<std::shared_ptr<Table>> tableResult = ConcatenateTables(tables);
  if (!tableResult.ok()) {
    return Status::Invalid("GetTable failed: ConcatenateTables");
  }
  std::shared_ptr<Table> table = tableResult.ValueOrDie();

  received_chunks.clear();
  // received_chunks_lock.unlock();

  return table;
}

unsigned int a = 0;
void Orchestrator::SyncWait(uint32_t waitFlagMask, uint32_t waitFlagValues) {
  std::cout << "SynWait: M=0x" << std::hex << waitFlagMask << " V=" << waitFlagValues << std::dec << std::endl;
  for (std::shared_ptr<tf_device> dev: devices) {
    // TODO: Thread safety?

    if (dev->is_local) continue;

    uint32_t shiftedMask = waitFlagMask;
    uint32_t shiftedWaitValues = waitFlagValues;
    // For every position, if the values is masked, wait for it to become the correct value
    for (uint8_t i = 0; i < 32; i++) {
      // if masked
      if (shiftedMask & 0x1) {
        while (true) {
          if ((a++ % (5 * 2*1000)) == 0) {
            std::cout << "\tWaiting for dev " << dev->id << " on flag pos=" << (int)i << " to become " << (shiftedWaitValues & 0x1) << std::endl;
          }

          if (((dev->state_flag >> i) & 0x1) == (shiftedWaitValues & 0x1)) {
            break;
          }

          usleep(500);
        }
      }

      shiftedMask >>= 1;
      shiftedWaitValues >>= 1;
    }
  }
}

Status Orchestrator::SetFlag(uint32_t flag_, uint32_t mask) {
  std::shared_ptr<tf_device> dev;

  // Keep non masked bits, dicard rest
  flag &= ~mask;
  // Set new masked flag bits
  flag |= (flag_ & mask);

  for (unsigned int i = 0; i < devices.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(dev, GetDevice(i))
    if (dev->is_local) continue;

    Status reply = dev->client->SendState(local_device, flag);
    if (!reply.ok()) {
      printf("Error from device %d\n", dev->id);
      return reply;
    }
  }

  return Status::OK();
}


Status Orchestrator::SetFlagRemote(uint32_t node_id, uint32_t flag) {
  const std::lock_guard<std::mutex> lock(devices_lock);
  // devices_lock.lock();

  std::shared_ptr<tf_device> dev;
  ARROW_ASSIGN_OR_RAISE(dev, GetDevice(node_id));

  dev->state_flag = flag;

  // devices_lock.unlock();

  return Status::OK();
}

/**
 * @brief Add chunk to the chunk list. Thread safe.
 * 
 * @param table Table to add to the list
 * @param start_index Where the index belongs in the final array. 
 */
Status Orchestrator::AddChunk(std::shared_ptr<Table> table, uint64_t start_index) {
  const std::lock_guard<std::mutex> lock(received_chunks_lock);

  std::cout << "AddChunk: " << table->num_rows() << " rows, and start_index=" << start_index << std::endl;
  std::shared_ptr<received_chunk> chunk = std::make_shared<received_chunk>(start_index, table);
  // (*chunk).start_index = start_index;
  // (*chunk).table = table;
  std::shared_ptr<Table> table_ = chunk->table;

  // received_chunks_lock.lock();
  received_chunks.push_back(std::move(chunk));

  // received_chunks_lock.unlock();

  return Status::OK();
}

void Orchestrator::ClearChunks() {
  const std::lock_guard<std::mutex> lock(received_chunks_lock);
  received_chunks.clear();
}

Result<std::shared_ptr<tf_device>> Orchestrator::GetDevice(int node_id) {
  if (node_id < 0) return Status::Invalid("GetDevice invalid node_id");

  for (std::shared_ptr<tf_device> dev_: devices) {
    if (dev_->id == (uint32_t)node_id) {
      return dev_;
    }
  }

  return Status::Invalid("GetDevice: node_id not a device", node_id);
}



Status Orchestrator::bm_serializer(double * measurements, uint64_t NUM, std::shared_ptr<RecordBatch> rbatch) {
  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    t_start = Clock::now();

    tf_orchestrate::RecordBatchDescription rbd;
    tf_orchestrate::RecordBatchAnalyzer rba(&rbd);
    grpc_tfo::RecordBatchMD_v2 grbatch;

    ARROW_RETURN_NOT_OK(rba.Analyze(*rbatch));

    ARROW_RETURN_NOT_OK(rbd.ToProto(&grbatch));

    t_end = Clock::now();
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();
}

Status Orchestrator::bm_deserializer(double * measurements, uint64_t NUM, std::shared_ptr<RecordBatch> rbatch) {
  tf_orchestrate::RecordBatchDescription rbd;
  tf_orchestrate::RecordBatchAnalyzer rba(&rbd);
  grpc_tfo::RecordBatchMD_v2 grbatch;
  ARROW_RETURN_NOT_OK(rba.Analyze(*rbatch));
  ARROW_RETURN_NOT_OK(rbd.ToProto(&grbatch));

  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    t_start = Clock::now();

    std::shared_ptr<Schema> schema = SchemaFromGrpc(&grbatch);
    std::vector<std::shared_ptr<Array>> columns;
    // Reconstruct fields
    for (int i = 0; i < grbatch.field_size(); i++) {
      grpc_tfo::RecordBatchMD_v2::Field field = grbatch.field(i);

      // Reconstruct all buffers
      std::vector<std::shared_ptr<Buffer>> buffers;
      for (int j = 0; j < field.buffer_size(); j++) {
        if (field.buffer(j).pointer() == 0) {
          buffers.push_back(nullptr);
        } else {
          buffers.push_back(std::make_shared<Buffer>((const uint8_t *)field.buffer(j).pointer(), field.buffer(j).size()));
        }
      }

      // Reconstruct Array from buffers
      auto data = ArrayData::Make(schema->field(i)->type(), grbatch.rows(), buffers);
      std::shared_ptr<Array> array = MakeArray(data);
      Status arr_val = array->Validate();
      if (!arr_val.ok()) {
        std::cout << "Array invalid format!! " << arr_val.message() << std::endl;
        return Status::Invalid("Array invalid format!!");
      }

      columns.push_back(array);
    }

    // Combine schema and fields into a recordBatch
    std::shared_ptr<RecordBatch> rbatch = RecordBatch::Make(schema, grbatch.rows(), columns);

    Result<std::shared_ptr<Table>> tableResult = Table::FromRecordBatches({rbatch});
    if (!tableResult.ok()) {
      return Status::Invalid("Array invalid format!!");
    }
    std::shared_ptr<Table> table = tableResult.ValueOrDie();
    Status addResult = Orchestrator::AddChunk(table, grbatch.start_index());
    if (!addResult.ok()) {
      return Status::OK();
    }

    t_end = Clock::now();
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();
}

Status Orchestrator::bm_malloc_local(double * measurements, uint64_t NUM) {
  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    uint64_t size = rand() % 5000;
    MemoryPool* pool = default_memory_pool();
    uint8_t* address;

    t_start = Clock::now();

    ARROW_RETURN_NOT_OK(pool->Allocate(size, &address));

    t_end = Clock::now();
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();

}

Status Orchestrator::bm_malloc_remote(double * measurements, uint64_t NUM, int node_id) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<tf_device> dev, GetDevice(node_id));

  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    uint32_t size = rand() % 5000;
    t_start = Clock::now();

    ARROW_ASSIGN_OR_RAISE(auto addr, dev->client->Malloc(size));

    t_end = Clock::now();
    addr++; // Use it so no compiler warning
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();
}


Status Orchestrator::bm_flush_empty(double * measurements, uint64_t NUM, void* address, size_t size) {
  // Initial wipe
  flush_memory_block (address, size, 0);
  
  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    t_start = Clock::now();

    flush_memory_block (address, size, 0);

    t_end = Clock::now();
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();
}

Status Orchestrator::bm_flush_full(double * measurements, uint64_t NUM, void* address, size_t size) {
  TimePoint t_start, t_end;
  for (uint64_t i = 0; i < NUM; i++) {
    // Touch all addresses so that they may be cached
    for (size_t j = 0; j < size; j++) {
      ((uint8_t *)address)[j] = (uint8_t)j;
    }

    t_start = Clock::now();

    flush_memory_block (address, size, 0);

    t_end = Clock::now();
    double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    measurements[i] = ms_double;
  }

  return Status::OK();
}



// } // namespace internal

// } // namespace tf_orchestrate

} // namespace arrow