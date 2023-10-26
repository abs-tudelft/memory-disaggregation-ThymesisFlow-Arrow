#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <unistd.h> // sleep
#include <iostream>
#include <vector>

#include "common.h"



arrow::Status RunMain(int argc, char** argv) {
	perf("");
	arrow::Orchestrator orc;

	ARROW_RETURN_NOT_OK(orc.AddDeviceLocal(
		0, true, "0.0.0.0:6000", 
		(void*)0x900000000000, 31UL*1024*1024*1024));
	
	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		1, false, "172.20.26.88:6001", 
		(void*)0xB00000000000, 31UL*1024*1024*1024, std::string("/dev/mishmem-s1")));
	ARROW_RETURN_NOT_OK(orc.MapDevice(1));

	ARROW_RETURN_NOT_OK(orc.InitializeServer());
	orc.WaitAllDevicesOnline();
	perf("WaitAllDevicesOnline");

	ARROW_RETURN_NOT_OK(waitFor(STATE_LOCAL_INIT_DONE));

	ARROW_RETURN_NOT_OK(orc.MapDevice(0));

	ARROW_RETURN_NOT_OK(waitFor(STATE_REMOTE_INIT_DONE));

	orc.PrintConfig();

	arrow::MemoryPool * local_memory_pool;
	arrow::MemoryPool * remote_memory_pool;
	ARROW_RETURN_NOT_OK(arrow::thymesismalloc_memory_pool(&local_memory_pool));
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(1));
	remote_memory_pool = dev->memory_pool.get();
	// Make a test recordbatch
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> local_record_batch, generateTestRecordBatch(local_memory_pool, 1024*1024*1024));

	ARROW_RETURN_NOT_OK(waitFor(STATE_POST_INIT));

	uint64_t NUM = 10;
	double measurements[NUM] = {0};


	// // Ping pong experiment
	// std::shared_ptr<arrow::OrchestratorClient> remote_client = dev->client;
	// for (uint64_t i = 0; i < NUM; i++) {
	// 	perf(NULL);
	// 	ARROW_RETURN_NOT_OK(remote_client->Ping());

	// 	measurements[i] = perf("Ping");
	// }
	// FloatArrayToCsv(measurements, NUM, 1, "ping.csv");

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONE));

	// std::cout << "\n\nbm_serializer" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_serializer(measurements, NUM, local_record_batch));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_serializer.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_deserializer" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_deserializer(measurements, NUM, local_record_batch));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_deserializer.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_malloc_local" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_malloc_local(measurements, NUM));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_malloc_local.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_malloc_remote" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_malloc_remote(measurements, NUM, 1));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_malloc_remote.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_flush_empty_local" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_flush_empty(measurements, NUM, (void*)0x900000000000, 1024*1024*1024));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_flush_empty_local.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_flush_full_local" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_flush_full(measurements, NUM, (void*)0x900000000000, 1024*1024*1024));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_flush_full_local.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_flush_empty" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_flush_empty(measurements, NUM, (void*)0xB00000000000, 1024*1024*1024));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_flush_empty_remote.csv");

	// usleep(1 * 1000*1000); 

	// std::cout << "\n\nbm_flush_full" << std::endl;
	// ARROW_RETURN_NOT_OK(orc.bm_flush_full(measurements, NUM, (void*)0xB00000000000, 1024*1024*1024));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_flush_full_remote.csv");


	
	usleep(1 * 1000*1000); 

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONEWAIT));

	// Other side is sending MD tables

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONE));

	// Other side is sending full tables

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONEWAIT));

	uint64_t SIZE = 500UL*1000UL*1000UL; // 500M elements. For uint64_t this means 4GB
	SIZE = SIZE / 128 * 128; // align down to 128
	if (((SIZE+2048UL) * 8UL) > (uint64_t)(0xFFFFFFFE)) {
		return arrow::Status::Invalid("\"SIZE\" too big to store in uint32_t (arrow requirement)");
	}

	std::cout << "\n\nbm_datatypes_local" << std::endl;
	ARROW_RETURN_NOT_OK(bm_datatypes(SIZE, local_memory_pool));

	usleep(1000*1000);

	// std::cout << "\n\nbm_datatypes_remote" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_datatypes(SIZE, remote_memory_pool));

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_serial_local" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_serial(measurements, NUM, SIZE, local_memory_pool));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_access_serial_local.csv");

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_serial_remote" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_serial(measurements, NUM, SIZE, remote_memory_pool));
	// FloatArrayToCsv(measurements, NUM, 1, "bm_access_serial_remote.csv");

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_strided_read_local" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_strided_read(NUM, SIZE, local_memory_pool));

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_strided_read_remote" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_strided_read(NUM, SIZE, remote_memory_pool));

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_strided_write_local" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_strided_write(NUM, SIZE, local_memory_pool));

	// usleep(1000*1000);

	// std::cout << "\n\nbm_access_strided_write_remote" << std::endl;
	// ARROW_RETURN_NOT_OK(bm_access_strided_write(NUM, SIZE, remote_memory_pool));

	// usleep(1000*1000);


	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL3_DONE));

	ARROW_RETURN_NOT_OK(waitFor(STATE_PRE_PROG_END));

	ARROW_RETURN_NOT_OK(waitFor(STATE_PROG_DONE));
	
	ARROW_RETURN_NOT_OK(orc.ShutdownServer());
	return arrow::Status::OK();
}

int main(int argc, char** argv) {
	arrow::Status status = RunMain(argc, argv);
	if (!status.ok()) {
		std::cerr << status.ToString() << std::endl;
		return EXIT_FAILURE;
	}

	printf("Gracefull success exit\n");
	return EXIT_SUCCESS;
}
