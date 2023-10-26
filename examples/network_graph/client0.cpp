#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <vector>

#include "common.h"

#define SIZE (1024UL*1024UL*1024UL - 8UL)

arrow::Status RunMain(int argc, char** argv) {
	perf("");
	arrow::Orchestrator orc;
	double t;

	ARROW_RETURN_NOT_OK(orc.AddDeviceLocal(
		0, true, "0.0.0.0:6000", 
		(void*)0x900000000000, 31UL*1024*1024*1024));
	ARROW_RETURN_NOT_OK(orc.MapDevice(0));
	
	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		1, false, "172.20.26.88:6001", 
		(void*)0xB00000000000, 31UL*1024*1024*1024, std::string("/dev/mishmem-s1")));
	ARROW_RETURN_NOT_OK(orc.MapDevice(1));

	ARROW_RETURN_NOT_OK(orc.InitializeServer());
	orc.WaitAllDevicesOnline();
	ARROW_RETURN_NOT_OK(waitFor(STATE_LOCAL_INIT_DONE));


	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(orc.local_device));
	std::shared_ptr<arrow::MemoryPool> local_memory_pool = dev->memory_pool;

	// ////
	// // Run infection benchmark
	// ////
	// ARROW_RETURN_NOT_OK(run_benchmark_infections(local_memory_pool.get()));

	// ////
	// // Run strided access benchmark
	// ////
	// ARROW_RETURN_NOT_OK(run_benchmark_strided(local_memory_pool.get()));

	////
	// Run strided random benchmark
	////
	ARROW_RETURN_NOT_OK(run_benchmark_random(local_memory_pool.get()));


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
