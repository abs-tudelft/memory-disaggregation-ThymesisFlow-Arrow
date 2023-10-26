#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <vector>

#include "common.h"

#define SIZE 1024*1024*1024

arrow::Status RunMain(int argc, char** argv) {
	perf("");
	arrow::Orchestrator orc;

	// Not able to write to remote memory
	ARROW_RETURN_NOT_OK(orc.AddDeviceLocal(
		0, false, "172.20.26.87:6000", 
		(void*)0x900000000000, 31UL*1024*1024*1024));
	ARROW_RETURN_NOT_OK(orc.MapDevice(0));

	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		1, true, "0.0.0.0:6001", 
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

	////
	// Run strided access benchmark
	////
	ARROW_RETURN_NOT_OK(run_benchmark_strided(local_memory_pool.get()));


	// ARROW_RETURN_NOT_OK(waitFor(STATE_REMOTE_INIT_DONE));

	// orc.PrintConfig();

	// ARROW_RETURN_NOT_OK(waitFor(STATE_POST_INIT));

	// // Compute writes into local memory, and computes sum

	// ARROW_RETURN_NOT_OK(waitFor(STATE_FILL3_DONE));

	// std::shared_ptr<arrow::Table> table3;
	// ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());
	
	// perf(NULL);
	// ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {table3->column(0)}));
	// std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	// perf("Sum verify");
	// std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;

	// ARROW_RETURN_NOT_OK(waitFor(STATE_PRE_PROG_END));

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
