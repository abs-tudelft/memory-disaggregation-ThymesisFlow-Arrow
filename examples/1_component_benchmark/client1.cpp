#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <vector>

#include <chrono>
using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;

#include "common.h"

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
	perf("WaitAllDevicesOnline");

	ARROW_RETURN_NOT_OK(waitFor(STATE_LOCAL_INIT_DONE));

	ARROW_RETURN_NOT_OK(waitFor(STATE_REMOTE_INIT_DONE));

	orc.PrintConfig();

	const uint64_t SIZE = 150*1024*1024;

	// Make a test recordbatch
	arrow::MemoryPool * local_memory_pool;
	ARROW_RETURN_NOT_OK(arrow::thymesismalloc_memory_pool(&local_memory_pool));
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> local_record_batch, generateTestRecordBatch(local_memory_pool, SIZE));
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> local_table, arrow::Table::FromRecordBatches({local_record_batch}));


	ARROW_RETURN_NOT_OK(waitFor(STATE_POST_INIT));

	// client0 is running ping pong test

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONE));

	// client 0 is running microbenchmarks

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONEWAIT));

	const int STEPS = 20;
	const int ITER_COUNT = 5; // How often to repeat measurements
	const int SLICE_SIZE = SIZE / STEPS;
	double md_measure[STEPS][3]; // i, rows, time
	double raw_measure[ITER_COUNT];

  TimePoint t_start, t_end;

	// unsigned long int sent_rows = 0;
	// for (int i = 0; i < STEPS; i++) {
	// 	for (int j = 0; j < ITER_COUNT; j++) {
	// 		std::shared_ptr<arrow::RecordBatch> rbatch_slice = local_record_batch->Slice(0, (i+1)*SLICE_SIZE);
			
	// 		t_start = Clock::now();
	// 		ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch_slice, sent_rows));
	// 		t_end = Clock::now();
	// 		sent_rows += (i+1)*SLICE_SIZE;

	// 		double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
	// 		raw_measure[j] = ms_double;
	// 	}

	// 	md_measure[i][0] = i;
	// 	md_measure[i][1] = (i+1)*SLICE_SIZE;
	// 	md_measure[i][2] = CalcArrayAvg(raw_measure, ITER_COUNT);
	// }
	// FloatArrayToCsv((double *)(&md_measure), STEPS, 3, "./bm_transfer_metadata.csv");

	// usleep(1 * 1000*1000); 

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONE));

	// ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(0));
	// sent_rows = 0;
	// for (int i = 0; i < STEPS; i++) {
	// 	for (int j = 0; j < ITER_COUNT; j++) {
	// 		std::shared_ptr<arrow::Table> table_slice = local_table->Slice(0, (i+1)*SLICE_SIZE);
	// 		t_start = Clock::now();
	// 		ARROW_RETURN_NOT_OK(orc.SendTableFull(table_slice, sent_rows));
	// 		t_end = Clock::now();
	// 		sent_rows += (i+1)*SLICE_SIZE;

	// 		double ms_double = std::chrono::duration<double, std::milli>(t_end - t_start).count();
	// 		raw_measure[j] = ms_double;

	// 		ARROW_RETURN_NOT_OK(dev->client->ClearChunks());
	// 	}
	// 	md_measure[i][0] = i;
	// 	md_measure[i][1] = (i+1)*SLICE_SIZE;
	// 	md_measure[i][2] = CalcArrayAvg(raw_measure, ITER_COUNT);
	// }
	// FloatArrayToCsv((double *)(&md_measure), STEPS, 3, "./bm_transfer_full.csv");

	usleep(1 * 1000*1000); 

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONEWAIT));

	// client0 running access pattern benchmarks

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
