#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <vector>

#include "common.h"

arrow::Status RunMain(int argc, char** argv) {
	perf("");
	arrow::Orchestrator orc;

	// ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
	// 	0, true, "0.0.0.0:6000", 
	// 	(void*)0x700000000000, 100*64*16384, std::string("./mapping0")));
	// ARROW_RETURN_NOT_OK(orc.InitializeServer());

	// ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
	// 	1, false, "127.0.0.1:6001", 
	// 	(void*)0x710000000000, 100*64*16384, std::string("./mapping1")));

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

	ARROW_RETURN_NOT_OK(waitFor(STATE_POST_INIT));

	// Fill fase
		// Do nothing

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONE));

	perf(NULL);
	// Combine all received tables, and print contents
	std::shared_ptr<arrow::Table> table;
	ARROW_ASSIGN_OR_RAISE(table, orc.GetHugeTable());
	perf("GetHugeTable lender (remote), write");
	std::cout << table->ToString();

	perf(NULL);

	ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {table->column(0)}));
	std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	ARROW_ASSIGN_OR_RAISE(arrow::Datum months_sum_datum, arrow::compute::CallFunction("sum", {table->column(1)}));
	std::shared_ptr<arrow::Scalar> months_sum = months_sum_datum.scalar();
	ARROW_ASSIGN_OR_RAISE(arrow::Datum years_sum_datum, arrow::compute::CallFunction("sum", {table->column(2)}));
	std::shared_ptr<arrow::Scalar> years_sum = years_sum_datum.scalar();
	perf("Sum verify: lender (remote) table");

	std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;
	std::cout << "sum_months=" << months_sum->ToString() << std::endl;
	std::cout << "sum_years=" << years_sum->ToString() << std::endl;
	

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONEWAIT));

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONE));

	// std::shared_ptr<arrow::Table> table2;
	// ARROW_ASSIGN_OR_RAISE(table2, orc.GetHugeTable());
	// ARROW_RETURN_NOT_OK(table2->ValidateFull());

	// perf(NULL);
	// std::cout << table2->ToString();
	// perf("print");


//  {// This is ugly and you should not do this in production code.
//    // In order to see the memory mappings of the currently
//      // running process, we use the pmap (for process-map) tool to
//      // query the kernel (/proc/self/maps)
//      char cmd[256];
//      snprintf(cmd, 256, "pmap %d", getpid());
//      printf("---- system(\"%s\"):\n", cmd);
//      system(cmd);
//    }

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONEWAIT));

	////
	// Remote malloc
	////
	const uint64_t SIZE = 1024*1024*1024; //1*1024*1024 * 100/110;

	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(1));
	std::shared_ptr<arrow::MemoryPool> memory_pool = dev->memory_pool;

	// Two ways to init data. 
		// 1) Copy over data already local to remote buffer
		// 2) Write data directly into a remote buffer

	// 1) Create builder which allocates using the remote allocator
	perf(NULL);
	std::unique_ptr<arrow::UInt16Builder> uint16builderRemote = std::make_unique<arrow::UInt16Builder>(memory_pool.get());
	perf("uint16builderRemote init");
	uint16_t* data_local_days = (uint16_t *)malloc(SIZE*2);
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		data_local_days[i] = i;
	}
	perf("Write into local buffer");
	ARROW_RETURN_NOT_OK(uint16builderRemote->AppendValues(data_local_days, SIZE));
	perf("Append into builder");
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> remote_days, uint16builderRemote->Finish());
	perf("Builder finish");

	// 2) Allocate a remote buffer and write into that
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> months_buffer, AllocateBuffer(SIZE*2, memory_pool.get()));
	perf("Remote buffer allocate");
	uint16_t* months_buffer_data = (uint16_t*)months_buffer->mutable_data();
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		months_buffer_data[i] = i;
	}
	double t = perf("Writing to remote RAW");
	std::cout << "Comes to throughput of: " << ((uint64_t)SIZE*2UL / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

	std::shared_ptr<arrow::ArrayData> months_ad = arrow::ArrayData::Make(arrow::uint16(), SIZE, {nullptr, months_buffer});
	std::shared_ptr<arrow::Array> remote_months = MakeArray(months_ad);

	perf(NULL);
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> years_buffer, AllocateBuffer(SIZE*8, memory_pool.get()));
	perf("Remote buffer allocate");
	int64_t* years_buffer_data = (int64_t*)years_buffer->mutable_data();
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		years_buffer_data[i] = i;
	}
	t = perf("Writing years to remote RAW");
	std::cout << "Comes to throughput of: " << ((uint64_t)SIZE*2UL / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

	std::shared_ptr<arrow::ArrayData> years_ad = arrow::ArrayData::Make(arrow::uint64(), SIZE, {nullptr, years_buffer});
	std::shared_ptr<arrow::Array> remote_years = MakeArray(years_ad);

	std::shared_ptr<arrow::RecordBatch> rbatch2 = arrow::RecordBatch::Make(
		arrow::schema({
			arrow::field("Odd_days", arrow::uint16()),
			arrow::field("Even_months", arrow::uint16()),
			arrow::field("years", arrow::uint16()),
		}),
		SIZE, 
		{remote_days, remote_months, remote_years});
	perf("Constructed rbatch");
	std::cout << rbatch2->ToString();
	orc.DebugRecordBatchPrintMetadata(rbatch2);
	perf(NULL);
	ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch2, 0));
	perf("SendRecordBatchMD rbatch2");

	std::shared_ptr<arrow::Table> table3;
	ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());

	perf(NULL);
	ARROW_ASSIGN_OR_RAISE(days_sum_datum, arrow::compute::CallFunction("sum", {table3->column(0)}));
	days_sum = days_sum_datum.scalar();
	perf("Sum verify: write from compute to memory: C1");
	ARROW_ASSIGN_OR_RAISE(months_sum_datum, arrow::compute::CallFunction("sum", {table3->column(1)}));
	months_sum = months_sum_datum.scalar();
	perf("Sum verify: write from compute to memory: C2");

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL3_DONE));

	// std::shared_ptr<arrow::Table> table3;
	// ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());
	// perf("GetHugeTable");
	// std::cout << table3->ToString();
	// perf("print");


	// {// This is ugly and you should not do this in production code.
  //   // In order to see the memory mappings of the currently
  //   // running process, we use the pmap (for process-map) tool to
  //   // query the kernel (/proc/self/maps)
  //   char cmd[256];
  //   snprintf(cmd, 256, "vmmap %d", getpid());
  //   printf("---- system(\"%s\"):\n", cmd);
  //   system(cmd);
  // }

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
