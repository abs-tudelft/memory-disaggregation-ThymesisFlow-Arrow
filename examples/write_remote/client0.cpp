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

	////
	// Remote malloc
	////
	const uint64_t SIZE = 1024UL*1024*1024 - 8; //1*1024*1024 * 100/110;
	double t;

	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(1));
	std::shared_ptr<arrow::MemoryPool> memory_pool = dev->memory_pool;

	perf(NULL);
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> years_buffer, AllocateBuffer(SIZE*8, memory_pool.get()));
	perf("Remote buffer allocate");
	uint64_t* years_buffer_data = (uint64_t*)years_buffer->mutable_data();
	perf(NULL);
	#pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < SIZE; i++) {
		years_buffer_data[i] = i;
	}
	t = perf("Writing years to remote RAW: uint64_t, parallel, simd, unroll partial(64)");
	std::cout << "Comes to throughput of: " << ((uint64_t)SIZE*8UL / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

	std::shared_ptr<arrow::ArrayData> years_ad = arrow::ArrayData::Make(arrow::uint64(), SIZE, {nullptr, years_buffer});
	std::shared_ptr<arrow::Array> remote_years = MakeArray(years_ad);

	std::shared_ptr<arrow::RecordBatch> rbatch2 = arrow::RecordBatch::Make(
		arrow::schema({
			arrow::field("years", arrow::uint64()),
		}),
		SIZE, 
		{remote_years});
	perf("Constructed rbatch");
	std::cout << rbatch2->ToString();
	orc.DebugRecordBatchPrintMetadata(rbatch2);
	perf(NULL);
	ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch2, 0));
	perf("SendRecordBatchMD rbatch2");

	std::shared_ptr<arrow::Table> table3;
	ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());

	perf(NULL);
	ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {table3->column(0)}));
	std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	perf("Sum verify");
	std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL3_DONE));

	// Other side computes sum

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
