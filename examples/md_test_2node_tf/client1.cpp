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

	// ARROW_RETURN_NOT_OK(orc.MapDevice(0));

	ARROW_RETURN_NOT_OK(waitFor(STATE_REMOTE_INIT_DONE));

	orc.PrintConfig();

	ARROW_RETURN_NOT_OK(waitFor(STATE_POST_INIT));

	// // Fill fase
	// std::shared_ptr<arrow::Buffer> buffer;
	// ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateBuffer(1000));
	// int8_t* buffer_data = (int8_t*)buffer->mutable_data();
	// for (unsigned long i = 0; i < buffer->size(); i++) {
	// 	buffer_data[i] = (int8_t)(i*2 & 0xFF);
	// }
	// std::cout << "Making array..." << std::endl;
  // std::shared_ptr<arrow::Buffer> null_buffer;
	// ARROW_ASSIGN_OR_RAISE(null_buffer, arrow::AllocateBuffer((buffer->size() + 7) / 8)); // at least 1/8th of size
	// for (unsigned long i = 0; i < null_buffer->size(); i++) {
	// 	((int8_t*)buffer->mutable_data())[i] = 0xFF;
	// }
	// auto data = arrow::ArrayData::Make(arrow::int8(), buffer->size(), {nullptr, buffer});
	// std::shared_ptr<arrow::Array> days = MakeArray(data);

	perf(NULL);

	const uint64_t SIZE = 1024*1024*1024; //1*1024*1024 * 100/110;
	arrow::MemoryPool * local_memory_pool;
	ARROW_RETURN_NOT_OK(arrow::thymesismalloc_memory_pool(&local_memory_pool));

	arrow::Int64Builder int64builder(local_memory_pool);
  int64_t* days_raw = (int64_t *)malloc(SIZE*8);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		days_raw[i] = (int64_t)i;
	}
	perf("Days fill");
	ARROW_RETURN_NOT_OK(int64builder.AppendValues(days_raw, SIZE));
	perf("Builder append");
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> days, int64builder.Finish());
	perf("Builder finish");

	arrow::Int8Builder int8builder(local_memory_pool);
	int8_t* months_raw = (int8_t *)malloc(SIZE);
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		months_raw[i] = (int8_t)i;
	}
	perf("months fill");
	ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, SIZE));
	perf("Builder append");
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> months, int8builder.Finish());

	arrow::UInt16Builder uint16builder(local_memory_pool);
	uint16_t* years_raw = (uint16_t *)malloc(SIZE*2);
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		years_raw[i] = (uint16_t)i;
	}
	perf("Years fill");
	ARROW_RETURN_NOT_OK(uint16builder.AppendValues(years_raw, SIZE));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> years, uint16builder.Finish());

	perf("Builder finish");

  ARROW_RETURN_NOT_OK(days->Validate());
  ARROW_RETURN_NOT_OK(months->Validate());
  ARROW_RETURN_NOT_OK(years->Validate());
	perf("array validation");

	std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

	field_day = arrow::field("Day", arrow::int64());
	field_month = arrow::field("Month", arrow::int8());
	field_year = arrow::field("Year", arrow::uint16());
  // The schema can be built from a vector of fields, and we do so here.
  schema = arrow::schema({field_day, field_month, field_year});
	perf("Schema create");

	std::shared_ptr<arrow::RecordBatch> rbatch = arrow::RecordBatch::Make(schema, SIZE, {days, months, years});
	std::cout << rbatch->ToString();
	orc.DebugRecordBatchPrintMetadata(rbatch);
	perf("RecordBatch::Make");

	ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {days}));
	std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	ARROW_ASSIGN_OR_RAISE(arrow::Datum months_sum_datum, arrow::compute::CallFunction("sum", {months}));
	std::shared_ptr<arrow::Scalar> months_sum = months_sum_datum.scalar();
	ARROW_ASSIGN_OR_RAISE(arrow::Datum years_sum_datum, arrow::compute::CallFunction("sum", {years}));
	std::shared_ptr<arrow::Scalar> years_sum = years_sum_datum.scalar();
	perf("Sum verify: Local table");

	std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;
	std::cout << "sum_months=" << months_sum->ToString() << std::endl;
	std::cout << "sum_years=" << years_sum->ToString() << std::endl;

	perf(NULL);
	ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch, 0));
	perf("SendRecordBatchMD");

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONE));

	// Other side computes sum

	// Clear local hugetable record by retrieving it
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, orc.GetHugeTable());

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL1_DONEWAIT));

	// std::shared_ptr<arrow::Table> table2 = arrow::Table::Make(schema, {days, months, years}, SIZE);
	// perf(NULL);
	// ARROW_RETURN_NOT_OK(orc.SendTableFull(table2, 0));
	// perf("SendTableFull");

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONE));

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL2_DONEWAIT));

	// Compute writes into local memory, and computes sum

	ARROW_RETURN_NOT_OK(waitFor(STATE_FILL3_DONE));

	std::shared_ptr<arrow::Table> table3;
	ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());
	
	perf(NULL);
	ARROW_ASSIGN_OR_RAISE(days_sum_datum, arrow::compute::CallFunction("sum", {table3->column(0)}));
	days_sum = days_sum_datum.scalar();
	perf("Sum verify: write from compute to memory: C1");
	ARROW_ASSIGN_OR_RAISE(months_sum_datum, arrow::compute::CallFunction("sum", {table3->column(1)}));
	months_sum = months_sum_datum.scalar();
	perf("Sum verify: write from compute to memory: C2");

	ARROW_RETURN_NOT_OK(waitFor(STATE_PRE_PROG_END));

	// std::cout << "std::shared_ptr<arrow::Array> remote_array.use_count(): " << remote_array.use_count() << std::endl;
	// remote_array.reset();
	// rbatch2.reset(); // Should call destructor
	// std::cout << "std::shared_ptr<arrow::Array> remote_array.use_count(): " << remote_array.use_count() << std::endl;

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
