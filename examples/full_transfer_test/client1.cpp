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
		(void*)0x300000000000, 1000*64*16384));
	ARROW_RETURN_NOT_OK(orc.MapDevice(0));

	// ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
	// 	1, true, "0.0.0.0:6001", 
	// 	(void*)0x710000000000, 100*64*16384, std::string("/dev/mishmem-s1")));
	ARROW_RETURN_NOT_OK(orc.AddDeviceLocal(
		1, true, "0.0.0.0:6001", 
		(void*)0x310000000000, 1000*64*16384));
	ARROW_RETURN_NOT_OK(orc.MapDevice(1));

	ARROW_RETURN_NOT_OK(orc.InitializeServer());
	orc.WaitAllDevicesOnline();
	perf("WaitAllDevicesOnline");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_LOCAL_INIT_DONE, STATE_LOCAL_INIT_DONE));
	orc.SyncWait(STATE_LOCAL_INIT_DONE, STATE_LOCAL_INIT_DONE);
	perf("STATE_LOCAL_INIT_DONE");

	// ARROW_RETURN_NOT_OK(orc.MapDevice(0));

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_REMOTE_INIT_DONE, STATE_REMOTE_INIT_DONE));
	orc.SyncWait(STATE_REMOTE_INIT_DONE, STATE_REMOTE_INIT_DONE);
	perf("STATE_REMOTE_INIT_DONE");

	// usleep(3 * 1000*1000);
	perf("Init");

	orc.PrintConfig();
	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_POST_INIT, STATE_POST_INIT));

	orc.SyncWait(STATE_POST_INIT, STATE_POST_INIT);
	perf("STATE_POST_INIT");

	
	// Fill fase
	std::shared_ptr<arrow::Buffer> buffer;
	ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateBuffer(1000));
	int8_t* buffer_data = (int8_t*)buffer->mutable_data();
	for (unsigned long i = 0; i < buffer->size(); i++) {
		buffer_data[i] = (int8_t)(i*2 & 0xFF);
	}
	std::cout << "Making array..." << std::endl;
  // std::shared_ptr<arrow::Buffer> null_buffer;
	// ARROW_ASSIGN_OR_RAISE(null_buffer, arrow::AllocateBuffer((buffer->size() + 7) / 8)); // at least 1/8th of size
	// for (unsigned long i = 0; i < null_buffer->size(); i++) {
	// 	((int8_t*)buffer->mutable_data())[i] = 0xFF;
	// }
	// auto data = arrow::ArrayData::Make(arrow::int8(), buffer->size(), {nullptr, buffer});
	// std::shared_ptr<arrow::Array> days = MakeArray(data);



	const int SIZE = 1*1024*1024 * 100/110;

	arrow::Int8Builder int8builder;
  int8_t* days_raw = (int8_t *)malloc(SIZE);
	for (unsigned long i = 0; i < SIZE; i++) {
		days_raw[i] = (int8_t)(i*2 & 0xFF);
	}
	perf("Days fill");
	ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, SIZE));
	perf("Builder append");
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> days, int8builder.Finish());
	perf("Builder finish");
	int8_t* months_raw = (int8_t *)malloc(SIZE);
	for (unsigned long i = 0; i < SIZE; i++) {
		months_raw[i] = (int8_t)(i%11 & 0xFF);
	}
	perf("months fill");
	ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, SIZE));
	perf("Builder append");
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> months, int8builder.Finish());

	// arrow::UInt16Builder uint16builder;
	// uint16_t* years_raw = (uint16_t *)malloc(SIZE*2);
	// for (unsigned long i = 0; i < SIZE; i++) {
	// 	years_raw[i] = (uint16_t)(i*217 & 0xFFFF);
	// }
	// ARROW_RETURN_NOT_OK(uint16builder.AppendValues(years_raw, SIZE));
  // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> years, uint16builder.Finish());

	perf("Builder finish");

  ARROW_RETURN_NOT_OK(days->Validate());
  ARROW_RETURN_NOT_OK(months->Validate());
  // ARROW_RETURN_NOT_OK(years->Validate());
	perf("array validation");

	// std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  // std::shared_ptr<arrow::Schema> schema;

	// field_day = arrow::field("Day", arrow::int8());
	// field_month = arrow::field("Month", arrow::int8());
	// field_year = arrow::field("Year", arrow::uint16());
  // // The schema can be built from a vector of fields, and we do so here.
  // schema = arrow::schema({field_day, field_month, field_year});
	// perf("Schema create");

	// std::shared_ptr<arrow::RecordBatch> rbatch = arrow::RecordBatch::Make(schema, SIZE, {days, months, years});
	// std::cout << rbatch->ToString();
	// orc.DebugRecordBatchPrintMetadata(rbatch);
	// perf("RecordBatch::Make");

	// ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {days}));
	// std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	// std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;
	// ARROW_ASSIGN_OR_RAISE(arrow::Datum months_sum_datum, arrow::compute::CallFunction("sum", {months}));
	// std::shared_ptr<arrow::Scalar> months_sum = months_sum_datum.scalar();
	// std::cout << "sum_months=" << months_sum->ToString() << std::endl;
	// ARROW_ASSIGN_OR_RAISE(arrow::Datum years_sum_datum, arrow::compute::CallFunction("sum", {years}));
	// std::shared_ptr<arrow::Scalar> years_sum = years_sum_datum.scalar();
	// std::cout << "sum_years=" << years_sum->ToString() << std::endl;

	// perf("Sum verify");

	// ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch, 0));
	perf("SendRecordBatchMD");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL1_DONE, STATE_FILL1_DONE));
	orc.SyncWait(STATE_FILL1_DONE, STATE_FILL1_DONE);
	perf("STATE_FILL1_DONE");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL1_DONEWAIT, STATE_FILL1_DONEWAIT));
	orc.SyncWait(STATE_FILL1_DONEWAIT, STATE_FILL1_DONEWAIT);
	perf("STATE_FILL1_DONEWAIT");

	// arrow::Int8Builder int8builder;
  // int8_t days_raw[5] = {1, 12, 17, 23, 28};
  // // AppendValues, as called, puts 5 values from days_raw into our Builder object.
  // ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
	// std::shared_ptr<arrow::Array> days;
  // ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());
	std::shared_ptr<arrow::Field> field_day;
	std::shared_ptr<arrow::Schema> schema;
	field_day = arrow::field("Day", arrow::int8());
	schema = arrow::schema({field_day});
	// std::shared_ptr<arrow::RecordBatch> rbatch;
	// rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});
	std::shared_ptr<arrow::Table> table2 = arrow::Table::Make(schema, {days}, SIZE);
	ARROW_RETURN_NOT_OK(orc.SendTableFull(table2, 0));
	perf("SendTableFull");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONE, STATE_FILL2_DONE));
	orc.SyncWait(STATE_FILL2_DONE, STATE_FILL2_DONE);
	perf("STATE_FILL2_DONE");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONEWAIT, STATE_FILL2_DONEWAIT));
	orc.SyncWait(STATE_FILL2_DONEWAIT, STATE_FILL2_DONEWAIT);
	perf("STATE_FILL2_DONEWAIT");

	//// Remote malloc
	/*
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::tf_device> dev, orc.GetDevice(0));
	std::shared_ptr<arrow::MemoryPool> memory_pool = dev->memory_pool;

	std::unique_ptr<arrow::UInt16Builder> uint16builderRemote = std::make_unique<arrow::UInt16Builder>(memory_pool.get());
	std::cout << "Builder created "; printf("memory_pool=0x%p\n", memory_pool.get());
	// arrow::UInt16Builder uint16builder;
	// std::unique_ptr<arrow::ArrayBuilder> uint16builderRemote;
	// arrow::MakeBuilder(memory_pool.get(), arrow::uint16(), &uint16builderRemote);
	uint16_t* data_local = (uint16_t *)malloc(SIZE*2);
	for (unsigned long i = 0; i < SIZE; i++) {
		data_local[i] = (uint16_t)((i*2+1) & 0xFFFF);
	}
	std::cout << "data created" << std::endl;
	ARROW_RETURN_NOT_OK(uint16builderRemote->AppendValues(data_local, SIZE));
	std::cout << "data appended" << std::endl;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> remote_array, uint16builderRemote->Finish());
	std::cout << "array created" << std::endl;
	// TODO unlock remote region + flush command
	std::shared_ptr<arrow::RecordBatch> rbatch2 = arrow::RecordBatch::Make(
		arrow::schema({arrow::field("Odds", arrow::uint8())}),
		SIZE, 
		{remote_array});
	std::cout << rbatch2->ToString();
	orc.DebugRecordBatchPrintMetadata(rbatch2);
	ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch2, 0));
	*/
	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL3_DONE, STATE_FILL3_DONE));
	orc.SyncWait(STATE_FILL3_DONE, STATE_FILL3_DONE);
	perf("STATE_FILL3_DONE");

	// {// This is ugly and you should not do this in production code.
  //   // In order to see the memory mappings of the currently
  //   // running process, we use the pmap (for process-map) tool to
  //   // query the kernel (/proc/self/maps)
  //   char cmd[256];
  //   snprintf(cmd, 256, "vmmap %d", getpid());
  //   printf("---- system(\"%s\"):\n", cmd);
  //   system(cmd);
  // }

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_PRE_PROG_END, STATE_PRE_PROG_END));
	orc.SyncWait(STATE_PRE_PROG_END, STATE_PRE_PROG_END);
	perf("STATE_PRE_PROG_END");

	// std::cout << "std::shared_ptr<arrow::Array> remote_array.use_count(): " << remote_array.use_count() << std::endl;
	// remote_array.reset();
	// rbatch2.reset(); // Should call destructor
	// std::cout << "std::shared_ptr<arrow::Array> remote_array.use_count(): " << remote_array.use_count() << std::endl;

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_PROG_DONE, STATE_PROG_DONE));
	orc.SyncWait(STATE_PROG_DONE, STATE_PROG_DONE);
	perf("STATE_PROG_DONE");

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
