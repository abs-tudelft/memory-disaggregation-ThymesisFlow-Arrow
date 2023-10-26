#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <vector>

#include "common.h"

arrow::Status RunMain(int argc, char** argv) {
	perf("");
	arrow::Orchestrator orc;

	// ARROW_RETURN_NOT_OK(orc.AddDeviceLocal(0, (void*)0x700000000000, 16384));
	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		0, false, 
		"127.0.0.1:6000", 
		(void*)0x700000000000, 100*64*16384, std::string("./mapping0")));

	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		1, false, "127.0.0.1:6001", 
		(void*)0x710000000000, 100*64*16384, std::string("./mapping1")));

	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		2, true, "0.0.0.0:6002", 
		(void*)0x720000000000, 100*64*16384, std::string("./mapping2")));
	ARROW_RETURN_NOT_OK(orc.InitializeServer());

	// usleep(3 * 1000*1000);

	perf("Init");

	orc.PrintConfig();
	orc.WaitAllDevicesOnline();
	perf("WaitAllDevicesOnline");
	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_POST_INIT, STATE_POST_INIT));

	orc.SyncWait(STATE_POST_INIT, STATE_POST_INIT);
	perf("STATE_POST_INIT");

	// Fill fase
	// std::shared_ptr<arrow::Buffer> buffer;
	// ARROW_ASSIGN_OR_RAISE(buffer, arrow::AllocateBuffer(1000));
	// int8_t* buffer_data = (int8_t*)buffer->mutable_data();
	// for (unsigned long i = 0; i < buffer->size(); i++) {
	// 	buffer_data[i] = (int8_t)(i*2 & 0xFF);
	// }
	// std::cout << "Making array..." << std::endl;
  // // std::shared_ptr<arrow::Buffer> null_buffer;
	// // ARROW_ASSIGN_OR_RAISE(null_buffer, arrow::AllocateBuffer((buffer->size() + 7) / 8)); // 1/8th of size
	// // for (unsigned long i = 0; i < null_buffer->size(); i++) {
	// // 	((int8_t*)buffer->mutable_data())[i] = 0xFF;
	// // }
	// auto data = arrow::ArrayData::Make(arrow::int8(), buffer->size(), {nullptr, buffer});
	// std::shared_ptr<arrow::Array> days = MakeArray(data);

	const int SIZE = 2*1024*1024 * 100/110;

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
	perf("Builder finish");

  ARROW_RETURN_NOT_OK(days->Validate());
  ARROW_RETURN_NOT_OK(months->Validate());
	perf("array validation");

	std::shared_ptr<arrow::Field> field_day, field_month;
  std::shared_ptr<arrow::Schema> schema;

	field_day = arrow::field("Day", arrow::int8());
	field_month = arrow::field("Month", arrow::int8());
  // The schema can be built from a vector of fields, and we do so here.
  schema = arrow::schema({field_day, field_month});
	perf("Schema create");

	std::shared_ptr<arrow::RecordBatch> rbatch = arrow::RecordBatch::Make(schema, SIZE, {days, months});
	std::cout << rbatch->ToString();
	orc.DebugRecordBatchPrintMetadata(rbatch);
	perf("RecordBatch::Make");

	ARROW_RETURN_NOT_OK(orc.SendRecordBatchMD(rbatch, SIZE));
	perf("SendRecordBatchMD");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL1_DONE, STATE_FILL1_DONE));
	orc.SyncWait(STATE_FILL1_DONE, STATE_FILL1_DONE);
	perf("STATE_FILL1_DONE");

	std::shared_ptr<arrow::Table> table2 = arrow::Table::Make(schema, {days, months}, SIZE);
	ARROW_RETURN_NOT_OK(orc.SendTableFull(table2, SIZE));
	perf("SendTableFull");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONE, STATE_FILL2_DONE));
	orc.SyncWait(STATE_FILL2_DONE, STATE_FILL2_DONE);
	perf("STATE_FILL2_DONE");

	// {// This is ugly and you should not do this in production code.
  //   // In order to see the memory mappings of the currently
  //   // running process, we use the pmap (for process-map) tool to
  //   // query the kernel (/proc/self/maps)
  //   char cmd[256];
  //   snprintf(cmd, 256, "vmmap %d", getpid());
  //   printf("---- system(\"%s\"):\n", cmd);
  //   system(cmd);
  // }

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_PROG_DONE, STATE_PROG_DONE));
	orc.SyncWait(STATE_PROG_DONE, STATE_PROG_DONE);
	perf("STATE_PROG_DONE");
	
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
