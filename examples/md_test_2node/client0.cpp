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

	ARROW_RETURN_NOT_OK(orc.AddDeviceSharedMemoryFile(
		0, true, "0.0.0.0:6000", 
		(void*)0x700000000000, 100*64*16384, std::string("/tf_mapping0")));
	ARROW_RETURN_NOT_OK(orc.MapDevice(0));
	
	ARROW_RETURN_NOT_OK(orc.AddDeviceSharedMemoryFile(
		1, false, "127.0.0.1:6001", 
		(void*)0x710000000000, 100*64*16384, std::string("/tf_mapping1")));

	ARROW_RETURN_NOT_OK(orc.InitializeServer());
	orc.WaitAllDevicesOnline();
	perf("WaitAllDevicesOnline");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_LOCAL_INIT_DONE, STATE_LOCAL_INIT_DONE));
	orc.SyncWait(STATE_LOCAL_INIT_DONE, STATE_LOCAL_INIT_DONE);
	perf("STATE_LOCAL_INIT_DONE");

	ARROW_RETURN_NOT_OK(orc.MapDevice(1));

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
		// Do nothing
	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL1_DONE, STATE_FILL1_DONE));
	orc.SyncWait(STATE_FILL1_DONE, STATE_FILL1_DONE);
	perf("STATE_FILL1_DONE");

	// Combine all received tables, and print contents
	std::shared_ptr<arrow::Table> table;
	ARROW_ASSIGN_OR_RAISE(table, orc.GetHugeTable());
	perf("GetHugeTable");
	std::cout << table->ToString();

	ARROW_ASSIGN_OR_RAISE(arrow::Datum days_sum_datum, arrow::compute::CallFunction("sum", {table->column(0)}));
	std::shared_ptr<arrow::Scalar> days_sum = days_sum_datum.scalar();
	std::cout << "sum_days=" << std::dec << days_sum->ToString()  << std::endl;
	ARROW_ASSIGN_OR_RAISE(arrow::Datum months_sum_datum, arrow::compute::CallFunction("sum", {table->column(1)}));
	std::shared_ptr<arrow::Scalar> months_sum = months_sum_datum.scalar();
	std::cout << "sum_months=" << months_sum->ToString() << std::endl;
	ARROW_ASSIGN_OR_RAISE(arrow::Datum years_sum_datum, arrow::compute::CallFunction("sum", {table->column(2)}));
	std::shared_ptr<arrow::Scalar> years_sum = years_sum_datum.scalar();
	std::cout << "sum_years=" << years_sum->ToString() << std::endl;

	perf("print");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONE, STATE_FILL2_DONE));
	orc.SyncWait(STATE_FILL2_DONE, STATE_FILL2_DONE);
	perf("STATE_FILL2_DONE");

	std::shared_ptr<arrow::Table> table2;
	ARROW_ASSIGN_OR_RAISE(table2, orc.GetHugeTable());
	perf("GetHugeTable");
	std::cout << table2->ToString();
	perf("print");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONEWAIT, STATE_FILL2_DONEWAIT));
	orc.SyncWait(STATE_FILL2_DONEWAIT, STATE_FILL2_DONEWAIT);
	perf("STATE_FILL2_DONEWAIT");
	

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL3_DONE, STATE_FILL3_DONE));
	orc.SyncWait(STATE_FILL3_DONE, STATE_FILL3_DONE);
	perf("STATE_FILL3_DONE");

	std::shared_ptr<arrow::Table> table3;
	ARROW_ASSIGN_OR_RAISE(table3, orc.GetHugeTable());
	perf("GetHugeTable");
	std::cout << table3->ToString();
	perf("print");


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
