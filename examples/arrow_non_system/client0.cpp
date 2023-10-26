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
		0, true, "0.0.0.0:6000", 
		(void*)0x700000000000, 100*64*16384, std::string("./mapping0")));
	ARROW_RETURN_NOT_OK(orc.InitializeServer());

	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		1, false, "127.0.0.1:6001", 
		(void*)0x710000000000, 100*64*16384, std::string("./mapping1")));

	ARROW_RETURN_NOT_OK(orc.AddDeviceFile(
		2, false, "127.0.0.1:6002", 
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
		// Do nothing
	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL1_DONE, STATE_FILL1_DONE));
	orc.SyncWait(STATE_FILL1_DONE, STATE_FILL1_DONE);
	perf("STATE_FILL1_DONE");

	// Combine all received tables, and print contents
	std::shared_ptr<arrow::Table> table;
	ARROW_ASSIGN_OR_RAISE(table, orc.GetHugeTable());
	perf("GetHugeTable");
	std::cout << table->ToString();
	perf("print");

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_FILL2_DONE, STATE_FILL2_DONE));
	orc.SyncWait(STATE_FILL2_DONE, STATE_FILL2_DONE);
	perf("STATE_FILL2_DONE");

	std::shared_ptr<arrow::Table> table2;
	ARROW_ASSIGN_OR_RAISE(table2, orc.GetHugeTable());
	perf("GetHugeTable");
	std::cout << table2->ToString();
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

	ARROW_RETURN_NOT_OK(orc.SetFlag(STATE_PROG_DONE, STATE_PROG_DONE));
	orc.SyncWait(STATE_PROG_DONE, STATE_PROG_DONE);
	
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
