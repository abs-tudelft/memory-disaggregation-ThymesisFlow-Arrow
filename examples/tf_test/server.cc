#include <arrow/api.h>

#include <iostream>
#include <unistd.h>

arrow::Status RunMain() {
  auto arrowBuildInfo = arrow::GetBuildInfo();
  
  std::cout << "Arrow version=" << arrowBuildInfo.version_string << std::endl;

  arrow::Orchestrator orc;
  orc.InitializeServer();

  RETURN_NOT_OK(orc.AddDevicePCIe(
    0, false,
    (void *)0xf00000, 4096,
    std::string("/dev/sys/INVALID")
  ));

  arrow::thymesis_print_maps();

  {// This is ugly and you should not do this in production code.

    // In order to see the memory mappings of the currently
    // running process, we use the pmap (for process-map) tool to
    // query the kernel (/proc/self/maps)
    char cmd[256];
    snprintf(cmd, 256, "pmap %d", getpid());
    printf("---- system(\"%s\"):\n", cmd);
    system(cmd);
  }

  while (1) {}

  return arrow::Status::OK();

}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    std::cout << "NOT OK" << std::endl;
    return 1;
  }
  std::cout << "OK" << std::endl;
  return 0;
}

