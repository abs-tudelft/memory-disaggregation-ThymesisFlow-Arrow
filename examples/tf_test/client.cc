#include <arrow/api.h>

#include <iostream>
#include <unistd.h>

arrow::Status RunMain() {
  auto arrowBuildInfo = arrow::GetBuildInfo();
  
  std::cout << "Arrow version=" << arrowBuildInfo.version_string << std::endl;

  arrow::Orchestrator orc;
  ARROW_RETURN_NOT_OK(orc.setFlag(10, 100));

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

