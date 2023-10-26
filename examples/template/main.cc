#include <arrow/api.h>

#include <iostream>

arrow::Status RunMain() {
  

  return arrow::Status::OK();

}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}

