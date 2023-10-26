#include <iostream>
#include <chrono>
#include "arrow/util/visibility.h"

#include "tperf.h"

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;


void tperf(const char * measurement_name) {
  static TimePoint t_prev = TimePoint();

  if (!std::chrono::duration_cast<std::chrono::milliseconds>(t_prev.time_since_epoch()).count()) {
    t_prev = Clock::now();
    return;
  }

  TimePoint t_now = Clock::now();
  if (measurement_name) {
    auto ms_double = std::chrono::duration<double, std::milli>(t_now - t_prev).count();

    std::cout << "Timed " << measurement_name << " : " << ms_double << " ms" << std::endl;
  }

  
  t_prev = t_now;
}