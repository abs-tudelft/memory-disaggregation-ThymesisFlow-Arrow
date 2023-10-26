#pragma once

#define STATE_POST_INIT (1 << 1)
#define STATE_FILL1_DONE (1 << 2)
#define STATE_FILL2_DONE (1 << 3)
#define STATE_PROG_DONE (1 << 4)


#include <chrono>

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
TimePoint t_prev = TimePoint();
void perf(const char * measurement_name) {
  if (!std::chrono::duration_cast<std::chrono::milliseconds>(t_prev.time_since_epoch()).count()) {
    t_prev = Clock::now();
    return;
  }

  TimePoint t_now = Clock::now();
  auto ms_double = std::chrono::duration<double, std::milli>(t_now - t_prev).count();

  std::cout << "Timed " << measurement_name << " : " << ms_double << " ms" << std::endl;
  t_prev = t_now;
}