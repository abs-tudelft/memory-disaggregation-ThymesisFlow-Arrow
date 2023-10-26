#pragma once

#define STATE_LOCAL_INIT_DONE (1 << 1)
#define STATE_REMOTE_INIT_DONE (1 << 2)
#define STATE_POST_INIT (1 << 3)
#define STATE_FILL1_DONE (1 << 4)
#define STATE_FILL2_DONE (1 << 5)
#define STATE_FILL2_DONEWAIT (1 << 6)
#define STATE_FILL3_DONE (1 << 7)
#define STATE_PRE_PROG_END (1 << 8)
#define STATE_PROG_DONE (1 << 9)


#include <chrono>

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
TimePoint t_prev = TimePoint();
double perf(const char * measurement_name) {
  if (!std::chrono::duration_cast<std::chrono::milliseconds>(t_prev.time_since_epoch()).count()) {
    t_prev = Clock::now();
    return 0;
  }

  TimePoint t_now = Clock::now();
  double ms_double = std::chrono::duration<double, std::milli>(t_now - t_prev).count();

  if (measurement_name)
    std::cout << "Timed " << measurement_name << " : " << ms_double << " ms" << std::endl;

  t_prev = t_now;

  return ms_double;
}


#include <iostream>
#include <fstream>

void FloatArrayToCsv(float * array, unsigned int rows, unsigned int columns, const char * fn) {
  std::ofstream of;
  of.open(fn);
  for (unsigned int r = 0; r < rows; r++) {
    for (unsigned int c = 0; c < columns; c++) {
      of << *(array + r*columns + c);
      if (c != (columns-1)) 
        of << ",";
    }
    of << "\n";
  }
  of.close();
}