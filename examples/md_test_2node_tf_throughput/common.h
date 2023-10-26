#pragma once

#define STATE_LOCAL_INIT_DONE (1 << 1)
#define STATE_REMOTE_INIT_DONE (1 << 2)
#define STATE_POST_INIT (1 << 3)
#define STATE_FILL1_DONE (1 << 4)
#define STATE_FILL1_DONEWAIT (1 << 5)
#define STATE_FILL2_DONE (1 << 6)
#define STATE_FILL2_DONEWAIT (1 << 7)
#define STATE_FILL3_DONE (1 << 8)
#define STATE_PRE_PROG_END (1 << 9)
#define STATE_PROG_DONE (1 << 10)


#include <chrono>

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
TimePoint t_prev = TimePoint();
double perf(const char * measurement_name) {
  if (!std::chrono::duration_cast<std::chrono::milliseconds>(t_prev.time_since_epoch()).count()) {
    t_prev = Clock::now();
    return -1;
  }

  TimePoint t_now = Clock::now();

  if (measurement_name) {
    double ms_double = std::chrono::duration<double, std::milli>(t_now - t_prev).count();

    std::cout << "Client Timed " << measurement_name << " : " << ms_double << " ms" << std::endl;
    t_prev = t_now;
    
    return ms_double;
  }

  t_prev = t_now;
  return -1;
}

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

arrow::Status waitFor(uint32_t waitFlagMask) {
  arrow::Orchestrator orc;

  ARROW_RETURN_NOT_OK(orc.SetFlag(waitFlagMask, waitFlagMask));
	orc.SyncWait(waitFlagMask, waitFlagMask);

  return arrow::Status::OK();
}