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

#include <arrow/status.h>

Status waitFor(const char * what, uint32_t waitFlagMask) {
  ARROW_RETURN_NOT_OK(orc.SetFlag(waitFlagMask, waitFlagMask));
	orc.SyncWait(waitFlagMask, waitFlagMask);

  return arrow::Status::OK();
}