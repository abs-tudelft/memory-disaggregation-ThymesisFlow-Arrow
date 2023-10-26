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

#include <omp.h>

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

void FloatArrayToCsv(double * array, unsigned int rows, unsigned int columns, const char * fn) {
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

double CalcArrayAvg(double * array, int len) {
  double sum = 0;

  for (int i = 0; i < len; i++) {
    sum += array[i];
  }

  return sum / len;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> generateTestRecordBatch(arrow::MemoryPool * memory_pool, uint64_t SIZE) {
	arrow::Int64Builder int64builder(memory_pool);
  int64_t* days_raw = (int64_t *)malloc(SIZE*8);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		days_raw[i] = (int64_t)i;
	}
	ARROW_RETURN_NOT_OK(int64builder.AppendValues(days_raw, SIZE));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> days, int64builder.Finish());

	arrow::Int8Builder int8builder(memory_pool);
	int8_t* months_raw = (int8_t *)malloc(SIZE);
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		months_raw[i] = (int8_t)i;
	}
	ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, SIZE));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> months, int8builder.Finish());

	arrow::UInt16Builder uint16builder(memory_pool);
	uint16_t* years_raw = (uint16_t *)malloc(SIZE*2);
	perf(NULL);
	#pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		years_raw[i] = (uint16_t)i;
	}
	ARROW_RETURN_NOT_OK(uint16builder.AppendValues(years_raw, SIZE));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> years, uint16builder.Finish());


  ARROW_RETURN_NOT_OK(days->Validate());
  ARROW_RETURN_NOT_OK(months->Validate());
  ARROW_RETURN_NOT_OK(years->Validate());

	std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

	field_day = arrow::field("Day", arrow::int64());
	field_month = arrow::field("Month", arrow::int8());
	field_year = arrow::field("Year", arrow::uint16());
  // The schema can be built from a vector of fields, and we do so here.
  schema = arrow::schema({field_day, field_month, field_year});

	std::shared_ptr<arrow::RecordBatch> rbatch = arrow::RecordBatch::Make(schema, SIZE, {days, months, years});

	return rbatch;
}

// double ms_double = ;
arrow::Status bm_datatypes(uint64_t SIZE, arrow::MemoryPool * memory_pool) {
  TimePoint t_start, t_end;
  double t_delta, throughput;

  void * address;
  ARROW_RETURN_NOT_OK(memory_pool->Allocate((SIZE+1)*8, (uint8_t**)&address));

  omp_set_dynamic(0); // thread number is exact, not an upper limit

  double measurements_4x24 [24][4] = {0};

  //
  // Writes
  //

  // 1 thread
  std::cout << "write,1T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint8_t *)address)[i] = (uint8_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[0][0] = 0; // 0=write, 1=read
  measurements_4x24[0][1] = 8; // bitlength datatype
  measurements_4x24[0][2] = 1; // threads
  measurements_4x24[0][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,1T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint16_t *)address)[i] = (uint16_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[1][0] = 0;
  measurements_4x24[1][1] = 16;
  measurements_4x24[1][2] = 1;
  measurements_4x24[1][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,1T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint32_t *)address)[i] = (uint32_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[2][0] = 0;
  measurements_4x24[2][1] = 32;
  measurements_4x24[2][2] = 1;
  measurements_4x24[2][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,1T,u64" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint64_t *)address)[i] = (uint64_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[3][0] = 0;
  measurements_4x24[3][1] = 64;
  measurements_4x24[3][2] = 1;
  measurements_4x24[3][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  // 4 threads
  std::cout << "write,4T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint8_t *)address)[i] = (uint8_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[4][0] = 0;
  measurements_4x24[4][1] = 8;
  measurements_4x24[4][2] = 4;
  measurements_4x24[4][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,4T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint16_t *)address)[i] = (uint16_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[5][0] = 0;
  measurements_4x24[5][1] = 16;
  measurements_4x24[5][2] = 4;
  measurements_4x24[5][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,4T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint32_t *)address)[i] = (uint32_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[6][0] = 0;
  measurements_4x24[6][1] = 32;
  measurements_4x24[6][2] = 4;
  measurements_4x24[6][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,4T,u64" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint64_t *)address)[i] = (uint64_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[7][0] = 0;
  measurements_4x24[7][1] = 64;
  measurements_4x24[7][2] = 4;
  measurements_4x24[7][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  // 48 threads
  std::cout << "write,48T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint8_t *)address)[i] = (uint8_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[8][0] = 0;
  measurements_4x24[8][1] = 8;
  measurements_4x24[8][2] = 48;
  measurements_4x24[8][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,48T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint16_t *)address)[i] = (uint16_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[9][0] = 0;
  measurements_4x24[9][1] = 16;
  measurements_4x24[9][2] = 48;
  measurements_4x24[9][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,48T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint32_t *)address)[i] = (uint32_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[10][0] = 0;
  measurements_4x24[10][1] = 32;
  measurements_4x24[10][2] = 48;
  measurements_4x24[10][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "write,48T,u64" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		((uint64_t *)address)[i] = (uint64_t)i;
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[11][0] = 0;
  measurements_4x24[11][1] = 64;
  measurements_4x24[11][2] = 48;
  measurements_4x24[11][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers




  //
  // Reads
  //
  uint64_t agg; // Random local variable so that reads are not optimized away.

  // 1 thread
  std::cout << "read,1T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint8_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[12][0] = 1;
  measurements_4x24[12][1] = 8;
  measurements_4x24[12][2] = 1;
  measurements_4x24[12][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,1T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint16_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[13][0] = 1;
  measurements_4x24[13][1] = 16;
  measurements_4x24[13][2] = 1;
  measurements_4x24[13][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,1T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint32_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[14][0] = 1;
  measurements_4x24[14][1] = 32;
  measurements_4x24[14][2] = 1;
  measurements_4x24[14][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,1T,u64" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(1)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint64_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[15][0] = 1;
  measurements_4x24[15][1] = 64;
  measurements_4x24[15][2] = 1;
  measurements_4x24[15][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  // 4 threads
  std::cout << "read,4T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint8_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[16][0] = 1;
  measurements_4x24[16][1] = 8;
  measurements_4x24[16][2] = 4;
  measurements_4x24[16][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,4T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint16_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[17][0] = 1;
  measurements_4x24[17][1] = 16;
  measurements_4x24[17][2] = 4;
  measurements_4x24[17][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,4T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint32_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[18][0] = 1;
  measurements_4x24[18][1] = 32;
  measurements_4x24[18][2] = 4;
  measurements_4x24[18][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,4T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(4)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint64_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[19][0] = 1;
  measurements_4x24[19][1] = 64;
  measurements_4x24[19][2] = 4;
  measurements_4x24[19][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  // 48 threads
  std::cout << "read,48T,u8" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint8_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[20][0] = 1;
  measurements_4x24[20][1] = 8;
  measurements_4x24[20][2] = 48;
  measurements_4x24[20][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,48T,u16" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint16_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*2/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[21][0] = 1;
  measurements_4x24[21][1] = 16;
  measurements_4x24[21][2] = 48;
  measurements_4x24[21][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,48T,u32" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint32_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*4/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[22][0] = 1;
  measurements_4x24[22][1] = 32;
  measurements_4x24[22][2] = 48;
  measurements_4x24[22][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "read,48T,u64" << std::endl;
  t_start = Clock::now();
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd num_threads(48)
	for (uint64_t i = 0; i < SIZE; i++) {
		agg += ((uint64_t *)address)[i];
	}
	t_end = Clock::now();
  t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
  throughput = ((double)SIZE*8/(1024*1024*1024)) / (t_delta/1000);
  measurements_4x24[23][0] = 1;
  measurements_4x24[23][1] = 64;
  measurements_4x24[23][2] = 48;
  measurements_4x24[23][3] = throughput;
  usleep(500*1000); // Sleep for clearing buffers

  std::cout << "agg=" << agg << std::endl;

  FloatArrayToCsv((double*)measurements_4x24, 24, 4, "bm_datatypes.csv");

  return arrow::Status::OK();
}

arrow::Status bm_access_serial(double * measurements, uint64_t NUM, uint64_t SIZE, arrow::MemoryPool * memory_pool) {
  TimePoint t_start, t_end;
  double t_delta, throughput;

  uint64_t * col1_buffer_data;
  ARROW_RETURN_NOT_OK(memory_pool->Allocate((SIZE+2048)*8, (uint8_t**)&col1_buffer_data));
  // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> col1_buffer, arrow::AllocateBuffer((SIZE+2048)*8, memory_pool));
	// uint64_t* col1_buffer_data = (uint64_t*)col1_buffer->mutable_data();

  for (int n = 0; n < NUM; n++) {
    t_start = Clock::now();
    #pragma omp unroll partial(64)
    #pragma omp parallel for simd
    for (uint64_t i = 0; i < SIZE+2048; i++) {
      col1_buffer_data[i] = i;
    }
    t_end = Clock::now();
    t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
    throughput = ((double)(SIZE+2048)*8 / (double)(1024UL*1024UL*1024UL) / (t_delta/1000));
    std::cout << "Comes to throughput of: " << throughput << " GB/s" << std::endl;

    measurements[n] = throughput;
  }

  return arrow::Status::OK();
}

arrow::Status bm_access_strided_read(uint64_t NUM, uint64_t SIZE, arrow::MemoryPool * memory_pool) {
  TimePoint t_start, t_end;
  double t_delta, throughput;

  uint64_t * col1_buffer_data;
  ARROW_RETURN_NOT_OK(memory_pool->Allocate((SIZE+2048)*8, (uint8_t**)&col1_buffer_data));
  // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> col1_buffer, arrow::AllocateBuffer((SIZE+2048)*8, memory_pool));
	// uint64_t* col1_buffer_data = (uint64_t*)col1_buffer->mutable_data();

  // Init random data
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < SIZE+2048; i++) {
		col1_buffer_data[i] = i;
	}

  std::vector<uint64_t> strides = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
  double measurements_output[strides.size()][2] = {0};
  double measurements[NUM] = {0};

  uint64_t agg = 0;
  // For every stride amount, do an experiment
  for (uint64_t stride_index = 0; stride_index < strides.size(); ++stride_index) {
    uint64_t stride = strides[stride_index];
    // Repeat experiment NUM times
    for (int n = 0; n < NUM; n++) {
      std::cout << "s="<<stride<<",n="<<n<<std::endl;
      t_start = Clock::now();
      // For every offset
      for (uint64_t i = 0; i < stride; i++) {
        // for (int64_t j = 0; j < ((SIZE + stride-1) / stride); j++) {
        #pragma omp unroll partial(64)
        #pragma omp parallel for
        for (int64_t j = 0; j < SIZE; j+= stride) {
          agg += col1_buffer_data[i+j];
        }
      }
      t_end = Clock::now();
      t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
      throughput = ((double)(SIZE+2048)*8 / (double)(1024UL*1024UL*1024UL) / (t_delta/1000));
      
      measurements[n] = throughput;

	    usleep(5 * 1000*1000); 

    }
	  usleep(5 * 1000*1000); 

    std::cout << "\tcalc avg=";
    double avg = CalcArrayAvg(measurements, NUM);
    std::cout << avg<<std::endl;
    measurements_output[stride_index][0] = stride;
    measurements_output[stride_index][1] = avg;

    // printf("\tStride=%lu time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", stride, t, ((SIZE+2048)*8 / (1024UL*1024UL*1024UL) / (t_delta/1000)), agg, cnt);
  }

  FloatArrayToCsv((double*)measurements_output, strides.size(), 2, "bm_access_strided_read.csv");

  return arrow::Status::OK();
}

arrow::Status bm_access_strided_write(uint64_t NUM, uint64_t SIZE, arrow::MemoryPool * memory_pool) {
  TimePoint t_start, t_end;
  double t_delta, throughput;

  uint64_t * col1_buffer_data;
  ARROW_RETURN_NOT_OK(memory_pool->Allocate((SIZE+2048)*8, (uint8_t**)&col1_buffer_data));
  // ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> col1_buffer, arrow::AllocateBuffer((SIZE+2048)*8, memory_pool));
	// uint64_t* col1_buffer_data = (uint64_t*)col1_buffer->mutable_data();

  // Init random data
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < SIZE+2048; i++) {
		col1_buffer_data[i] = i;
	}

  std::vector<uint64_t> strides = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
  double measurements_output[strides.size()][2] = {0};
  double measurements[NUM] = {0};

  uint64_t agg = 0;
  // For every stride amount, do an experiment
  for (uint64_t stride_index = 0; stride_index < strides.size(); ++stride_index) {
    uint64_t stride = strides[stride_index];
    // Repeat experiment NUM times
    for (int n = 0; n < NUM; n++) {
      std::cout << "s="<<stride<<",n="<<n<<std::endl;
      t_start = Clock::now();
      // For every offset
      for (uint64_t i = 0; i < stride; i++) {
        // for (int64_t j = 0; j < ((SIZE + stride-1) / stride); j++) {
        #pragma omp unroll partial(64)
        #pragma omp parallel for simd
        for (int64_t j = 0; j < SIZE; j+= stride) {
          col1_buffer_data[i+j] = i+j;
        }
      }
      t_end = Clock::now();
      t_delta = std::chrono::duration<double, std::milli>(t_end-t_start).count();
      throughput = ((double)(SIZE+2048)*8 / (double)(1024UL*1024UL*1024UL) / (t_delta/1000));
      
      measurements[n] = throughput;
    }

    std::cout << "\tcalc avg=";
    double avg = CalcArrayAvg(measurements, NUM);
    std::cout << avg<<std::endl;
    measurements_output[stride_index][0] = stride;
    measurements_output[stride_index][1] = avg;

    // printf("\tStride=%lu time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", stride, t, ((SIZE+2048)*8 / (1024UL*1024UL*1024UL) / (t/1000)), agg, cnt);
  }

  FloatArrayToCsv((double *)measurements_output, strides.size(), 2, "bm_access_strided_write.csv");

  return arrow::Status::OK();
}