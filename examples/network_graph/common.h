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

#include <limits.h> // INT_MAX
#include <arrow/csv/api.h>
arrow::Result<std::shared_ptr<arrow::Table>> load_graph(arrow::MemoryPool * memory_pool) {
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("soc-twitter-follows.mtx"));
  std::shared_ptr<arrow::Table> out_table;

  arrow::io::IOContext io_context = arrow::io::IOContext(memory_pool);

  auto parse_options = arrow::csv::ParseOptions::Defaults();
  parse_options.delimiter = ' ';

  arrow::csv::ConvertOptions convert_options = arrow::csv::ConvertOptions::Defaults();
  // convert_options.include_columns = {}
  //std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
  convert_options.column_types = {{"col1", arrow::uint32()}, {"col2", arrow::uint32()}, };

  arrow::csv::ReadOptions read_options = arrow::csv::ReadOptions::Defaults();
  read_options.column_names = {"col1", "col2"};
  read_options.use_threads = false; // Disable threads to ensure contiguous vectors
  read_options.block_size = INT_MAX;

  ARROW_ASSIGN_OR_RAISE(
    auto csv_reader,
    arrow::csv::TableReader::Make(
      io_context, infile, read_options,
      parse_options, convert_options));

  ARROW_ASSIGN_OR_RAISE(out_table, csv_reader->Read());

  return out_table;
}

arrow::Result<uint64_t> get_max(std::shared_ptr<arrow::ChunkedArray> cArray) {
  std::cout << "get max for " << cArray->ToString() << std::endl;

  arrow::Datum max_datum;
  ARROW_ASSIGN_OR_RAISE(max_datum, arrow::compute::CallFunction("max", {cArray}));
	std::shared_ptr<arrow::Scalar> max_scalar = max_datum.scalar();
  if (!max_scalar->is_valid) return arrow::Status::Invalid("get_max scalar not valid");
  ARROW_RETURN_NOT_OK(max_scalar->ValidateFull());

  uint64_t max = std::static_pointer_cast<arrow::UInt32Scalar>(max_scalar)->value;

  return max;
}

arrow::Result<uint64_t> get_sum(std::shared_ptr<arrow::ChunkedArray> cArray) {
  arrow::Datum sum_datum;
  ARROW_ASSIGN_OR_RAISE(sum_datum, arrow::compute::CallFunction("sum", {cArray}));
	std::shared_ptr<arrow::Scalar> sum_scalar = sum_datum.scalar();
  uint64_t sum = std::static_pointer_cast<arrow::UInt32Scalar>(sum_scalar)->value;

  return sum;
}

#include <algorithm>    // std::max
arrow::Status run_benchmark_infections(arrow::MemoryPool * memory_pool) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, load_graph(memory_pool));

  double t;

  // Scan for highest node number, serial access pattern
  perf(NULL);
  ARROW_ASSIGN_OR_RAISE(uint64_t max0, get_max(table->column(0)));
  ARROW_ASSIGN_OR_RAISE(uint64_t max1, get_max(table->column(1)));
  uint64_t highest_node = std::max(max0, max1);
  perf("Max calculation");
  std::cout << "Max node number is: " << highest_node << " max0="<<max0 << " max1="<<max1 << std::endl;

  // Allocate output array, size of highest node number
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> nodes_infected_buffer, arrow::AllocateBuffer(highest_node, memory_pool));
  uint8_t* nodes_infected = (uint8_t*)nodes_infected_buffer->mutable_data();
	perf(NULL);
	#pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < highest_node; i++) {
		nodes_infected[i] = 0;
	}
	t = perf("memset 0");
	std::cout << "Comes to throughput of: " << ((uint32_t)highest_node / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> edges_visited_buffer, arrow::AllocateBuffer(table->num_rows(), memory_pool));
  uint8_t* edges_visited = (uint8_t*)edges_visited_buffer->mutable_data();
	perf(NULL);
	#pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < highest_node; i++) {
		edges_visited[i] = 0;
	}
	t = perf("memset 0");
	std::cout << "Comes to throughput of: " << ((uint32_t)highest_node / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

  // "infect" an initial node
  // uint32_t initial_infected = std::static_pointer_cast<arrow::UInt32Scalar>(table->column(1)->GetScalar(0).ValueOrDie())->value;
  uint32_t initial_infected = 99250;
  nodes_infected[initial_infected] = 1;
  // Iterate infection
  // iterate until no updates
  bool updated_something = true;
  uint64_t iter_count = 0;
  while (updated_something) {
    updated_something = false;

    for (uint32_t i = 0; i < table->num_rows(); i++) {
      // If this edge has not passed an infection before. An earlier passed infection means both the nodes are infected already.
      if (edges_visited[i] == 0) {
        uint32_t n0 = std::static_pointer_cast<arrow::UInt32Scalar>(table->column(0)->GetScalar(i).ValueOrDie())->value;
        uint32_t n1 = std::static_pointer_cast<arrow::UInt32Scalar>(table->column(1)->GetScalar(i).ValueOrDie())->value;
        
        // std::cout << "n0="<<n0 << " n1="<<n1 << std::endl;
        if (nodes_infected[n0]) {
          nodes_infected[n1] = 1;
          edges_visited[i] = 1;
          updated_something = true;
        } else if (nodes_infected[n1]) {
          nodes_infected[n0] = 1;
          edges_visited[i] = 1;
          updated_something = true;
        }
      }
    }

    iter_count++;
    if ((iter_count % 100) == 0) {
      std::cout << "iter_count = " << iter_count << std::endl;
    }
  }

  // Infected count
	std::shared_ptr<arrow::ArrayData> nodes_infected_ad = arrow::ArrayData::Make(arrow::uint8(), highest_node, {nullptr, nodes_infected_buffer});
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ChunkedArray> nodes_infected_ca, arrow::ChunkedArray::Make({arrow::MakeArray(nodes_infected_ad)}));
  ARROW_ASSIGN_OR_RAISE(uint64_t count, get_sum(nodes_infected_ca));


  std::cout << "Finished, took iter_count = " << iter_count << " infected count = " << count << std::endl;

  return arrow::Status::OK();
}



#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <cmath>
#include <unordered_map>


unsigned long long modexp(unsigned long long b, unsigned long long e, unsigned long long m) {
    std::vector<int> bits;
    for (int bit = 0; bit < sizeof(e) * 8; ++bit) {
        bits.push_back((e >> bit) & 1);
    }
    unsigned long long s = b;
    unsigned long long v = 1;
    for (int bit : bits) {
        if (bit == 1) {
            v *= s;
            v %= m;
        }
        s *= s;
        s %= m;
    }
    return v;
}

class CyclicPRNG {
private:
    int size;
    unsigned long long modulus;
    std::unordered_map<unsigned long long, int> modulus_factors;
    unsigned long long generator;
    unsigned long long start;
    unsigned long long end;
    unsigned long long current;
    std::chrono::time_point<std::chrono::system_clock> cycle_start_time;
    int completed_cycle_count;
    bool consistent;

    void initCyclicGroup() {
        auto next_prime = [](int num) {
            num = num + 1 + (num % 2);
            while (true) {
                bool isPrime = true;
                for (int i = 2; i <= std::sqrt(num); ++i) {
                    if (num % i == 0) {
                        isPrime = false;
                        break;
                    }
                }
                if (isPrime) {
                    return num;
                }
                num += 2;
            }
        };

        modulus = next_prime(size);
        unsigned long long temp = modulus - 1;
        for (int i = 2; i * i <= temp; ++i) {
            while (temp % i == 0) {
                modulus_factors[i]++;
                temp /= i;
            }
        }
        if (temp > 1) {
            modulus_factors[temp]++;
        }
    }

    void initGenerator() {
        bool found = false;
        unsigned long long base = 0;
        if (modulus == 3) {
            generator = 2;
            return;
        }
        while (!found) {
            base = std::rand() % (modulus - 2) + 2;
            found = generator != base;
            for (auto& factor : modulus_factors) {
                if (modexp(base, (modulus - 1) / factor.first, modulus) == 1) {
                    found = false;
                    break;
                }
            }
        }
        generator = base;
    }

    unsigned long long cycleUntilInRange(unsigned long long element) {
        while (element > size) {
            element = (element * generator) % modulus;
        }
        return element;
    }

    void initPermutation() {
        unsigned long long exp = std::rand() % (modulus - 2) + 2;
        end = cycleUntilInRange(modexp(generator, exp, modulus));
        start = cycleUntilInRange((end * generator) % modulus);
        current = start;
    }

    void restartCycle() {
        if (consistent) {
            current = start;
        } else {
            initGenerator();
            initPermutation();
        }
        cycle_start_time = std::chrono::system_clock::now();
        completed_cycle_count++;
    }

public:
    CyclicPRNG(int cycle_size, bool consistent = false) : size(cycle_size), completed_cycle_count(0), consistent(consistent) {
        if (size < 1) {
            throw std::invalid_argument("Random Number Generator must be given a positive non-zero integer");
        }
        initCyclicGroup();
        initGenerator();
        initPermutation();
        cycle_start_time = std::chrono::system_clock::now();
    }

    unsigned long long getRandom() {
        if (size <= 1) {
            return 1;
        }
        unsigned long long value = current;
        current = cycleUntilInRange((current * generator) % modulus);
        if (value == end) {
            restartCycle();
        }
        return value;
    }
};




arrow::Status run_benchmark_strided(arrow::MemoryPool * memory_pool) {
  double t;
  uint64_t size = 1024*1024*1024;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> col1_buffer, arrow::AllocateBuffer((size+2048)*8, memory_pool));
	perf("Remote buffer allocate");
	uint64_t* col1_buffer_data = (uint64_t*)col1_buffer->mutable_data();
	perf(NULL);
	#pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < size+2048; i++) {
		col1_buffer_data[i] = i;
	}
	t = perf("Buffer write");
	std::cout << "Comes to throughput of: " << ((size+2048)*8 / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

  std::vector<uint64_t> strides = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048};
  uint64_t agg = 0;
  uint64_t cnt = 0;
  for (uint64_t stride : strides) {
    perf(NULL);
    for (uint64_t i = 0; i < stride; i++) {
      // for (int64_t j = 0; j < ((size + stride-1) / stride); j++) {
      #pragma omp unroll partial(64)
	    #pragma omp parallel for simd
      for (int64_t j = 0; j < size; j+= stride) {
        agg += col1_buffer_data[i+j];
        cnt++;
      }
    }
    t = perf("Stride read");

    printf("\tStride=%lu time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", stride, t, ((size+2048)*8 / (1024UL*1024UL*1024UL) / (t/1000)), agg, cnt);
    agg = 0;
    cnt = 0;
  }

  agg = 0;
  cnt = 0;
  for (uint64_t stride : strides) {
    perf(NULL);
    for (uint64_t i = 0; i < stride; i++) {
      // for (int64_t j = 0; j < ((size + stride-1) / stride); j++) {
      #pragma omp unroll partial(64)
	    #pragma omp parallel for simd
      for (uint64_t j = 0; j < size; j+= stride) {
        col1_buffer_data[i+j] = i+j;
        cnt++;
      }
    }
    t = perf("Stride write");

    printf("\tStride=%lu time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", stride, t, (size*8 / (1024UL*1024UL*1024UL) / (t/1000)), agg, cnt);
    agg = 0;
    cnt = 0;
  }

  return arrow::Status::OK();
}



arrow::Status run_benchmark_random(arrow::MemoryPool * memory_pool) {
  double t;
  uint64_t size = 1024UL*1024*1024;
  CyclicPRNG prng_read(size);
  CyclicPRNG prng_write(size);
  CyclicPRNG prng_test(size);

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> col1_buffer, arrow::AllocateBuffer(size*8, memory_pool));
	perf("Remote buffer allocate");
	uint64_t* col1_buffer_data = (uint64_t*)col1_buffer->mutable_data();
	perf(NULL);
	#pragma omp unroll partial(64)
	#pragma omp parallel for simd
	for (uint64_t i = 0; i < size; i++) {
		col1_buffer_data[i] = i;
	}
	t = perf("Buffer write");
	std::cout << "Comes to throughput of: " << (size*8 / (1024UL*1024UL*1024UL) / (t/1000)) << " GB/s" << std::endl;

  uint64_t agg = 0;
  uint64_t cnt = 0;

  perf(NULL);
  for (uint64_t i = 0; i < size; i++) {
    agg += prng_test.getRandom();
  }
  perf("size randoms");

  agg = 0;
  cnt = 0;
  perf(NULL);
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd
  for (uint64_t i = 0; i < size; i++) {
    uint64_t index = prng_read.getRandom()-1;
    agg += col1_buffer_data[index];
    cnt++;
    // if (cnt % (1024UL*1024) == 0) printf("cnt=%lu\n",cnt);
  }
  t = perf("Random access read");
  printf("time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", t, (size*8 / (1024UL*1024UL*1024UL) / (t/1000)), agg, cnt);

  agg = 0;
  cnt = 0;
  perf(NULL);
  #pragma omp unroll partial(64)
	#pragma omp parallel for simd
  for (uint64_t i = 0; i < size; i++) {
    uint64_t index = prng_read.getRandom()-1;
    col1_buffer_data[index] = index;
    cnt++;
    // if (cnt % (1024UL*1024) == 0) printf("cnt=%lu\n",cnt);
  }
  t = perf("Random access write");
  printf("time=%f throughput=%f\n\t\tagg=%lu cnt=%lu\n", t, (size*8 / (1024UL*1024UL*1024UL) / (t/1000)), agg, cnt);

  return arrow::Status::OK();
}