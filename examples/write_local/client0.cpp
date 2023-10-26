#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <vector>

#include "common.h"

arrow::Status RunMain(int argc, char** argv) {
	perf("");
	
	const uint64_t SIZE = 1 * 1024*1024*1024;
	printf("SIZE = %lu\n", SIZE);

	double res;
	uint64_t agg = 0;

	uint8_t* uint8 = (uint8_t *)malloc(SIZE);
	perf(NULL);
	// #pragma omp parallel for
	for (uint64_t i = 0; i < SIZE; i++) {
		// uint8[i] = (uint8_t)i;
		agg += uint8[i];
	}
	res = perf("uint8: single-threaded");
	printf("\t%f GB/s\n", (SIZE/(1024*1024*1024)) / (res/1000));
	free(uint8);
	
	// uint8 = (uint8_t *)malloc(SIZE);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=2) {
	// 	uint8[i] = (uint8_t)i;
	// 	uint8[i+1] = (uint8_t)i;
	// }
	// res = perf("uint8 two per loop");
	// printf("\t%f GB/s\n", (SIZE/(1024*1024*1024)) / (res/1000));
	// free(uint8);

	// uint16_t* uint16 = (uint16_t *)malloc(SIZE*2);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i++) {
	// 	uint16[i] = (uint16_t)i;
	// }
	// res = perf("uint16");
	// printf("\t%f GB/s\n", (SIZE*2/(1024*1024*1024)) / (res/1000));
	// free(uint16);

	// uint32_t* uint32 = (uint32_t *)malloc(SIZE*4);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i++) {
	// 	uint32[i] = (uint32_t)i;
	// }
	// res = perf("uint32");
	// printf("\t%f GB/s\n", (SIZE*4/(1024*1024*1024)) / (res/1000));
	// free(uint32);

	// uint64_t* uint64 = (uint64_t *)malloc(SIZE*8);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i++) {
	// 	uint64[i] = (uint64_t)i;
	// }
	// res = perf("uint64");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));
	// free(uint64);

	// uint64 = (uint64_t *)malloc(SIZE*8);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=2) {
	// 	uint64[i] = (uint64_t)i;
	// 	uint64[i+1] = (uint64_t)i;
	// }
	// res = perf("uint64 double per loop");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));
	// free(uint64);

	// uint64 = (uint64_t *)malloc(SIZE*8);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=3) {
	// 	uint64[i] = (uint64_t)i;
	// 	uint64[i+1] = (uint64_t)i;
	// 	uint64[i+2] = (uint64_t)i;
	// }
	// res = perf("uint64 thrice per loop");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));
	// free(uint64);

	// uint64 = (uint64_t *)malloc(SIZE*8);
	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=4) {
	// 	uint64[i] = (uint64_t)i;
	// 	uint64[i+1] = (uint64_t)i;
	// 	uint64[i+2] = (uint64_t)i;
	// 	uint64[i+3] = (uint64_t)i;
	// }
	// res = perf("uint64 4 per loop");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));
	// free(uint64);

	uint64_t* uint64 = (uint64_t *)malloc(SIZE*8);
	perf(NULL);
	for (uint64_t i = 0; i < SIZE; i+=1) {
		// uint64[i] = (uint64_t)i;
		agg += uint64[i];
	}
	res = perf("uint64 read: single-threaded");
	printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));

	// perf(NULL);
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=1) {
	// 	// uint64[i] = (uint64_t)i;
	// 	agg += uint64[i];
	// }
	// res = perf("uint64 read: parallel");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));

	// perf(NULL);
	// // #pragma omp simd
	// #pragma omp unroll partial(64)
	// #pragma omp parallel for
	// for (uint64_t i = 0; i < SIZE; i+=1) {
	// 	// uint64[i] = (uint64_t)i;
	// 	agg += uint64[i];
	// }
	// res = perf("uint64 read: 64 unroll, parallel");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));

	// perf(NULL);
	// // #pragma omp simd
	// #pragma omp unroll partial(64)
	// #pragma omp parallel for simd
	// for (uint64_t i = 0; i < SIZE; i+=1) {
	// 	// uint64[i] = (uint64_t)i;
	// 	agg += uint64[i];
	// }
	// res = perf("uint64 read: 64 unroll, parallel, simd");
	// printf("\t%f GB/s\n", (SIZE*8/(1024*1024*1024)) / (res/1000));

	free(uint64);
	printf("agg=%lu\n", agg); // Ue variable to prevent compiler form removing it

	return arrow::Status::OK();
}

int main(int argc, char** argv) {
	arrow::Status status = RunMain(argc, argv);
	if (!status.ok()) {
		std::cerr << status.ToString() << std::endl;
		return EXIT_FAILURE;
	}

	printf("Gracefull success exit\n");
	return EXIT_SUCCESS;
}
