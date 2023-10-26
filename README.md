# Zero-Copy, Zero-Serialization Memory Disaggregation using Apache Arrow and ThymesisFlow

This repository contains the code of Philip Groet's Master Thesis project. The project aims to create a Memory disaggregation platform in which servers can read each other's memory in a fully transparent fashion. Any load/store/memory instructions executed on remote memory address ranges are transparently rerouted to the remote memory. Transparently meaning that the user application has no knowledge where a certain memory address is located, and all hardware logic for memory CPU caching for example is also active for remote memory accesses.

This transparent behavior is especially useful for:
- Remote memory is caches locally
- Out-Of-Order instruction processing hardware of the processor also works on remote memory
- CPU pipelining hardware correctly identifies address dependencies of remote memory

On the hardware side ThymesisFlow is used. ThymesisFlow is an IBM prototype that makes use of the new OpenCAPI bus to allow different servers to access each others memory without intervention of the host processor. ThymesisFlow is meant to be used for a `memory stealing` situation in which memory is permanently lended to another server, the borrower CPU is not supposed to access memory it has lended. This project however breaks this design, every server is able to access not only remote memory, but also its own local memory in a fully cache coherent fashion.

On the software side we use Apache Arrow, an in-memory data format. It ensures that data structures are understandable between different programming languages, as well as multiple computer architectures. 

The combination of this system allows for:
- Ability to read and write to memory
- Ability to access both local and remote memory
- Use Arrow to make the memory understandable by every server accessing it.


The main challenges of the project were:
- ThymesisFlow is partially coherent in the case of a full memory sharing system. Has to be made fully coherent.
- Arrow does not allow for creating objects which are immediately sharable. An object first needs to be created, then copied into a shared address space
- The `table descriptor`, as I have come to call it, of Apache Arrow is not easily serializable. A `table descriptor` being a the data that describes the data object: schema, length, data types, and other metadata. It does *not* contain the actual data. If another server wants to understand a chunk of data it needs this `table descriptor` to understand the format.
- Systems needed to be devised to allow for communication between nodes. There needs to be a second communication channel besides the ThymesisFlow communication to ensure the coherency protocols devised in the thesis.
- The Arrow library API should not be modified. Applications using Apache Arrow should be one-on-one compatible with the disaggregated Arrow version.

## Structure of this repo

This repo contains prototype code which allows for creating Arrow objects in both local and remote memory regions. It contains a modified Arrow library which is one on one compatible with the official Arrow library stored in the `arrow/` directory.

The `examples/` directory contains code which tests the library, and the benchmarks used for my thesis. Notable examples:
- `1_component_benchmarks` contains all benchmarks for individual components, as also macro benchmarks which measure end-to-end performance. It also contains the measured data contained in th thesis document, with scripts to generate graphs.
- `write_local` and `write_remote` are scripts to measure the maximum achievable memory bandwidth to local memory, as well as through the ThymesisFlow link. It tests for differing data sizes, threads, SIMD, and loop unrolling
- `md_test_2node` is a test script which runs on a single machine. 2 instances are simulated as processes, and they communicate with each other through Linux shared memory mappings. This script was used to test the functionality of all Arrow code, before moving to a ThymesisFlow system.
- `md_test_2node__MD_FS_compare` is also a single machine multi node simulation in which the performance difference is measured between a full copy between the two processes, and a "zero-copy" one where only the metadata is sent over.
- `md_test_2node_tf` tests the functionality of Arrow on ThymesisFlow hardware. This test runs a client on a lender, and runs a client on a different server on a borrower client. It includes example on how to communicate Arrow data with the other side and tests data integrity 

### Changed parts Arrow

The Arrow source code was copied from the official Arrow repository. The branch was done at version `10.0.1`, to differentiate the modified version from the original, the modified version identifies itself as `10.0.255`.

Note that only the C++, denoted as cpp, part of the Arrow library has been modified. If you want to support memory disaggregation on the other languages you would have to rewrite the code for that language.

Notable files relevant for this thesis:
- `tf_orchestrate.[cc|h]` contains all code related to the user callable API, the inter-node communication, and the component benchmarks.
- `memory_pool_thymesisflow.cc` defines an Arrow MemoryPool which allows Arrow to allocate directly into a specified memory region. It does not use system malloc, but uses a different malloc implementation which allows for allocating inside the specified address ranges.
- Inside the `memory_pool.cc` the class `ThymesismallocRemoteMemoryPool` is defined. This MemoryPool can be passed to Arrow functions to make allocations happen in remotely mapped memory regions. Any remote allocations will be proxied to the remote machine using gRPC.
- `tf_serializer.[cc|h]` contains code to (de-)serialize a table descriptor from an Arrow RecordBatch and Table. It is mostly sourced from the Fletcher codebase.
- `tmalloc.[cc|h]` is a custom malloc implementation. Is used to allow allocating not only in kernel decided locations, but also able to malloc in a predefined mapped region.


### Bringup build system

Basic structure of running a program on the ThymesisFlow test setup of HPI:
1. Compile Apache Arrow
2. Compile application, link against Arrow
3. Initialize HPI ThymesisFlow setup


Arrow has an immensely complicated build system with cmake. You can enable and disable modules, which will change how Arrow is compiled. First we generate our build files using cmake. The below command enables the ThymesisFlow code and enables Protobuf and gRPC as code dependencies. gRPC will be compiled from source, and any future compilations of Arrow will link against this local version.
```bash
cd arrow/cpp
mkdir build-debug
cd build-debug
cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DARROW_OPTIONAL_INSTALL=ON \
  -DARROW_JEMALLOC=OFF \
  -DARROW_THYMESISMALLOC=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_CSV=ON \
  ..
```

Arrow should now have generated its library files in `arrow/cpp/build-debug/debug/`. Denoted with the modified version number: `libarrow.so.1000.255.0`.

Next the user application build system needs to be linking against this version of Arrow. I chose to do a direct link, instead of installing the libraries into system folders to prevent version mismatches. First cd to an application you want to compile: `cd examples/md_test_2node_tf/` for example. Next execute the following script to create soft links to the compiled Arrow libraries:
```bash
cd lib
ln -s ../../../arrow/cpp/build-debug/release/ debug
ln -s ../../../arrow/cpp/build-debug/release/ release
cd ../include
ln -s ../../../arrow/cpp/build-debug/absl_ep-install/include absl
ln -s ../../../arrow/cpp/build-debug/src arrow_generated
ln -s ../../../arrow/cpp/build-debug/grpc_ep-install/include grpc
ln -s ../../../arrow/cpp/build-debug/protobuf_ep-install/include/ protobuf
cd arrow
ln -s ../../../../arrow/cpp/src/arrow/ arrow
cd ../../
``` 

We are now set to compile the user application, run `make`.
Depending on the example, one or more binaries will have been generated. If they require ThymesisFlow, the ThymesisFlow hardware and software will have to be initialized before running. See next chapter

## Bringup HPI
In my thesis, in the appendix I put in a guide to bring up the ThymesisFlow setup. After the setup has been initialized, you can run the application