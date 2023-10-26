cmake_minimum_required(VERSION 3.16.1 FATAL_ERROR)

project(arrow_example C CXX)

include_directories(include/arrow)
include_directories(include/arrow_generated)
include_directories(include/grpc)
include_directories(include/absl)
include_directories(include/protobuf)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    link_directories(lib/debug)
endif()
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    link_directories(lib/release)
endif()

set(ARROW_LINK_LIBS)

list(APPEND ARROW_LINK_LIBS libarrow.so)

find_package(OpenMP REQUIRED)

set(CMAKE_CXX_FLAGS "-std=c++17 -O2")

foreach(_target
  client0 client1)
  add_executable(${_target} "${_target}.cpp")
  target_link_libraries(${_target} ${ARROW_LINK_LIBS} OpenMP::OpenMP_CXX)
endforeach()

# add_executable(arrow_example main.cpp)
# target_link_libraries(arrow_example ${ARROW_LINK_LIBS})
