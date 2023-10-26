cmake_minimum_required(VERSION 3.16)

project(apache-io)

include_directories(include/arrow)
include_directories(include/arrow_generated)
include_directories(include/grpc)
include_directories(include/absl)
include_directories(include/protobuf)

link_directories(lib/debug)
set(ARROW_LINK_LIBS)
list(APPEND ARROW_LINK_LIBS libarrow.so)

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++17")

add_executable(client client.cc)
target_link_libraries(client ${ARROW_LINK_LIBS})

add_executable(server server.cc)
target_link_libraries(server ${ARROW_LINK_LIBS})

# target_link_libraries(main ../../arrow/cpp/build-debug/debug/libarrow.a ../../arrow/cpp/build-debug/debug/libarrow_bundled_dependencies.a)

# SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -ggdb")
