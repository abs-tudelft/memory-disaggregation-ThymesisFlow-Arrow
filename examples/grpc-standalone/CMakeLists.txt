cmake_minimum_required(VERSION 3.16)

project(grpc-standalone)

find_package(Arrow REQUIRED)

include(FetchContent)
FetchContent_Declare(
  gRPC
  GIT_REPOSITORY https://github.com/grpc/grpc
  GIT_TAG        v1.47.5  # e.g v1.28.0
)
set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(gRPC)

# Proto file
get_filename_component(hw_proto "protos/arrow_tf.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/arrow_tf.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/arrow_tf.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/arrow_tf.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/arrow_tf.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND $<TARGET_FILE:protoc>
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="$<TARGET_FILE:grpc_cpp_plugin>"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# rg_grpc_proto
add_library(hw_grpc_proto
  ${hw_proto_srcs}
  ${hw_proto_hdrs}
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs})
target_link_libraries(hw_grpc_proto
  grpc++_reflection
  grpc++
  libprotobuf)

# Targets greeter_[async_](client|server)
foreach(_target
  client server)
  add_executable(${_target} "${_target}.cc")
  target_link_libraries(${_target}
    hw_grpc_proto
    grpc++_reflection
    grpc++
    libprotobuf)
endforeach()

# add_executable(main main.cc)
# # SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -ggdb")
# target_link_libraries(main grpc++ PRIVATE Arrow::arrow_static )