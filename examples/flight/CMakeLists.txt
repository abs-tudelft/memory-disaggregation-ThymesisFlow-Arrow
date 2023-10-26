cmake_minimum_required(VERSION 3.22)

project(apache-io)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

find_package(Parquet REQUIRED)
set(ARROW_PARQUET ON)
# Prefer shared linkage but use static if shared build is deactivated
# if(ARROW_BUILD_SHARED)
#   set(PARQUET_LINK_LIBS parquet_shared)
# else()
  set(PARQUET_LINK_LIBS parquet_static)
# endif()
# find_package(ArrowDataset REQUIRED)

find_package(Threads REQUIRED)
include(FetchContent)
FetchContent_Declare(
  grpc
  GIT_REPOSITORY https://github.com/grpc/grpc.git
  # when using gRPC, you will actually set this to an existing tag, such as
  # v1.25.0, v1.26.0 etc..
  # For the purpose of testing, we override the tag used to the commit
  # that's currently under test.
  GIT_TAG        v1.52.1)
FetchContent_MakeAvailable(grpc)
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)

# Proto file
get_filename_component(hw_proto "./proto/helloworld.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# hw_grpc_proto
add_library(hw_grpc_proto 
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# # Targets greeter_[async_](client|server)
# foreach(_target
#   greeter_client greeter_server 
#   greeter_callback_client greeter_callback_server 
#   greeter_async_client greeter_async_client2 greeter_async_server)
#   add_executable(${_target} "${_target}.cc")
#   target_link_libraries(${_target}
#     hw_grpc_proto
#     ${_REFLECTION}
#     ${_GRPC_GRPCPP}
#     ${_PROTOBUF_LIBPROTOBUF})
# endforeach()


add_executable(main main.cc)
target_link_libraries(main 
  Arrow::arrow_static ${PARQUET_LINK_LIBS} ArrowFlight::arrow_flight_static
  hw_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF}
)

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DDEBUG -ggdb -g3 -gdwarf")