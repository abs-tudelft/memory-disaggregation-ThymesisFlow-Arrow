cmake_minimum_required(VERSION 3.22)

project(apache-io)

find_package(Arrow REQUIRED)

find_package(Parquet REQUIRED)
set(ARROW_PARQUET ON)
# Prefer shared linkage but use static if shared build is deactivated
if(ARROW_BUILD_SHARED)
  set(PARQUET_LINK_LIBS parquet_shared)
else()
  set(PARQUET_LINK_LIBS parquet_static)
endif()
# find_package(ArrowDataset REQUIRED)

add_executable(main main.cc)
target_link_libraries(main PRIVATE Arrow::arrow_shared ${PARQUET_LINK_LIBS})

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -ggdb")