cmake_minimum_required(VERSION 3.16)

macro(print_all_variables)
    message(STATUS "print_all_variables------------------------------------------{")
    get_cmake_property(_variableNames VARIABLES)
    foreach (_variableName ${_variableNames})
        message(STATUS "${_variableName}=${${_variableName}}")
    endforeach()
    message(STATUS "print_all_variables------------------------------------------}")
endmacro()

project(apache-io)

# set(ARROW_BUILD_ROOT ../../arrow/cpp/build-debug/src/arrow)
# set(CMAKE_PREFIX_PATH ${ARROW_BUILD_ROOT} ${CMAKE_PREFIX_PATH})
# print_all_variables()
find_package(Arrow REQUIRED)
# target_link_libraries(main ../../arrow/cpp/build-debug/debug/libarrow.a)

find_package(Parquet REQUIRED)
# Prefer shared linkage but use static if shared build is deactivated
# if(ARROW_BUILD_SHARED)
#   set(PARQUET_LINK_LIBS parquet_shared)
# else()
#   set(PARQUET_LINK_LIBS parquet_static)
# endif()
set(PARQUET_LINK_LIBS parquet)

add_executable(main main.cc)
target_link_libraries(main PRIVATE Arrow::arrow_shared ${PARQUET_LINK_LIBS})
# target_link_libraries(main ../../arrow/cpp/build-debug/debug/libarrow.a ../../arrow/cpp/build-debug/debug/libarrow_bundled_dependencies.a)

# SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -ggdb")
