
cmake_minimum_required(VERSION 3.15)

set(command "/usr/bin/cmake;-DCMAKE_C_COMPILER=/usr/bin/cc;-DCMAKE_CXX_COMPILER=/usr/bin/c++;-DCMAKE_AR=/usr/bin/ar;-DCMAKE_RANLIB=/usr/bin/ranlib;-DCMAKE_BUILD_TYPE=DEBUG;-DCMAKE_C_FLAGS=  -ggdb -O0 -g -fPIC;-DCMAKE_C_FLAGS_DEBUG=  -ggdb -O0 -g -fPIC;-DCMAKE_CXX_FLAGS=  -fdiagnostics-color=always -ggdb -O0 -g -fPIC;-DCMAKE_CXX_FLAGS_DEBUG=  -fdiagnostics-color=always -ggdb -O0 -g -fPIC;-DCMAKE_CXX_STANDARD=17;-DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=;-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=;-DCMAKE_VERBOSE_MAKEFILE=FALSE;-DCMAKE_INSTALL_PREFIX=/mnt/afstuderen/arrow/cpp/build-support/thrift_ep-install;-DCMAKE_INSTALL_RPATH=/mnt/afstuderen/arrow/cpp/build-support/thrift_ep-install/lib;-DBoost_NO_BOOST_CMAKE=ON;-DBUILD_COMPILER=OFF;-DBUILD_EXAMPLES=OFF;-DBUILD_SHARED_LIBS=OFF;-DBUILD_TESTING=OFF;-DBUILD_TUTORIALS=OFF;-DCMAKE_DEBUG_POSTFIX=;-DWITH_AS3=OFF;-DWITH_CPP=ON;-DWITH_C_GLIB=OFF;-DWITH_JAVA=OFF;-DWITH_JAVASCRIPT=OFF;-DWITH_LIBEVENT=OFF;-DWITH_NODEJS=OFF;-DWITH_PYTHON=OFF;-DWITH_QT5=OFF;-DWITH_ZLIB=OFF;-DBOOST_ROOT=/mnt/afstuderen/arrow/cpp/build-support/boost_ep-prefix/src/boost_ep;-DBoost_NAMESPACE=boost;-GNinja;/mnt/afstuderen/arrow/cpp/build-support/thrift_ep-prefix/src/thrift_ep")
set(log_merged "")
set(log_output_on_failure "1")
set(stdout_log "/mnt/afstuderen/arrow/cpp/build-support/thrift_ep-prefix/src/thrift_ep-stamp/thrift_ep-configure-out.log")
set(stderr_log "/mnt/afstuderen/arrow/cpp/build-support/thrift_ep-prefix/src/thrift_ep-stamp/thrift_ep-configure-err.log")
execute_process(
  COMMAND ${command}
  RESULT_VARIABLE result
  OUTPUT_FILE "${stdout_log}"
  ERROR_FILE "${stderr_log}"
  )
macro(read_up_to_max_size log_file output_var)
  file(SIZE ${log_file} determined_size)
  set(max_size 10240)
  if (determined_size GREATER max_size)
    math(EXPR seek_position "${determined_size} - ${max_size}")
    file(READ ${log_file} ${output_var} OFFSET ${seek_position})
    set(${output_var} "...skipping to end...\n${${output_var}}")
  else()
    file(READ ${log_file} ${output_var})
  endif()
endmacro()
if(result)
  set(msg "Command failed: ${result}\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  if (${log_merged})
    set(msg "${msg}\nSee also\n  ${stderr_log}")
  else()
    set(msg "${msg}\nSee also\n  /mnt/afstuderen/arrow/cpp/build-support/thrift_ep-prefix/src/thrift_ep-stamp/thrift_ep-configure-*.log")
  endif()
  if (${log_output_on_failure})
    message(SEND_ERROR "${msg}")
    if (${log_merged})
      read_up_to_max_size("${stderr_log}" error_log_contents)
      message(STATUS "Log output is:\n${error_log_contents}")
    else()
      read_up_to_max_size("${stdout_log}" out_log_contents)
      read_up_to_max_size("${stderr_log}" err_log_contents)
      message(STATUS "stdout output is:\n${out_log_contents}")
      message(STATUS "stderr output is:\n${err_log_contents}")
    endif()
    message(FATAL_ERROR "Stopping after outputting logs.")
  else()
    message(FATAL_ERROR "${msg}")
  endif()
else()
  set(msg "thrift_ep configure command succeeded.  See also /mnt/afstuderen/arrow/cpp/build-support/thrift_ep-prefix/src/thrift_ep-stamp/thrift_ep-configure-*.log")
  message(STATUS "${msg}")
endif()
