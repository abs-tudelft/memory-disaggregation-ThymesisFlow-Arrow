project(apache-arrow-test)

find_package(Arrow REQUIRED)

add_executable(main main.cc)
target_link_libraries(main PRIVATE Arrow::arrow_shared)