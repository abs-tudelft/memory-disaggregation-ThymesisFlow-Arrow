# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.25

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /opt/homebrew/Cellar/cmake/3.25.1/bin/cmake

# The command to remove a file.
RM = /opt/homebrew/Cellar/cmake/3.25.1/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system

# Include any dependencies generated for this target.
include CMakeFiles/arrow_example.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/arrow_example.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/arrow_example.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/arrow_example.dir/flags.make

CMakeFiles/arrow_example.dir/main.cpp.o: CMakeFiles/arrow_example.dir/flags.make
CMakeFiles/arrow_example.dir/main.cpp.o: main.cpp
CMakeFiles/arrow_example.dir/main.cpp.o: CMakeFiles/arrow_example.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/arrow_example.dir/main.cpp.o"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/arrow_example.dir/main.cpp.o -MF CMakeFiles/arrow_example.dir/main.cpp.o.d -o CMakeFiles/arrow_example.dir/main.cpp.o -c /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/main.cpp

CMakeFiles/arrow_example.dir/main.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/arrow_example.dir/main.cpp.i"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/main.cpp > CMakeFiles/arrow_example.dir/main.cpp.i

CMakeFiles/arrow_example.dir/main.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/arrow_example.dir/main.cpp.s"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/main.cpp -o CMakeFiles/arrow_example.dir/main.cpp.s

# Object files for target arrow_example
arrow_example_OBJECTS = \
"CMakeFiles/arrow_example.dir/main.cpp.o"

# External object files for target arrow_example
arrow_example_EXTERNAL_OBJECTS =

arrow_example: CMakeFiles/arrow_example.dir/main.cpp.o
arrow_example: CMakeFiles/arrow_example.dir/build.make
arrow_example: CMakeFiles/arrow_example.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable arrow_example"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/arrow_example.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/arrow_example.dir/build: arrow_example
.PHONY : CMakeFiles/arrow_example.dir/build

CMakeFiles/arrow_example.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/arrow_example.dir/cmake_clean.cmake
.PHONY : CMakeFiles/arrow_example.dir/clean

CMakeFiles/arrow_example.dir/depend:
	cd /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles/arrow_example.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/arrow_example.dir/depend

