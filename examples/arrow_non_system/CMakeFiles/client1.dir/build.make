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
include CMakeFiles/client1.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/client1.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/client1.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/client1.dir/flags.make

CMakeFiles/client1.dir/client1.cpp.o: CMakeFiles/client1.dir/flags.make
CMakeFiles/client1.dir/client1.cpp.o: client1.cpp
CMakeFiles/client1.dir/client1.cpp.o: CMakeFiles/client1.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/client1.dir/client1.cpp.o"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/client1.dir/client1.cpp.o -MF CMakeFiles/client1.dir/client1.cpp.o.d -o CMakeFiles/client1.dir/client1.cpp.o -c /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/client1.cpp

CMakeFiles/client1.dir/client1.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/client1.dir/client1.cpp.i"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/client1.cpp > CMakeFiles/client1.dir/client1.cpp.i

CMakeFiles/client1.dir/client1.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/client1.dir/client1.cpp.s"
	/Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/client1.cpp -o CMakeFiles/client1.dir/client1.cpp.s

# Object files for target client1
client1_OBJECTS = \
"CMakeFiles/client1.dir/client1.cpp.o"

# External object files for target client1
client1_EXTERNAL_OBJECTS =

client1: CMakeFiles/client1.dir/client1.cpp.o
client1: CMakeFiles/client1.dir/build.make
client1: CMakeFiles/client1.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable client1"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/client1.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/client1.dir/build: client1
.PHONY : CMakeFiles/client1.dir/build

CMakeFiles/client1.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/client1.dir/cmake_clean.cmake
.PHONY : CMakeFiles/client1.dir/clean

CMakeFiles/client1.dir/depend:
	cd /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system /Users/philipgroet/projects/afstuderen/arrow-tests/arrow_non_system/CMakeFiles/client1.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/client1.dir/depend

