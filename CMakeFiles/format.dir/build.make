# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

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
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/shuaishuai/project/KVstorageBaseRaft-cpp

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/shuaishuai/project/KVstorageBaseRaft-cpp

# Utility rule file for format.

# Include any custom commands dependencies for this target.
include CMakeFiles/format.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/format.dir/progress.make

CMakeFiles/format:
	bash /home/shuaishuai/project/KVstorageBaseRaft-cpp/format.sh
	echo format\ done!

format: CMakeFiles/format
format: CMakeFiles/format.dir/build.make
.PHONY : format

# Rule to build all files generated by this target.
CMakeFiles/format.dir/build: format
.PHONY : CMakeFiles/format.dir/build

CMakeFiles/format.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/format.dir/cmake_clean.cmake
.PHONY : CMakeFiles/format.dir/clean

CMakeFiles/format.dir/depend:
	cd /home/shuaishuai/project/KVstorageBaseRaft-cpp && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/shuaishuai/project/KVstorageBaseRaft-cpp /home/shuaishuai/project/KVstorageBaseRaft-cpp /home/shuaishuai/project/KVstorageBaseRaft-cpp /home/shuaishuai/project/KVstorageBaseRaft-cpp /home/shuaishuai/project/KVstorageBaseRaft-cpp/CMakeFiles/format.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/format.dir/depend

