# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/rlk/Downloads/mini_pg

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/rlk/Downloads/mini_pg/build

# Include any dependencies generated for this target.
include CMakeFiles/test_minidb.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/test_minidb.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/test_minidb.dir/flags.make

CMakeFiles/test_minidb.dir/test/test_users_thread.c.o: CMakeFiles/test_minidb.dir/flags.make
CMakeFiles/test_minidb.dir/test/test_users_thread.c.o: ../test/test_users_thread.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/rlk/Downloads/mini_pg/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building C object CMakeFiles/test_minidb.dir/test/test_users_thread.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/test_minidb.dir/test/test_users_thread.c.o   -c /home/rlk/Downloads/mini_pg/test/test_users_thread.c

CMakeFiles/test_minidb.dir/test/test_users_thread.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/test_minidb.dir/test/test_users_thread.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/rlk/Downloads/mini_pg/test/test_users_thread.c > CMakeFiles/test_minidb.dir/test/test_users_thread.c.i

CMakeFiles/test_minidb.dir/test/test_users_thread.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/test_minidb.dir/test/test_users_thread.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/rlk/Downloads/mini_pg/test/test_users_thread.c -o CMakeFiles/test_minidb.dir/test/test_users_thread.c.s

# Object files for target test_minidb
test_minidb_OBJECTS = \
"CMakeFiles/test_minidb.dir/test/test_users_thread.c.o"

# External object files for target test_minidb
test_minidb_EXTERNAL_OBJECTS =

bin/test_minidb: CMakeFiles/test_minidb.dir/test/test_users_thread.c.o
bin/test_minidb: CMakeFiles/test_minidb.dir/build.make
bin/test_minidb: lib/libminidb_core.a
bin/test_minidb: /usr/lib/x86_64-linux-gnu/libz.so
bin/test_minidb: CMakeFiles/test_minidb.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/rlk/Downloads/mini_pg/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking C executable bin/test_minidb"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/test_minidb.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/test_minidb.dir/build: bin/test_minidb

.PHONY : CMakeFiles/test_minidb.dir/build

CMakeFiles/test_minidb.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/test_minidb.dir/cmake_clean.cmake
.PHONY : CMakeFiles/test_minidb.dir/clean

CMakeFiles/test_minidb.dir/depend:
	cd /home/rlk/Downloads/mini_pg/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/rlk/Downloads/mini_pg /home/rlk/Downloads/mini_pg /home/rlk/Downloads/mini_pg/build /home/rlk/Downloads/mini_pg/build /home/rlk/Downloads/mini_pg/build/CMakeFiles/test_minidb.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/test_minidb.dir/depend

